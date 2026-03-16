use std::collections::BTreeSet;

use aria_core::{
    ContextBlock, ContextBlockKind, ContextPlan, ContextPlanDecision, ExecutionContextPack,
    ExecutionContract, GatewayChannel, InspectionBlockRecord, PromptContextMessage,
    ReferenceResolution, ReferenceResolutionOutcome, RetrievedContextBundle, WorkingSet,
    WorkingSetEntry, WorkingSetEntryKind, WorkingSetStatus,
};

#[derive(Debug, Clone)]
pub struct ContextPlannerInput {
    pub system_prompt: String,
    pub history_messages: Vec<PromptContextMessage>,
    pub candidate_blocks: Vec<ContextBlock>,
    pub user_request: String,
    pub channel: GatewayChannel,
    pub execution_contract: Option<ExecutionContract>,
    pub retrieved_context: Option<RetrievedContextBundle>,
    pub working_set: Option<WorkingSet>,
}

pub struct ContextPlanner;

impl ContextPlanner {
    pub fn plan(input: ContextPlannerInput) -> ExecutionContextPack {
        let ContextPlannerInput {
            system_prompt,
            history_messages,
            candidate_blocks,
            user_request,
            channel,
            execution_contract,
            retrieved_context,
            working_set,
        } = input;
        let mut block_records = Vec::new();
        let mut included_blocks = Vec::new();
        let mut seen = BTreeSet::new();

        for block in candidate_blocks {
            let decision = if block.content.trim().is_empty() {
                ContextPlanDecision::DroppedEmpty
            } else if !seen.insert(format!("{:?}:{}:{}", block.kind, block.label, block.content)) {
                ContextPlanDecision::DroppedDuplicate
            } else {
                ContextPlanDecision::Included
            };
            block_records.push(InspectionBlockRecord {
                kind: block.kind,
                label: block.label.clone(),
                decision,
                token_estimate: block.token_estimate,
                reason: match decision {
                    ContextPlanDecision::DroppedEmpty => Some("empty content".into()),
                    ContextPlanDecision::DroppedDuplicate => Some("duplicate block".into()),
                    _ => None,
                },
            });
            if matches!(decision, ContextPlanDecision::Included) {
                included_blocks.push(block);
            }
        }

        let working_set = working_set.map(|set| Self::resolve_working_set(&user_request, set));
        if let Some(ref set) = working_set {
            if let Some(block) = Self::working_set_context_block(set) {
                block_records.push(InspectionBlockRecord {
                    kind: block.kind,
                    label: block.label.clone(),
                    decision: ContextPlanDecision::Included,
                    token_estimate: block.token_estimate,
                    reason: None,
                });
                included_blocks.push(block);
            }
            if let Some(block) = Self::ambiguity_context_block(set) {
                block_records.push(InspectionBlockRecord {
                    kind: block.kind,
                    label: block.label.clone(),
                    decision: ContextPlanDecision::Included,
                    token_estimate: block.token_estimate,
                    reason: None,
                });
                included_blocks.push(block);
            }
        }

        let context_plan = ContextPlan {
            summary: Some(format!(
                "included_blocks={} dropped_blocks={}",
                block_records
                    .iter()
                    .filter(|record| matches!(record.decision, ContextPlanDecision::Included))
                    .count(),
                block_records
                    .iter()
                    .filter(|record| !matches!(record.decision, ContextPlanDecision::Included))
                    .count()
            )),
            block_records,
            ambiguity: working_set
                .as_ref()
                .and_then(|set| set.reference_resolution.clone()),
        };

        ExecutionContextPack {
            system_prompt,
            history_messages,
            context_blocks: included_blocks,
            user_request,
            channel,
            execution_contract,
            retrieved_context,
            working_set,
            context_plan: Some(context_plan),
        }
    }

    fn resolve_working_set(request_text: &str, mut working_set: WorkingSet) -> WorkingSet {
        if working_set.entries.is_empty() {
            working_set.reference_resolution = Some(ReferenceResolution {
                query_text: request_text.to_string(),
                outcome: ReferenceResolutionOutcome::Unresolved,
                matched_entry_ids: Vec::new(),
                active_target_entry_id: None,
                reason: Some("no working-set entries available".into()),
            });
            return working_set;
        }

        let mut scored = working_set
            .entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.status,
                    WorkingSetStatus::Active | WorkingSetStatus::Completed | WorkingSetStatus::Resolved
                )
            })
            .map(|entry| (entry.entry_id.clone(), Self::score_reference_match(request_text, entry)))
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let top = scored.first().cloned();
        let next = scored.get(1).cloned();
        let resolution = match (top, next) {
            (Some((entry_id, score)), Some((other_id, other_score)))
                if score > 0.0 && (score - other_score) >= 0.35 =>
            {
                working_set.active_target_entry_id = Some(entry_id.clone());
                ReferenceResolution {
                    query_text: request_text.to_string(),
                    outcome: ReferenceResolutionOutcome::Resolved,
                    matched_entry_ids: vec![entry_id],
                    active_target_entry_id: working_set.active_target_entry_id.clone(),
                    reason: Some(format!(
                        "selected over competing candidate '{}' with margin {:.2}",
                        other_id,
                        score - other_score
                    )),
                }
            }
            (Some((entry_id, score)), None) if score >= 0.0 => {
                working_set.active_target_entry_id = Some(entry_id.clone());
                ReferenceResolution {
                    query_text: request_text.to_string(),
                    outcome: ReferenceResolutionOutcome::Resolved,
                    matched_entry_ids: vec![entry_id],
                    active_target_entry_id: working_set.active_target_entry_id.clone(),
                    reason: Some("single candidate in working set".into()),
                }
            }
            (Some((entry_id, score)), Some((other_id, other_score)))
                if score > 0.0 && (score - other_score) < 0.35 =>
            {
                ReferenceResolution {
                    query_text: request_text.to_string(),
                    outcome: ReferenceResolutionOutcome::Ambiguous,
                    matched_entry_ids: vec![entry_id, other_id],
                    active_target_entry_id: None,
                    reason: Some("multiple working-set entries matched the request".into()),
                }
            }
            _ => ReferenceResolution {
                query_text: request_text.to_string(),
                outcome: ReferenceResolutionOutcome::Unresolved,
                matched_entry_ids: Vec::new(),
                active_target_entry_id: None,
                reason: Some("request did not clearly match an existing working-set entry".into()),
            },
        };
        working_set.reference_resolution = Some(resolution);
        working_set
    }

    fn score_reference_match(request_text: &str, entry: &WorkingSetEntry) -> f32 {
        let lower = request_text.to_ascii_lowercase();
        let mut score = 0.0;
        if matches!(entry.kind, WorkingSetEntryKind::PendingApproval) {
            score -= 0.5;
        }
        if let Some(locator) = &entry.locator {
            if lower.contains(&locator.to_ascii_lowercase()) {
                score += 2.0;
            }
            if let Some(ext) = locator.rsplit('.').next() {
                if lower.contains(ext) {
                    score += 0.2;
                }
            }
        }
        if let Some(operation) = &entry.operation {
            if lower.contains(&operation.to_ascii_lowercase()) {
                score += 0.8;
            }
        }
        if let Some(origin_tool) = &entry.origin_tool {
            if lower.contains(&origin_tool.to_ascii_lowercase()) {
                score += 0.6;
            }
        }
        if lower.contains(&entry.summary.to_ascii_lowercase()) {
            score += 1.4;
        } else {
            for token in entry.summary.to_ascii_lowercase().split_whitespace() {
                if token.len() > 3 && lower.contains(token) {
                    score += 0.15;
                }
            }
        }
        if lower.contains("it") || lower.contains("that") || lower.contains("the file") {
            score += 0.2;
        }
        score + (entry.created_at_us as f32 / 1_000_000_000_000_000.0)
    }

    fn working_set_context_block(working_set: &WorkingSet) -> Option<ContextBlock> {
        if working_set.entries.is_empty() {
            return None;
        }
        let content = working_set
            .entries
            .iter()
            .take(8)
            .map(|entry| {
                format!(
                    "- id={} kind={:?} artifact={:?} locator={:?} operation={:?} tool={:?} status={:?} summary={}",
                    entry.entry_id,
                    entry.kind,
                    entry.artifact_kind,
                    entry.locator,
                    entry.operation,
                    entry.origin_tool,
                    entry.status,
                    entry.summary
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Some(ContextBlock {
            kind: ContextBlockKind::WorkingSet,
            label: "working_set".into(),
            token_estimate: content.split_whitespace().count() as u32,
            content: format!(
                "<working_set>\n{}\nResolve follow-up references against this working set before asking for clarification.\n</working_set>",
                content
            ),
        })
    }

    fn ambiguity_context_block(working_set: &WorkingSet) -> Option<ContextBlock> {
        let resolution = working_set.reference_resolution.as_ref()?;
        if !matches!(resolution.outcome, ReferenceResolutionOutcome::Ambiguous) {
            return None;
        }
        let content = format!(
            "<ambiguity>\noutcome=ambiguous\nmatched_entry_ids={:?}\nreason={}\nAsk a clarification question instead of guessing the target.\n</ambiguity>",
            resolution.matched_entry_ids,
            resolution.reason.clone().unwrap_or_default()
        );
        Some(ContextBlock {
            kind: ContextBlockKind::Ambiguity,
            label: "reference_resolution".into(),
            token_estimate: content.split_whitespace().count() as u32,
            content,
        })
    }
}
