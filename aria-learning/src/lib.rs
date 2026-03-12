use aria_core::{AgentCapabilityProfile, GatewayChannel, SideEffectLevel, ToolRuntimePolicy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceOutcome {
    Succeeded,
    Failed,
    ClarificationRequired,
    ApprovalRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFingerprint {
    pub version: u16,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFingerprintParts {
    pub agent_id: String,
    pub prompt_mode: String,
    pub text: String,
    #[serde(default)]
    pub tools: Vec<String>,
}

impl TaskFingerprint {
    pub fn from_parts(
        agent_id: &str,
        prompt_mode: &str,
        user_text: &str,
        tool_names: &[String],
    ) -> Self {
        let normalized_tools = tool_names
            .iter()
            .map(|tool| normalize_token(tool))
            .filter(|tool| !tool.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(",");
        let key = format!(
            "v1|agent={}|mode={}|text={}|tools={}",
            normalize_token(agent_id),
            normalize_token(prompt_mode),
            normalize_text(user_text),
            normalized_tools
        );
        Self { version: 1, key }
    }
}

pub fn parse_task_fingerprint(key: &str) -> Option<TaskFingerprintParts> {
    let rest = key.strip_prefix("v1|")?;
    let mut agent_id = None;
    let mut prompt_mode = None;
    let mut text = None;
    let mut tools = Vec::new();
    for part in rest.split('|') {
        let (name, value) = part.split_once('=')?;
        match name {
            "agent" => agent_id = Some(value.to_string()),
            "mode" => prompt_mode = Some(value.to_string()),
            "text" => text = Some(value.to_string()),
            "tools" => {
                tools = value
                    .split(',')
                    .filter(|tool| !tool.is_empty())
                    .map(|tool| tool.to_string())
                    .collect();
            }
            _ => {}
        }
    }
    Some(TaskFingerprintParts {
        agent_id: agent_id?,
        prompt_mode: prompt_mode?,
        text: text?,
        tools,
    })
}

pub fn task_fingerprint_matches_request(
    key: &str,
    agent_id: &str,
    prompt_mode: &str,
    request_text: &str,
) -> bool {
    let Some(parts) = parse_task_fingerprint(key) else {
        return false;
    };
    parts.agent_id == normalize_token(agent_id)
        && parts.prompt_mode == normalize_token(prompt_mode)
        && parts.text == normalize_text(request_text)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub request_id: String,
    pub session_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub channel: GatewayChannel,
    pub prompt_mode: String,
    pub task_fingerprint: TaskFingerprint,
    pub user_input_summary: String,
    pub tool_names: Vec<String>,
    pub retrieved_corpora: Vec<String>,
    pub outcome: TraceOutcome,
    pub latency_ms: u32,
    pub response_summary: String,
    #[serde(default)]
    pub tool_runtime_policy: Option<ToolRuntimePolicy>,
    pub recorded_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RewardKind {
    Accepted,
    Rejected,
    Edited,
    Retried,
    OverrideApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewardEvent {
    pub event_id: String,
    pub request_id: String,
    pub session_id: String,
    pub kind: RewardKind,
    pub value: i32,
    pub notes: Option<String>,
    pub recorded_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySample {
    pub trace: ExecutionTrace,
    #[serde(default)]
    pub rewards: Vec<RewardEvent>,
    pub reward_score: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintEvaluationSummary {
    pub task_fingerprint: String,
    pub trace_count: u32,
    pub success_count: u32,
    pub approval_required_count: u32,
    pub clarification_count: u32,
    pub failure_count: u32,
    pub cumulative_reward: i32,
    pub latest_recorded_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintCluster {
    pub summary: FingerprintEvaluationSummary,
    #[serde(default)]
    pub top_agents: Vec<String>,
    #[serde(default)]
    pub top_prompt_modes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptOptimizationExample {
    pub user_input_summary: String,
    pub response_summary: String,
    #[serde(default)]
    pub retrieved_corpora: Vec<String>,
    pub reward_score: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptOptimizationDataset {
    pub task_fingerprint: String,
    pub agent_id: String,
    pub prompt_mode: String,
    #[serde(default)]
    pub examples: Vec<PromptOptimizationExample>,
    pub success_count: u32,
    pub cumulative_reward: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroWorkflowExample {
    pub user_input_summary: String,
    pub response_summary: String,
    #[serde(default)]
    pub tool_names: Vec<String>,
    pub reward_score: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroCompilationDataset {
    pub task_fingerprint: String,
    pub agent_id: String,
    pub prompt_mode: String,
    #[serde(default)]
    pub dominant_tool_sequence: Vec<String>,
    #[serde(default)]
    pub examples: Vec<MacroWorkflowExample>,
    pub success_count: u32,
    pub cumulative_reward: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmCompilationDataset {
    pub task_fingerprint: String,
    pub agent_id: String,
    pub prompt_mode: String,
    #[serde(default)]
    pub deterministic_tool_sequence: Vec<String>,
    #[serde(default)]
    pub example_inputs: Vec<String>,
    pub success_count: u32,
    pub cumulative_reward: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateArtifactKind {
    Prompt,
    Macro,
    Wasm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateArtifactStatus {
    Proposed,
    Evaluating,
    Promoted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateArtifactRecord {
    pub candidate_id: String,
    pub task_fingerprint: String,
    pub kind: CandidateArtifactKind,
    pub status: CandidateArtifactStatus,
    pub title: String,
    pub summary: String,
    pub payload_json: String,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateEvaluationRun {
    pub run_id: String,
    pub candidate_id: String,
    pub task_fingerprint: String,
    pub sample_count: u32,
    pub score: i32,
    pub passed: bool,
    pub notes: String,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePromotionAction {
    Promote,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePromotionStatus {
    Applied,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidatePromotionRecord {
    pub promotion_id: String,
    pub candidate_id: String,
    pub task_fingerprint: String,
    pub action: CandidatePromotionAction,
    pub status: CandidatePromotionStatus,
    pub requires_human_approval: bool,
    pub approved_by: Option<String>,
    pub notes: String,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectorModelKind {
    RouterHint,
    ToolRanker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectorModelRecord {
    pub model_id: String,
    pub task_fingerprint: String,
    pub kind: SelectorModelKind,
    pub payload_json: String,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningDerivativeKind {
    CandidateSynthesis,
    PromptCompile,
    MacroCompile,
    WasmCompile,
    SelectorSynthesis,
    Promotion,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearningDerivativeEvent {
    pub event_id: String,
    pub task_fingerprint: String,
    pub kind: LearningDerivativeKind,
    pub artifact_id: String,
    pub notes: String,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearningMetricsSnapshot {
    pub trace_count: u64,
    pub reward_count: u64,
    pub candidate_count: u64,
    pub promoted_candidate_count: u64,
    pub selector_model_count: u64,
    pub derivative_event_count: u64,
}

pub fn fingerprint_is_stable_workflow(summary: &FingerprintEvaluationSummary) -> bool {
    if summary.trace_count < 3 {
        return false;
    }
    if summary.failure_count > 0 {
        return false;
    }
    if summary.clarification_count > 0 {
        return false;
    }
    summary.success_count >= 2 && summary.cumulative_reward >= 0
}

pub fn synthesize_candidate_for_cluster(
    cluster: &FingerprintCluster,
    samples: &[ReplaySample],
    now_us: u64,
) -> Option<CandidateArtifactRecord> {
    if !fingerprint_is_stable_workflow(&cluster.summary) {
        return None;
    }

    let mut tool_names = BTreeSet::new();
    let mut corpora = BTreeSet::new();
    let mut example_inputs = Vec::new();
    for sample in samples {
        for tool in &sample.trace.tool_names {
            let normalized = normalize_token(tool);
            if !normalized.is_empty() {
                tool_names.insert(normalized);
            }
        }
        for corpus in &sample.trace.retrieved_corpora {
            let normalized = normalize_token(corpus);
            if !normalized.is_empty() {
                corpora.insert(normalized);
            }
        }
        if example_inputs.len() < 3 {
            example_inputs.push(sample.trace.user_input_summary.clone());
        }
    }

    let kind = if tool_names.is_empty() {
        CandidateArtifactKind::Prompt
    } else {
        CandidateArtifactKind::Macro
    };
    let title = match kind {
        CandidateArtifactKind::Prompt => "Prompt optimization candidate".to_string(),
        CandidateArtifactKind::Macro => "Workflow macro candidate".to_string(),
        CandidateArtifactKind::Wasm => "Wasm candidate".to_string(),
    };
    let summary = match kind {
        CandidateArtifactKind::Prompt => format!(
            "Stable repeated task with {} successful traces and no tool usage; optimize prompt selection and examples.",
            cluster.summary.success_count
        ),
        CandidateArtifactKind::Macro => format!(
            "Stable repeated workflow with tools [{}]; compile into a reusable deterministic macro.",
            tool_names.iter().cloned().collect::<Vec<_>>().join(",")
        ),
        CandidateArtifactKind::Wasm => "Promote deterministic workflow into a Wasm component.".to_string(),
    };
    let payload_json = json!({
        "top_agents": cluster.top_agents,
        "top_prompt_modes": cluster.top_prompt_modes,
        "tools": tool_names.into_iter().collect::<Vec<_>>(),
        "retrieved_corpora": corpora.into_iter().collect::<Vec<_>>(),
        "example_inputs": example_inputs,
        "trace_count": cluster.summary.trace_count,
        "success_count": cluster.summary.success_count,
        "cumulative_reward": cluster.summary.cumulative_reward,
    })
    .to_string();

    Some(CandidateArtifactRecord {
        candidate_id: format!(
            "{}:{}",
            match kind {
                CandidateArtifactKind::Prompt => "prompt",
                CandidateArtifactKind::Macro => "macro",
                CandidateArtifactKind::Wasm => "wasm",
            },
            cluster.summary.task_fingerprint
        ),
        task_fingerprint: cluster.summary.task_fingerprint.clone(),
        kind,
        status: CandidateArtifactStatus::Proposed,
        title,
        summary,
        payload_json,
        created_at_us: now_us,
        updated_at_us: now_us,
    })
}

pub fn evaluate_candidate_against_replay(
    candidate: &CandidateArtifactRecord,
    samples: &[ReplaySample],
    now_us: u64,
) -> CandidateEvaluationRun {
    let sample_count = samples.len() as u32;
    let success_count = samples
        .iter()
        .filter(|sample| matches!(sample.trace.outcome, TraceOutcome::Succeeded))
        .count() as i32;
    let reward_total: i32 = samples.iter().map(|sample| sample.reward_score).sum();
    let tool_heavy_count = samples
        .iter()
        .filter(|sample| !sample.trace.tool_names.is_empty())
        .count() as i32;
    let deterministic_sequence = samples
        .iter()
        .filter(|sample| {
            matches!(sample.trace.outcome, TraceOutcome::Succeeded)
                && sample.reward_score > 0
                && !sample.trace.tool_names.is_empty()
        })
        .map(|sample| sample.trace.tool_names.clone())
        .reduce(|left, right| if left == right { left } else { Vec::new() })
        .map(|sequence| !sequence.is_empty())
        .unwrap_or(false);

    let success_component = if sample_count == 0 {
        0
    } else {
        (success_count * 100) / sample_count as i32
    };
    let reward_component = reward_total * 10;
    let fit_component = match candidate.kind {
        CandidateArtifactKind::Prompt => {
            if tool_heavy_count == 0 {
                20
            } else {
                -20
            }
        }
        CandidateArtifactKind::Macro => {
            if tool_heavy_count > 0 {
                20
            } else {
                -20
            }
        }
        CandidateArtifactKind::Wasm => {
            if tool_heavy_count > 0 && sample_count >= 5 && deterministic_sequence {
                20
            } else {
                -40
            }
        }
    };

    let score = success_component + reward_component + fit_component;
    let passed = match candidate.kind {
        CandidateArtifactKind::Wasm => {
            sample_count >= 5
                && deterministic_sequence
                && success_count == sample_count as i32
                && score >= 90
        }
        _ => sample_count >= 3 && score >= 70,
    };
    let notes = format!(
        "success_component={} reward_component={} fit_component={} tool_heavy_count={} deterministic_sequence={}",
        success_component, reward_component, fit_component, tool_heavy_count, deterministic_sequence
    );

    CandidateEvaluationRun {
        run_id: format!("eval:{}:{}", candidate.candidate_id, now_us),
        candidate_id: candidate.candidate_id.clone(),
        task_fingerprint: candidate.task_fingerprint.clone(),
        sample_count,
        score,
        passed,
        notes,
        created_at_us: now_us,
    }
}

pub fn candidate_requires_human_approval(
    candidate: &CandidateArtifactRecord,
    capability_profiles: &[AgentCapabilityProfile],
) -> bool {
    if matches!(candidate.kind, CandidateArtifactKind::Wasm) {
        return true;
    }
    capability_profiles.iter().any(|profile| {
        profile.requires_elevation
            || matches!(profile.side_effect_level, SideEffectLevel::Privileged)
    })
}

pub fn build_candidate_promotion_record(
    candidate: &CandidateArtifactRecord,
    latest_eval: Option<&CandidateEvaluationRun>,
    capability_profiles: &[AgentCapabilityProfile],
    action: CandidatePromotionAction,
    approved_by: Option<String>,
    now_us: u64,
) -> CandidatePromotionRecord {
    let requires_human_approval = candidate_requires_human_approval(candidate, capability_profiles);
    let (status, notes) = match action {
        CandidatePromotionAction::Rollback => (
            CandidatePromotionStatus::Applied,
            "rollback recorded".to_string(),
        ),
        CandidatePromotionAction::Promote => {
            if !latest_eval.map(|run| run.passed).unwrap_or(false) {
                (
                    CandidatePromotionStatus::Blocked,
                    "latest evaluation did not pass".to_string(),
                )
            } else if requires_human_approval && approved_by.is_none() {
                (
                    CandidatePromotionStatus::Blocked,
                    "human approval required for privileged promotion".to_string(),
                )
            } else {
                (
                    CandidatePromotionStatus::Applied,
                    "promotion applied".to_string(),
                )
            }
        }
    };

    CandidatePromotionRecord {
        promotion_id: format!(
            "{}:{}:{}",
            match action {
                CandidatePromotionAction::Promote => "promote",
                CandidatePromotionAction::Rollback => "rollback",
            },
            candidate.candidate_id,
            now_us
        ),
        candidate_id: candidate.candidate_id.clone(),
        task_fingerprint: candidate.task_fingerprint.clone(),
        action,
        status,
        requires_human_approval,
        approved_by,
        notes,
        created_at_us: now_us,
    }
}

pub fn train_selector_models_for_samples(
    fingerprint: &str,
    samples: &[ReplaySample],
    now_us: u64,
) -> Vec<SelectorModelRecord> {
    let accepted_successes = samples
        .iter()
        .filter(|sample| {
            matches!(sample.trace.outcome, TraceOutcome::Succeeded) && sample.reward_score > 0
        })
        .collect::<Vec<_>>();
    if accepted_successes.is_empty() {
        return Vec::new();
    }

    let mut models = Vec::new();

    let mut agent_counts = std::collections::BTreeMap::<String, u32>::new();
    for sample in &accepted_successes {
        *agent_counts
            .entry(sample.trace.agent_id.clone())
            .or_default() += 1;
    }
    if let Some((agent_id, count)) = agent_counts.into_iter().max_by_key(|(_, count)| *count) {
        models.push(SelectorModelRecord {
            model_id: format!("router_hint:{}", fingerprint),
            task_fingerprint: fingerprint.to_string(),
            kind: SelectorModelKind::RouterHint,
            payload_json: json!({
                "agent_id": agent_id,
                "support": count,
            })
            .to_string(),
            created_at_us: now_us,
            updated_at_us: now_us,
        });
    }

    let mut tool_counts = std::collections::BTreeMap::<String, u32>::new();
    for sample in &accepted_successes {
        for tool in &sample.trace.tool_names {
            *tool_counts.entry(normalize_token(tool)).or_default() += 1;
        }
    }
    if !tool_counts.is_empty() {
        let mut ranked_tools = tool_counts.into_iter().collect::<Vec<_>>();
        ranked_tools.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        models.push(SelectorModelRecord {
            model_id: format!("tool_ranker:{}", fingerprint),
            task_fingerprint: fingerprint.to_string(),
            kind: SelectorModelKind::ToolRanker,
            payload_json: json!({
                "tools": ranked_tools
                    .iter()
                    .map(|(tool, count)| json!({"name": tool, "count": count}))
                    .collect::<Vec<_>>(),
            })
            .to_string(),
            created_at_us: now_us,
            updated_at_us: now_us,
        });
    }

    models
}

pub fn build_prompt_optimization_dataset(
    fingerprint: &str,
    samples: &[ReplaySample],
) -> Option<PromptOptimizationDataset> {
    let accepted_prompt_samples = samples
        .iter()
        .filter(|sample| {
            matches!(sample.trace.outcome, TraceOutcome::Succeeded)
                && sample.reward_score > 0
                && sample.trace.tool_names.is_empty()
        })
        .collect::<Vec<_>>();
    if accepted_prompt_samples.len() < 2 {
        return None;
    }

    let agent_id = accepted_prompt_samples[0].trace.agent_id.clone();
    let prompt_mode = accepted_prompt_samples[0].trace.prompt_mode.clone();
    let success_count = accepted_prompt_samples.len() as u32;
    let cumulative_reward = accepted_prompt_samples
        .iter()
        .map(|sample| sample.reward_score)
        .sum();
    let examples = accepted_prompt_samples
        .iter()
        .take(5)
        .map(|sample| PromptOptimizationExample {
            user_input_summary: sample.trace.user_input_summary.clone(),
            response_summary: sample.trace.response_summary.clone(),
            retrieved_corpora: sample.trace.retrieved_corpora.clone(),
            reward_score: sample.reward_score,
        })
        .collect();

    Some(PromptOptimizationDataset {
        task_fingerprint: fingerprint.to_string(),
        agent_id,
        prompt_mode,
        examples,
        success_count,
        cumulative_reward,
    })
}

pub fn compile_prompt_candidate_from_dataset(
    dataset: &PromptOptimizationDataset,
    now_us: u64,
) -> CandidateArtifactRecord {
    let corpora = dataset
        .examples
        .iter()
        .flat_map(|example| example.retrieved_corpora.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let example_inputs = dataset
        .examples
        .iter()
        .map(|example| example.user_input_summary.clone())
        .collect::<Vec<_>>();
    let response_patterns = dataset
        .examples
        .iter()
        .map(|example| example.response_summary.clone())
        .collect::<Vec<_>>();

    CandidateArtifactRecord {
        candidate_id: format!("prompt:{}", dataset.task_fingerprint),
        task_fingerprint: dataset.task_fingerprint.clone(),
        kind: CandidateArtifactKind::Prompt,
        status: CandidateArtifactStatus::Proposed,
        title: "Prompt optimization candidate".to_string(),
        summary: format!(
            "Compiled prompt dataset with {} accepted prompt-only traces for agent '{}' in mode '{}'.",
            dataset.success_count, dataset.agent_id, dataset.prompt_mode
        ),
        payload_json: json!({
            "agent_id": dataset.agent_id,
            "prompt_mode": dataset.prompt_mode,
            "example_inputs": example_inputs,
            "response_patterns": response_patterns,
            "retrieved_corpora": corpora,
            "success_count": dataset.success_count,
            "cumulative_reward": dataset.cumulative_reward,
            "compiled_from": "prompt_optimization_dataset",
        })
        .to_string(),
        created_at_us: now_us,
        updated_at_us: now_us,
    }
}

pub fn build_macro_compilation_dataset(
    fingerprint: &str,
    samples: &[ReplaySample],
) -> Option<MacroCompilationDataset> {
    let accepted_macro_samples = samples
        .iter()
        .filter(|sample| {
            matches!(sample.trace.outcome, TraceOutcome::Succeeded)
                && sample.reward_score > 0
                && !sample.trace.tool_names.is_empty()
        })
        .collect::<Vec<_>>();
    if accepted_macro_samples.len() < 2 {
        return None;
    }

    let agent_id = accepted_macro_samples[0].trace.agent_id.clone();
    let prompt_mode = accepted_macro_samples[0].trace.prompt_mode.clone();
    let success_count = accepted_macro_samples.len() as u32;
    let cumulative_reward = accepted_macro_samples
        .iter()
        .map(|sample| sample.reward_score)
        .sum();

    let mut sequence_counts = std::collections::BTreeMap::<String, u32>::new();
    for sample in &accepted_macro_samples {
        let sequence = sample.trace.tool_names.join(">");
        *sequence_counts.entry(sequence).or_default() += 1;
    }
    let dominant_tool_sequence = sequence_counts
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)))
        .map(|(sequence, _)| {
            sequence
                .split('>')
                .filter(|tool| !tool.is_empty())
                .map(|tool| tool.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let examples = accepted_macro_samples
        .iter()
        .take(5)
        .map(|sample| MacroWorkflowExample {
            user_input_summary: sample.trace.user_input_summary.clone(),
            response_summary: sample.trace.response_summary.clone(),
            tool_names: sample.trace.tool_names.clone(),
            reward_score: sample.reward_score,
        })
        .collect();

    Some(MacroCompilationDataset {
        task_fingerprint: fingerprint.to_string(),
        agent_id,
        prompt_mode,
        dominant_tool_sequence,
        examples,
        success_count,
        cumulative_reward,
    })
}

pub fn compile_macro_candidate_from_dataset(
    dataset: &MacroCompilationDataset,
    now_us: u64,
) -> CandidateArtifactRecord {
    let example_inputs = dataset
        .examples
        .iter()
        .map(|example| example.user_input_summary.clone())
        .collect::<Vec<_>>();
    let response_patterns = dataset
        .examples
        .iter()
        .map(|example| example.response_summary.clone())
        .collect::<Vec<_>>();

    CandidateArtifactRecord {
        candidate_id: format!("macro:{}", dataset.task_fingerprint),
        task_fingerprint: dataset.task_fingerprint.clone(),
        kind: CandidateArtifactKind::Macro,
        status: CandidateArtifactStatus::Proposed,
        title: "Workflow macro candidate".to_string(),
        summary: format!(
            "Compiled workflow macro with {} accepted traces and dominant tool sequence [{}].",
            dataset.success_count,
            dataset.dominant_tool_sequence.join(",")
        ),
        payload_json: json!({
            "agent_id": dataset.agent_id,
            "prompt_mode": dataset.prompt_mode,
            "tools": dataset.dominant_tool_sequence,
            "example_inputs": example_inputs,
            "response_patterns": response_patterns,
            "success_count": dataset.success_count,
            "cumulative_reward": dataset.cumulative_reward,
            "compiled_from": "macro_compilation_dataset",
        })
        .to_string(),
        created_at_us: now_us,
        updated_at_us: now_us,
    }
}

pub fn build_wasm_compilation_dataset(
    fingerprint: &str,
    samples: &[ReplaySample],
) -> Option<WasmCompilationDataset> {
    let accepted_samples = samples
        .iter()
        .filter(|sample| {
            matches!(sample.trace.outcome, TraceOutcome::Succeeded)
                && sample.reward_score > 0
                && !sample.trace.tool_names.is_empty()
        })
        .collect::<Vec<_>>();
    if accepted_samples.len() < 5 {
        return None;
    }

    let first_sequence = accepted_samples[0].trace.tool_names.clone();
    if first_sequence.is_empty()
        || accepted_samples
            .iter()
            .any(|sample| sample.trace.tool_names != first_sequence)
    {
        return None;
    }

    let agent_id = accepted_samples[0].trace.agent_id.clone();
    let prompt_mode = accepted_samples[0].trace.prompt_mode.clone();
    let success_count = accepted_samples.len() as u32;
    let cumulative_reward = accepted_samples
        .iter()
        .map(|sample| sample.reward_score)
        .sum();
    let example_inputs = accepted_samples
        .iter()
        .take(5)
        .map(|sample| sample.trace.user_input_summary.clone())
        .collect::<Vec<_>>();

    Some(WasmCompilationDataset {
        task_fingerprint: fingerprint.to_string(),
        agent_id,
        prompt_mode,
        deterministic_tool_sequence: first_sequence,
        example_inputs,
        success_count,
        cumulative_reward,
    })
}

pub fn compile_wasm_candidate_from_dataset(
    dataset: &WasmCompilationDataset,
    now_us: u64,
) -> CandidateArtifactRecord {
    CandidateArtifactRecord {
        candidate_id: format!("wasm:{}", dataset.task_fingerprint),
        task_fingerprint: dataset.task_fingerprint.clone(),
        kind: CandidateArtifactKind::Wasm,
        status: CandidateArtifactStatus::Proposed,
        title: "Deterministic Wasm candidate".to_string(),
        summary: format!(
            "Deterministic workflow with {} accepted traces and fixed tool sequence [{}]. Requires strict eval and approval before activation.",
            dataset.success_count,
            dataset.deterministic_tool_sequence.join(",")
        ),
        payload_json: json!({
            "agent_id": dataset.agent_id,
            "prompt_mode": dataset.prompt_mode,
            "tools": dataset.deterministic_tool_sequence,
            "example_inputs": dataset.example_inputs,
            "success_count": dataset.success_count,
            "cumulative_reward": dataset.cumulative_reward,
            "compiled_from": "wasm_compilation_dataset",
            "activation": "blocked_until_manual_runtime_support",
        })
        .to_string(),
        created_at_us: now_us,
        updated_at_us: now_us,
    }
}

fn normalize_token(input: &str) -> String {
    normalize_text(input).replace(' ', "_")
}

fn normalize_text(input: &str) -> String {
    let mut out = String::new();
    let mut last_was_sep = false;
    for ch in input.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_was_sep = false;
        } else if !last_was_sep {
            out.push(' ');
            last_was_sep = true;
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_fingerprint_normalizes_whitespace_case_and_tool_order() {
        let a = TaskFingerprint::from_parts(
            "Researcher",
            "Execution",
            " Summarize   the latest docs for me ",
            &["read_file".into(), "search_tool_registry".into()],
        );
        let b = TaskFingerprint::from_parts(
            "researcher",
            "execution",
            "summarize the latest docs for me",
            &["search_tool_registry".into(), "read_file".into()],
        );

        assert_eq!(a, b);
        assert!(a.key.contains("agent=researcher"));
        assert!(a.key.contains("mode=execution"));
        assert!(a.key.contains("tools=read_file,search_tool_registry"));
    }

    #[test]
    fn task_fingerprint_drops_duplicate_tools() {
        let fp = TaskFingerprint::from_parts(
            "developer",
            "execution",
            "write the file",
            &["write_file".into(), "write_file".into(), "read_file".into()],
        );

        assert!(fp.key.contains("tools=read_file,write_file"));
    }

    #[test]
    fn parse_task_fingerprint_extracts_parts() {
        let parts = parse_task_fingerprint(
            "v1|agent=developer|mode=execution|text=write_file|tools=read_file,write_file",
        )
        .expect("parts");
        assert_eq!(parts.agent_id, "developer");
        assert_eq!(parts.prompt_mode, "execution");
        assert_eq!(parts.text, "write_file");
        assert_eq!(
            parts.tools,
            vec!["read_file".to_string(), "write_file".to_string()]
        );
    }

    #[test]
    fn replay_sample_accumulates_reward_score() {
        let trace = ExecutionTrace {
            request_id: "req-1".into(),
            session_id: "sess-1".into(),
            user_id: "u1".into(),
            agent_id: "developer".into(),
            channel: GatewayChannel::Cli,
            prompt_mode: "execution".into(),
            task_fingerprint: TaskFingerprint::from_parts(
                "developer",
                "execution",
                "write file",
                &Vec::new(),
            ),
            user_input_summary: "write file".into(),
            tool_names: Vec::new(),
            retrieved_corpora: Vec::new(),
            outcome: TraceOutcome::Succeeded,
            latency_ms: 10,
            response_summary: "ok".into(),
            tool_runtime_policy: None,
            recorded_at_us: 1,
        };
        let rewards = vec![
            RewardEvent {
                event_id: "evt-1".into(),
                request_id: "req-1".into(),
                session_id: "sess-1".into(),
                kind: RewardKind::Accepted,
                value: 1,
                notes: None,
                recorded_at_us: 2,
            },
            RewardEvent {
                event_id: "evt-2".into(),
                request_id: "req-1".into(),
                session_id: "sess-1".into(),
                kind: RewardKind::OverrideApplied,
                value: 1,
                notes: None,
                recorded_at_us: 3,
            },
        ];
        let sample = ReplaySample {
            trace,
            rewards,
            reward_score: 2,
        };

        assert_eq!(sample.reward_score, 2);
        assert_eq!(sample.rewards.len(), 2);
    }

    #[test]
    fn fingerprint_is_stable_workflow_requires_repeat_success_without_failures() {
        let stable = FingerprintEvaluationSummary {
            task_fingerprint: "fp-1".into(),
            trace_count: 3,
            success_count: 3,
            approval_required_count: 0,
            clarification_count: 0,
            failure_count: 0,
            cumulative_reward: 2,
            latest_recorded_at_us: 10,
        };
        let unstable = FingerprintEvaluationSummary {
            failure_count: 1,
            ..stable.clone()
        };

        assert!(fingerprint_is_stable_workflow(&stable));
        assert!(!fingerprint_is_stable_workflow(&unstable));
    }

    #[test]
    fn synthesize_candidate_for_cluster_prefers_macro_when_tools_repeat() {
        let cluster = FingerprintCluster {
            summary: FingerprintEvaluationSummary {
                task_fingerprint: "fp-1".into(),
                trace_count: 3,
                success_count: 3,
                approval_required_count: 0,
                clarification_count: 0,
                failure_count: 0,
                cumulative_reward: 2,
                latest_recorded_at_us: 10,
            },
            top_agents: vec!["developer".into()],
            top_prompt_modes: vec!["execution".into()],
        };
        let samples = vec![ReplaySample {
            trace: ExecutionTrace {
                request_id: "req-1".into(),
                session_id: "sess-1".into(),
                user_id: "u1".into(),
                agent_id: "developer".into(),
                channel: GatewayChannel::Cli,
                prompt_mode: "execution".into(),
                task_fingerprint: TaskFingerprint {
                    version: 1,
                    key: "fp-1".into(),
                },
                user_input_summary: "summarize docs".into(),
                tool_names: vec!["read_file".into(), "search_tool_registry".into()],
                retrieved_corpora: vec!["workspace".into()],
                outcome: TraceOutcome::Succeeded,
                latency_ms: 10,
                response_summary: "ok".into(),
                tool_runtime_policy: None,
                recorded_at_us: 1,
            },
            rewards: Vec::new(),
            reward_score: 1,
        }];

        let candidate =
            synthesize_candidate_for_cluster(&cluster, &samples, 100).expect("candidate");
        assert_eq!(candidate.kind, CandidateArtifactKind::Macro);
        assert!(candidate.summary.contains("reusable deterministic macro"));
    }

    #[test]
    fn synthesize_candidate_for_cluster_prefers_prompt_without_tools() {
        let cluster = FingerprintCluster {
            summary: FingerprintEvaluationSummary {
                task_fingerprint: "fp-2".into(),
                trace_count: 3,
                success_count: 3,
                approval_required_count: 0,
                clarification_count: 0,
                failure_count: 0,
                cumulative_reward: 1,
                latest_recorded_at_us: 10,
            },
            top_agents: vec!["researcher".into()],
            top_prompt_modes: vec!["planning".into()],
        };
        let samples = vec![ReplaySample {
            trace: ExecutionTrace {
                request_id: "req-1".into(),
                session_id: "sess-1".into(),
                user_id: "u1".into(),
                agent_id: "researcher".into(),
                channel: GatewayChannel::Cli,
                prompt_mode: "planning".into(),
                task_fingerprint: TaskFingerprint {
                    version: 1,
                    key: "fp-2".into(),
                },
                user_input_summary: "plan the research summary".into(),
                tool_names: Vec::new(),
                retrieved_corpora: vec!["workspace".into()],
                outcome: TraceOutcome::Succeeded,
                latency_ms: 10,
                response_summary: "ok".into(),
                tool_runtime_policy: None,
                recorded_at_us: 1,
            },
            rewards: Vec::new(),
            reward_score: 1,
        }];

        let candidate =
            synthesize_candidate_for_cluster(&cluster, &samples, 100).expect("candidate");
        assert_eq!(candidate.kind, CandidateArtifactKind::Prompt);
        assert!(candidate.summary.contains("optimize prompt selection"));
    }

    #[test]
    fn evaluate_candidate_against_replay_passes_stable_matching_macro_candidate() {
        let candidate = CandidateArtifactRecord {
            candidate_id: "cand-1".into(),
            task_fingerprint: "fp-1".into(),
            kind: CandidateArtifactKind::Macro,
            status: CandidateArtifactStatus::Proposed,
            title: "Macro".into(),
            summary: "Macro".into(),
            payload_json: "{}".into(),
            created_at_us: 1,
            updated_at_us: 1,
        };
        let samples = (0..3)
            .map(|i| ReplaySample {
                trace: ExecutionTrace {
                    request_id: format!("req-{}", i),
                    session_id: "sess-1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "summarize docs".into(),
                    tool_names: vec!["read_file".into()],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 10,
                    response_summary: "ok".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: i,
                },
                rewards: Vec::new(),
                reward_score: 1,
            })
            .collect::<Vec<_>>();

        let run = evaluate_candidate_against_replay(&candidate, &samples, 100);
        assert!(run.passed);
        assert!(run.score >= 70);
    }

    #[test]
    fn evaluate_candidate_against_replay_rejects_mismatched_prompt_candidate() {
        let candidate = CandidateArtifactRecord {
            candidate_id: "cand-2".into(),
            task_fingerprint: "fp-2".into(),
            kind: CandidateArtifactKind::Prompt,
            status: CandidateArtifactStatus::Proposed,
            title: "Prompt".into(),
            summary: "Prompt".into(),
            payload_json: "{}".into(),
            created_at_us: 1,
            updated_at_us: 1,
        };
        let samples = vec![ReplaySample {
            trace: ExecutionTrace {
                request_id: "req-1".into(),
                session_id: "sess-1".into(),
                user_id: "u1".into(),
                agent_id: "developer".into(),
                channel: GatewayChannel::Cli,
                prompt_mode: "execution".into(),
                task_fingerprint: TaskFingerprint {
                    version: 1,
                    key: "fp-2".into(),
                },
                user_input_summary: "summarize docs".into(),
                tool_names: vec!["read_file".into()],
                retrieved_corpora: vec!["workspace".into()],
                outcome: TraceOutcome::Succeeded,
                latency_ms: 10,
                response_summary: "ok".into(),
                tool_runtime_policy: None,
                recorded_at_us: 1,
            },
            rewards: Vec::new(),
            reward_score: 0,
        }];

        let run = evaluate_candidate_against_replay(&candidate, &samples, 100);
        assert!(!run.passed);
        assert_eq!(run.sample_count, 1);
        assert!(run.notes.contains("fit_component=-20"));
    }

    #[test]
    fn build_candidate_promotion_record_blocks_privileged_promotion_without_approval() {
        let candidate = CandidateArtifactRecord {
            candidate_id: "cand-1".into(),
            task_fingerprint: "fp-1".into(),
            kind: CandidateArtifactKind::Macro,
            status: CandidateArtifactStatus::Proposed,
            title: "Macro".into(),
            summary: "Macro".into(),
            payload_json: "{}".into(),
            created_at_us: 1,
            updated_at_us: 1,
        };
        let eval = CandidateEvaluationRun {
            run_id: "eval-1".into(),
            candidate_id: "cand-1".into(),
            task_fingerprint: "fp-1".into(),
            sample_count: 3,
            score: 90,
            passed: true,
            notes: String::new(),
            created_at_us: 2,
        };
        let profiles = vec![AgentCapabilityProfile {
            agent_id: "omni".into(),
            class: aria_core::AgentClass::Generalist,
            tool_allowlist: Vec::new(),
            skill_allowlist: Vec::new(),
            mcp_server_allowlist: Vec::new(),
            mcp_tool_allowlist: Vec::new(),
            mcp_prompt_allowlist: Vec::new(),
            mcp_resource_allowlist: Vec::new(),
            filesystem_scopes: Vec::new(),
            retrieval_scopes: Vec::new(),
            delegation_scope: None,
            web_domain_allowlist: Vec::new(),
            web_domain_blocklist: Vec::new(),
            browser_profile_allowlist: Vec::new(),
            browser_action_scope: None,
            browser_session_scope: None,
            crawl_scope: None,
            web_approval_policy: None,
            web_transport_allowlist: Vec::new(),
            requires_elevation: true,
            side_effect_level: SideEffectLevel::Privileged,
            trust_profile: None,
        }];

        let record = build_candidate_promotion_record(
            &candidate,
            Some(&eval),
            &profiles,
            CandidatePromotionAction::Promote,
            None,
            100,
        );
        assert_eq!(record.status, CandidatePromotionStatus::Blocked);
        assert!(record.requires_human_approval);
    }

    #[test]
    fn build_candidate_promotion_record_applies_after_passed_eval_and_approval() {
        let candidate = CandidateArtifactRecord {
            candidate_id: "cand-2".into(),
            task_fingerprint: "fp-2".into(),
            kind: CandidateArtifactKind::Prompt,
            status: CandidateArtifactStatus::Proposed,
            title: "Prompt".into(),
            summary: "Prompt".into(),
            payload_json: "{}".into(),
            created_at_us: 1,
            updated_at_us: 1,
        };
        let eval = CandidateEvaluationRun {
            run_id: "eval-2".into(),
            candidate_id: "cand-2".into(),
            task_fingerprint: "fp-2".into(),
            sample_count: 3,
            score: 90,
            passed: true,
            notes: String::new(),
            created_at_us: 2,
        };

        let record = build_candidate_promotion_record(
            &candidate,
            Some(&eval),
            &[],
            CandidatePromotionAction::Promote,
            Some("u1".into()),
            100,
        );
        assert_eq!(record.status, CandidatePromotionStatus::Applied);
        assert!(!record.requires_human_approval);
    }

    #[test]
    fn train_selector_models_for_samples_builds_router_hint_and_tool_ranker() {
        let samples = vec![
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "req-1".into(),
                    session_id: "sess-1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "write the file".into(),
                    tool_names: vec!["write_file".into(), "read_file".into()],
                    retrieved_corpora: Vec::new(),
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 1,
                    response_summary: "ok".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 1,
                },
                rewards: Vec::new(),
                reward_score: 1,
            },
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "req-2".into(),
                    session_id: "sess-1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "write the file".into(),
                    tool_names: vec!["write_file".into()],
                    retrieved_corpora: Vec::new(),
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 1,
                    response_summary: "ok".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 2,
                },
                rewards: Vec::new(),
                reward_score: 1,
            },
        ];

        let models = train_selector_models_for_samples("fp-1", &samples, 100);
        assert_eq!(models.len(), 2);
        assert!(models
            .iter()
            .any(|m| m.kind == SelectorModelKind::RouterHint));
        let tool_ranker = models
            .iter()
            .find(|m| m.kind == SelectorModelKind::ToolRanker)
            .expect("tool ranker");
        assert!(tool_ranker.payload_json.contains("write_file"));
    }

    #[test]
    fn build_prompt_optimization_dataset_collects_accepted_prompt_samples() {
        let samples = vec![
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "r1".into(),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "summarize release notes".into(),
                    tool_names: vec![],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 5,
                    response_summary: "succinct release summary".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 1,
                },
                rewards: vec![],
                reward_score: 1,
            },
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "r2".into(),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "summarize release notes".into(),
                    tool_names: vec![],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 5,
                    response_summary: "short release digest".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 2,
                },
                rewards: vec![],
                reward_score: 2,
            },
        ];

        let dataset = build_prompt_optimization_dataset("fp-1", &samples).expect("prompt dataset");
        assert_eq!(dataset.agent_id, "developer");
        assert_eq!(dataset.prompt_mode, "execution");
        assert_eq!(dataset.success_count, 2);
        assert_eq!(dataset.cumulative_reward, 3);
        assert_eq!(dataset.examples.len(), 2);
    }

    #[test]
    fn compile_prompt_candidate_from_dataset_produces_prompt_artifact() {
        let dataset = PromptOptimizationDataset {
            task_fingerprint: "fp-1".into(),
            agent_id: "developer".into(),
            prompt_mode: "execution".into(),
            examples: vec![PromptOptimizationExample {
                user_input_summary: "summarize release notes".into(),
                response_summary: "short release digest".into(),
                retrieved_corpora: vec!["workspace".into()],
                reward_score: 2,
            }],
            success_count: 3,
            cumulative_reward: 4,
        };

        let candidate = compile_prompt_candidate_from_dataset(&dataset, 100);
        assert_eq!(candidate.kind, CandidateArtifactKind::Prompt);
        assert!(candidate.summary.contains("Compiled prompt dataset"));
        assert!(candidate
            .payload_json
            .contains("\"compiled_from\":\"prompt_optimization_dataset\""));
        assert!(candidate.payload_json.contains("\"response_patterns\""));
    }

    #[test]
    fn build_macro_compilation_dataset_collects_dominant_tool_sequence() {
        let samples = vec![
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "r1".into(),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "prepare release file".into(),
                    tool_names: vec!["read_file".into(), "write_file".into()],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 5,
                    response_summary: "release file created".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 1,
                },
                rewards: vec![],
                reward_score: 1,
            },
            ReplaySample {
                trace: ExecutionTrace {
                    request_id: "r2".into(),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "prepare release file".into(),
                    tool_names: vec!["read_file".into(), "write_file".into()],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 5,
                    response_summary: "release file created".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: 2,
                },
                rewards: vec![],
                reward_score: 2,
            },
        ];

        let dataset = build_macro_compilation_dataset("fp-1", &samples).expect("macro dataset");
        assert_eq!(dataset.agent_id, "developer");
        assert_eq!(dataset.prompt_mode, "execution");
        assert_eq!(dataset.success_count, 2);
        assert_eq!(
            dataset.dominant_tool_sequence,
            vec!["read_file".to_string(), "write_file".to_string()]
        );
    }

    #[test]
    fn compile_macro_candidate_from_dataset_produces_macro_artifact() {
        let dataset = MacroCompilationDataset {
            task_fingerprint: "fp-1".into(),
            agent_id: "developer".into(),
            prompt_mode: "execution".into(),
            dominant_tool_sequence: vec!["read_file".into(), "write_file".into()],
            examples: vec![MacroWorkflowExample {
                user_input_summary: "prepare release file".into(),
                response_summary: "release file created".into(),
                tool_names: vec!["read_file".into(), "write_file".into()],
                reward_score: 2,
            }],
            success_count: 3,
            cumulative_reward: 4,
        };

        let candidate = compile_macro_candidate_from_dataset(&dataset, 100);
        assert_eq!(candidate.kind, CandidateArtifactKind::Macro);
        assert!(candidate.summary.contains("dominant tool sequence"));
        assert!(candidate
            .payload_json
            .contains("\"compiled_from\":\"macro_compilation_dataset\""));
        assert!(candidate
            .payload_json
            .contains("\"tools\":[\"read_file\",\"write_file\"]"));
    }

    #[test]
    fn build_wasm_compilation_dataset_requires_deterministic_sequence() {
        let samples = (0..5)
            .map(|i| ReplaySample {
                trace: ExecutionTrace {
                    request_id: format!("r{}", i),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "prepare release file".into(),
                    tool_names: vec!["read_file".into(), "write_file".into()],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 5,
                    response_summary: "release file created".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: i,
                },
                rewards: vec![],
                reward_score: 1,
            })
            .collect::<Vec<_>>();

        let dataset = build_wasm_compilation_dataset("fp-1", &samples).expect("wasm dataset");
        assert_eq!(dataset.success_count, 5);
        assert_eq!(
            dataset.deterministic_tool_sequence,
            vec!["read_file".to_string(), "write_file".to_string()]
        );
    }

    #[test]
    fn compile_wasm_candidate_from_dataset_produces_blocked_activation_artifact() {
        let dataset = WasmCompilationDataset {
            task_fingerprint: "fp-1".into(),
            agent_id: "developer".into(),
            prompt_mode: "execution".into(),
            deterministic_tool_sequence: vec!["read_file".into(), "write_file".into()],
            example_inputs: vec!["prepare release file".into()],
            success_count: 5,
            cumulative_reward: 5,
        };

        let candidate = compile_wasm_candidate_from_dataset(&dataset, 100);
        assert_eq!(candidate.kind, CandidateArtifactKind::Wasm);
        assert!(candidate
            .summary
            .contains("Requires strict eval and approval"));
        assert!(candidate
            .payload_json
            .contains("\"compiled_from\":\"wasm_compilation_dataset\""));
        assert!(candidate
            .payload_json
            .contains("\"activation\":\"blocked_until_manual_runtime_support\""));
    }

    #[test]
    fn evaluate_candidate_against_replay_requires_stricter_wasm_determinism() {
        let candidate = CandidateArtifactRecord {
            candidate_id: "wasm:fp-1".into(),
            task_fingerprint: "fp-1".into(),
            kind: CandidateArtifactKind::Wasm,
            status: CandidateArtifactStatus::Proposed,
            title: "Wasm".into(),
            summary: "Wasm".into(),
            payload_json: "{}".into(),
            created_at_us: 1,
            updated_at_us: 1,
        };
        let samples = (0..5)
            .map(|i| ReplaySample {
                trace: ExecutionTrace {
                    request_id: format!("r{}", i),
                    session_id: "s1".into(),
                    user_id: "u1".into(),
                    agent_id: "developer".into(),
                    channel: GatewayChannel::Cli,
                    prompt_mode: "execution".into(),
                    task_fingerprint: TaskFingerprint {
                        version: 1,
                        key: "fp-1".into(),
                    },
                    user_input_summary: "prepare release file".into(),
                    tool_names: vec!["read_file".into(), "write_file".into()],
                    retrieved_corpora: vec!["workspace".into()],
                    outcome: TraceOutcome::Succeeded,
                    latency_ms: 1,
                    response_summary: "done".into(),
                    tool_runtime_policy: None,
                    recorded_at_us: i,
                },
                rewards: vec![],
                reward_score: 1,
            })
            .collect::<Vec<_>>();

        let run = evaluate_candidate_against_replay(&candidate, &samples, 100);
        assert!(run.passed);
        assert!(run.notes.contains("deterministic_sequence=true"));
    }
}
