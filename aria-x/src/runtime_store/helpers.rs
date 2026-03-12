use super::*;

pub(super) fn trace_outcome_name(outcome: TraceOutcome) -> &'static str {
    match outcome {
        TraceOutcome::Succeeded => "succeeded",
        TraceOutcome::Failed => "failed",
        TraceOutcome::ClarificationRequired => "clarification_required",
        TraceOutcome::ApprovalRequired => "approval_required",
    }
}

pub(super) fn agent_run_status_name(status: aria_core::AgentRunStatus) -> &'static str {
    match status {
        aria_core::AgentRunStatus::Queued => "queued",
        aria_core::AgentRunStatus::Running => "running",
        aria_core::AgentRunStatus::Completed => "completed",
        aria_core::AgentRunStatus::Failed => "failed",
        aria_core::AgentRunStatus::Cancelled => "cancelled",
        aria_core::AgentRunStatus::TimedOut => "timed_out",
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn reward_kind_name(kind: RewardKind) -> &'static str {
    match kind {
        RewardKind::Accepted => "accepted",
        RewardKind::Rejected => "rejected",
        RewardKind::Edited => "edited",
        RewardKind::Retried => "retried",
        RewardKind::OverrideApplied => "override_applied",
    }
}

pub(super) fn count_rows(conn: &Connection, table: &str) -> Result<i64, String> {
    let query = format!("SELECT COUNT(*) FROM {}", table);
    conn.query_row(&query, [], |row| row.get::<_, i64>(0))
        .map_err(|e| format!("count rows for {} failed: {}", table, e))
}

pub(super) fn candidate_kind_name(kind: aria_learning::CandidateArtifactKind) -> &'static str {
    match kind {
        aria_learning::CandidateArtifactKind::Prompt => "prompt",
        aria_learning::CandidateArtifactKind::Macro => "macro",
        aria_learning::CandidateArtifactKind::Wasm => "wasm",
    }
}

pub(super) fn candidate_status_name(
    status: aria_learning::CandidateArtifactStatus,
) -> &'static str {
    match status {
        aria_learning::CandidateArtifactStatus::Proposed => "proposed",
        aria_learning::CandidateArtifactStatus::Evaluating => "evaluating",
        aria_learning::CandidateArtifactStatus::Promoted => "promoted",
        aria_learning::CandidateArtifactStatus::Rejected => "rejected",
    }
}

pub(super) fn candidate_promotion_action_name(action: CandidatePromotionAction) -> &'static str {
    match action {
        CandidatePromotionAction::Promote => "promote",
        CandidatePromotionAction::Rollback => "rollback",
    }
}

pub(super) fn candidate_promotion_status_name(status: CandidatePromotionStatus) -> &'static str {
    match status {
        CandidatePromotionStatus::Applied => "applied",
        CandidatePromotionStatus::Blocked => "blocked",
    }
}

pub(super) fn selector_model_kind_name(kind: SelectorModelKind) -> &'static str {
    match kind {
        SelectorModelKind::RouterHint => "router_hint",
        SelectorModelKind::ToolRanker => "tool_ranker",
    }
}

pub(super) fn control_document_kind_name(kind: aria_core::ControlDocumentKind) -> &'static str {
    match kind {
        aria_core::ControlDocumentKind::Instructions => "instructions",
        aria_core::ControlDocumentKind::Skills => "skills",
        aria_core::ControlDocumentKind::Tools => "tools",
        aria_core::ControlDocumentKind::Memory => "memory",
    }
}

pub(super) fn compaction_status_name(status: aria_core::CompactionStatus) -> &'static str {
    match status {
        aria_core::CompactionStatus::Idle => "idle",
        aria_core::CompactionStatus::Queued => "queued",
        aria_core::CompactionStatus::Running => "running",
        aria_core::CompactionStatus::Succeeded => "succeeded",
        aria_core::CompactionStatus::Failed => "failed",
    }
}

pub(super) fn learning_derivative_kind_name(kind: LearningDerivativeKind) -> &'static str {
    match kind {
        LearningDerivativeKind::CandidateSynthesis => "candidate_synthesis",
        LearningDerivativeKind::PromptCompile => "prompt_compile",
        LearningDerivativeKind::MacroCompile => "macro_compile",
        LearningDerivativeKind::WasmCompile => "wasm_compile",
        LearningDerivativeKind::SelectorSynthesis => "selector_synthesis",
        LearningDerivativeKind::Promotion => "promotion",
        LearningDerivativeKind::Rollback => "rollback",
    }
}
