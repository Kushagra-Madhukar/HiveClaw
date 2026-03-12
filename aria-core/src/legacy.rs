use super::*;

/// Legacy v0 representation of `AgentRequest` before `MessageContent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyAgentRequestV0 {
    pub request_id: Uuid,
    pub session_id: Uuid,
    pub channel: GatewayChannel,
    pub user_id: String,
    /// Plain text content field in older payloads.
    pub content: String,
    pub timestamp_us: u64,
}

/// Legacy v0 representation of `AgentResponse` before structured traces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyAgentResponseV0 {
    pub request_id: Uuid,
    /// Plain text content field in older payloads.
    pub content: String,
    /// Free-form trace strings in older recordings.
    pub skill_trace: Vec<String>,
    pub latency_ms: u32,
}

impl From<LegacyAgentRequestV0> for AgentRequest {
    fn from(v0: LegacyAgentRequestV0) -> Self {
        AgentRequest {
            request_id: v0.request_id,
            session_id: v0.session_id,
            channel: v0.channel,
            user_id: v0.user_id,
            content: MessageContent::Text(v0.content),
            tool_runtime_policy: None,
            timestamp_us: v0.timestamp_us,
        }
    }
}

impl From<LegacyAgentResponseV0> for AgentResponse {
    fn from(v0: LegacyAgentResponseV0) -> Self {
        AgentResponse {
            request_id: v0.request_id,
            content: MessageContent::Text(v0.content),
            skill_trace: v0
                .skill_trace
                .into_iter()
                .map(|trace| SkillExecutionRecord {
                    tool_name: String::from("legacy"),
                    arguments_json: String::new(),
                    result_summary: trace,
                    duration_ms: 0,
                    policy_decision: PolicyDecision::Allow,
                })
                .collect(),
            latency_ms: v0.latency_ms,
        }
    }
}

impl AgentRequest {
    /// Attempt to parse either a v0 or v1 JSON-encoded request.
    pub fn from_json_any_version(json: &str) -> Result<Self, AriaError> {
        // First try the current format.
        if let Ok(current) = serde_json::from_str::<AgentRequest>(json) {
            return Ok(current);
        }
        // Fallback to legacy v0.
        let legacy: LegacyAgentRequestV0 = serde_json::from_str(json)
            .map_err(|e: serde_json::Error| AriaError::SerializationError(e.to_string()))?;
        Ok(legacy.into())
    }
}

impl AgentResponse {
    /// Attempt to parse either a v0 or v1 JSON-encoded response.
    pub fn from_json_any_version(json: &str) -> Result<Self, AriaError> {
        // First try the current format.
        if let Ok(current) = serde_json::from_str::<AgentResponse>(json) {
            return Ok(current);
        }
        // Fallback to legacy v0.
        let legacy: LegacyAgentResponseV0 = serde_json::from_str(json)
            .map_err(|e: serde_json::Error| AriaError::SerializationError(e.to_string()))?;
        Ok(legacy.into())
    }
}
