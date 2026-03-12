use aria_core::{AgentRequest, GatewayChannel, InboundEnvelope, MessageContent, ToolRuntimePolicy};

/// Build a normalized `AgentRequest` from channel inputs.
pub fn build_text_request(
    channel: GatewayChannel,
    user_id: String,
    session_seed: u64,
    request_seed: u64,
    text: String,
    timestamp_us: u64,
) -> AgentRequest {
    inbound_envelope_to_request(build_text_envelope(
        channel,
        user_id,
        session_seed,
        request_seed,
        text,
        timestamp_us,
    ))
}

/// Build a channel-agnostic inbound envelope from channel text payload.
pub fn build_text_envelope(
    channel: GatewayChannel,
    user_id: String,
    session_seed: u64,
    request_seed: u64,
    text: String,
    timestamp_us: u64,
) -> InboundEnvelope {
    let mut envelope_id = [0u8; 16];
    envelope_id[0..8].copy_from_slice(&request_seed.to_le_bytes());
    envelope_id[8..16].copy_from_slice(&timestamp_us.to_le_bytes());

    let mut session_id = [0u8; 16];
    session_id[0..8].copy_from_slice(&session_seed.to_le_bytes());

    InboundEnvelope {
        envelope_id,
        session_id,
        channel,
        user_id,
        provider_message_id: Some(request_seed.to_string()),
        content: MessageContent::Text(text),
        attachments: Vec::new(),
        timestamp_us,
    }
}

/// Convert an inbound envelope into legacy `AgentRequest` for orchestrator paths.
pub fn inbound_envelope_to_request(envelope: InboundEnvelope) -> AgentRequest {
    let (content, tool_runtime_policy) = project_content_with_tool_runtime_policy(envelope.content);
    AgentRequest {
        request_id: envelope.envelope_id,
        session_id: envelope.session_id,
        channel: envelope.channel,
        user_id: envelope.user_id,
        content,
        tool_runtime_policy,
        timestamp_us: envelope.timestamp_us,
    }
}

pub fn project_content_with_tool_runtime_policy(
    content: MessageContent,
) -> (MessageContent, Option<ToolRuntimePolicy>) {
    match content {
        MessageContent::Text(text) => {
            let (text, policy) = extract_tool_runtime_policy_from_text(&text);
            (MessageContent::Text(text), policy)
        }
        other => (other, None),
    }
}

fn extract_tool_runtime_policy_from_text(text: &str) -> (String, Option<ToolRuntimePolicy>) {
    let Some((first_line, remainder)) = text.split_once('\n') else {
        return (text.to_string(), None);
    };
    let directive = first_line.trim();
    let Some(policy_json) = directive.strip_prefix("::tool-policy ") else {
        return (text.to_string(), None);
    };
    let Ok(policy) = serde_json::from_str::<ToolRuntimePolicy>(policy_json.trim()) else {
        return (text.to_string(), None);
    };
    (remainder.trim_start().to_string(), Some(policy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aria_core::ToolChoicePolicy;

    #[test]
    fn inbound_envelope_projects_text_tool_runtime_policy() {
        let request = inbound_envelope_to_request(InboundEnvelope {
            envelope_id: [1; 16],
            session_id: [2; 16],
            channel: GatewayChannel::Cli,
            user_id: "u1".into(),
            provider_message_id: Some("1".into()),
            content: MessageContent::Text(
                "::tool-policy {\"tool_choice\":\"required\",\"allow_parallel_tool_calls\":false}\nlist files".into(),
            ),
            attachments: Vec::new(),
            timestamp_us: 42,
        });

        assert_eq!(request.content, MessageContent::Text("list files".into()));
        assert_eq!(
            request.tool_runtime_policy,
            Some(ToolRuntimePolicy {
                tool_choice: ToolChoicePolicy::Required,
                allow_parallel_tool_calls: false,
            })
        );
    }

    #[test]
    fn invalid_tool_policy_directive_degrades_safely_to_plain_text() {
        let request = inbound_envelope_to_request(InboundEnvelope {
            envelope_id: [1; 16],
            session_id: [2; 16],
            channel: GatewayChannel::Cli,
            user_id: "u1".into(),
            provider_message_id: Some("1".into()),
            content: MessageContent::Text("::tool-policy not-json\nlist files".into()),
            attachments: Vec::new(),
            timestamp_us: 42,
        });

        assert_eq!(
            request.content,
            MessageContent::Text("::tool-policy not-json\nlist files".into())
        );
        assert_eq!(request.tool_runtime_policy, None);
    }
}
