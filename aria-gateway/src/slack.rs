use aria_core::{AgentRequest, GatewayChannel, InboundEnvelope};
use serde::Deserialize;

use crate::{
    normalizer::{build_text_envelope, inbound_envelope_to_request},
    GatewayError,
};

#[derive(Debug, Deserialize)]
struct SlackPayload {
    user: String,
    channel: String,
    text: String,
    timestamp_us: u64,
}

pub struct SlackNormalizer;

impl SlackNormalizer {
    pub fn normalize_envelope(json: &str) -> Result<InboundEnvelope, GatewayError> {
        let payload: SlackPayload =
            serde_json::from_str(json).map_err(|e| GatewayError::ParseError(e.to_string()))?;
        let channel_seed = payload.channel.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(16777619).wrapping_add(u64::from(b))
        });
        Ok(build_text_envelope(
            GatewayChannel::Slack,
            payload.user,
            channel_seed,
            channel_seed,
            payload.text,
            payload.timestamp_us,
        ))
    }

    pub fn normalize(json: &str) -> Result<AgentRequest, GatewayError> {
        Self::normalize_envelope(json).map(inbound_envelope_to_request)
    }
}
