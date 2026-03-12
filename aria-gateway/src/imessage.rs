use aria_core::{AgentRequest, GatewayChannel, InboundEnvelope};
use serde::Deserialize;

use crate::{
    normalizer::{build_text_envelope, inbound_envelope_to_request},
    GatewayError,
};

#[derive(Debug, Deserialize)]
struct IMessagePayload {
    sender_id: String,
    thread_id: u64,
    body: String,
    timestamp_us: u64,
}

pub struct IMessageNormalizer;

impl IMessageNormalizer {
    pub fn normalize_envelope(json: &str) -> Result<InboundEnvelope, GatewayError> {
        let payload: IMessagePayload =
            serde_json::from_str(json).map_err(|e| GatewayError::ParseError(e.to_string()))?;
        Ok(build_text_envelope(
            GatewayChannel::IMessage,
            payload.sender_id,
            payload.thread_id,
            payload.thread_id,
            payload.body,
            payload.timestamp_us,
        ))
    }

    pub fn normalize(json: &str) -> Result<AgentRequest, GatewayError> {
        Self::normalize_envelope(json).map(inbound_envelope_to_request)
    }
}
