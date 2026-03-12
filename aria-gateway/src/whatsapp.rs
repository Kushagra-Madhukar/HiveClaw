use aria_core::{AgentRequest, GatewayChannel, InboundEnvelope};
use serde::Deserialize;

use crate::{
    normalizer::{build_text_envelope, inbound_envelope_to_request},
    GatewayError,
};

#[derive(Debug, Deserialize)]
struct WhatsAppPayload {
    user_id: String,
    chat_id: u64,
    text: String,
    timestamp_us: u64,
}

pub struct WhatsAppNormalizer;

impl WhatsAppNormalizer {
    pub fn normalize_envelope(json: &str) -> Result<InboundEnvelope, GatewayError> {
        let payload: WhatsAppPayload =
            serde_json::from_str(json).map_err(|e| GatewayError::ParseError(e.to_string()))?;
        Ok(build_text_envelope(
            GatewayChannel::WhatsApp,
            payload.user_id,
            payload.chat_id,
            payload.chat_id,
            payload.text,
            payload.timestamp_us,
        ))
    }

    pub fn normalize(json: &str) -> Result<AgentRequest, GatewayError> {
        Self::normalize_envelope(json).map(inbound_envelope_to_request)
    }
}
