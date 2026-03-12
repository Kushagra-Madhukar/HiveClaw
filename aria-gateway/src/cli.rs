use aria_core::{AgentRequest, GatewayChannel, InboundEnvelope};

use crate::normalizer::{build_text_envelope, inbound_envelope_to_request};

pub struct CliNormalizer;

impl CliNormalizer {
    pub fn normalize_line_envelope(
        user_id: &str,
        session_seed: u64,
        line: &str,
        timestamp_us: u64,
    ) -> InboundEnvelope {
        build_text_envelope(
            GatewayChannel::Cli,
            user_id.to_string(),
            session_seed,
            session_seed,
            line.to_string(),
            timestamp_us,
        )
    }

    pub fn normalize_line(
        user_id: &str,
        session_seed: u64,
        line: &str,
        timestamp_us: u64,
    ) -> AgentRequest {
        inbound_envelope_to_request(Self::normalize_line_envelope(
            user_id,
            session_seed,
            line,
            timestamp_us,
        ))
    }
}
