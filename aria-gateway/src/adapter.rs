use aria_core::{AgentRequest, InboundEnvelope};

use crate::GatewayError;

/// Async trait for inbound signal adapters.
#[async_trait::async_trait]
pub trait GatewayAdapter: Send + Sync {
    /// Receive and normalize the next inbound signal.
    async fn receive(&self) -> Result<AgentRequest, GatewayError>;

    /// Receive and normalize the next inbound signal into a channel-agnostic envelope.
    async fn receive_envelope(&self) -> Result<InboundEnvelope, GatewayError> {
        let req = self.receive().await?;
        Ok(InboundEnvelope {
            envelope_id: req.request_id,
            session_id: req.session_id,
            channel: req.channel,
            user_id: req.user_id,
            provider_message_id: None,
            content: req.content,
            attachments: Vec::new(),
            timestamp_us: req.timestamp_us,
        })
    }
}
