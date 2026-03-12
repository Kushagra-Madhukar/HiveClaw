use super::*;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Unified error type for aria-core operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AriaError {
    /// Serialization or deserialization failed.
    SerializationError(String),
    /// A required field was invalid or missing.
    ValidationError(String),
}

impl fmt::Display for AriaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AriaError::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            AriaError::ValidationError(msg) => write!(f, "validation error: {}", msg),
        }
    }
}
