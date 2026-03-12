use super::*;

pub type ProviderId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider_id: ProviderId,
    pub model_id: String,
}

impl ModelRef {
    pub fn new(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            model_id: model_id.into(),
        }
    }

    pub fn as_slash_ref(&self) -> String {
        format!("{}/{}", self.provider_id, self.model_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupport {
    Supported,
    Unsupported,
    Unknown,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSchemaMode {
    StrictJsonSchema,
    ReducedJsonSchema,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultMode {
    NativeStructured,
    TextBlock,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolModality {
    Text,
    Image,
    Audio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterFamily {
    OpenAiCompatible,
    Anthropic,
    GoogleGemini,
    OllamaNative,
    TextOnlyCli,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySourceKind {
    LocalOverride,
    RuntimeProbe,
    ProviderCatalog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilityProfile {
    pub provider_id: ProviderId,
    pub adapter_family: AdapterFamily,
    pub supports_model_listing: CapabilitySupport,
    pub supports_runtime_probe: CapabilitySupport,
    pub source: CapabilitySourceKind,
    pub observed_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilityProfile {
    pub model_ref: ModelRef,
    pub adapter_family: AdapterFamily,
    pub tool_calling: CapabilitySupport,
    pub parallel_tool_calling: CapabilitySupport,
    pub streaming: CapabilitySupport,
    pub vision: CapabilitySupport,
    pub json_mode: CapabilitySupport,
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    pub tool_schema_mode: ToolSchemaMode,
    pub tool_result_mode: ToolResultMode,
    pub supports_images: CapabilitySupport,
    pub supports_audio: CapabilitySupport,
    pub source: CapabilitySourceKind,
    #[serde(default)]
    pub source_detail: Option<String>,
    pub observed_at_us: u64,
    #[serde(default)]
    pub expires_at_us: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilityProbeRecord {
    pub probe_id: String,
    pub model_ref: ModelRef,
    pub adapter_family: AdapterFamily,
    pub tool_calling: CapabilitySupport,
    pub parallel_tool_calling: CapabilitySupport,
    pub streaming: CapabilitySupport,
    pub vision: CapabilitySupport,
    pub json_mode: CapabilitySupport,
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    pub supports_images: CapabilitySupport,
    pub supports_audio: CapabilitySupport,
    #[serde(default)]
    pub schema_acceptance: Option<CapabilitySupport>,
    #[serde(default)]
    pub native_tool_probe: Option<CapabilitySupport>,
    #[serde(default)]
    pub modality_probe: Option<CapabilitySupport>,
    pub source: CapabilitySourceKind,
    #[serde(default)]
    pub probe_method: Option<String>,
    #[serde(default)]
    pub probe_status: Option<String>,
    #[serde(default)]
    pub probe_error: Option<String>,
    #[serde(default)]
    pub raw_summary: Option<String>,
    pub observed_at_us: u64,
    #[serde(default)]
    pub expires_at_us: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallingMode {
    NativeTools,
    CompatTools,
    TextFallbackNoTools,
    TextFallbackWithRepair,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoicePolicy {
    Auto,
    None,
    Required,
    Specific(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRuntimePolicy {
    pub tool_choice: ToolChoicePolicy,
    pub allow_parallel_tool_calls: bool,
}

impl Default for ToolRuntimePolicy {
    fn default() -> Self {
        Self {
            tool_choice: ToolChoicePolicy::Auto,
            allow_parallel_tool_calls: true,
        }
    }
}
