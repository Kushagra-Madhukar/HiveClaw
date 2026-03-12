use super::{
    adapter_for_family, collect_sse_like_stream, default_model_capability_profile, LLMBackend,
    ModelMetadata, ModelProvider, SecretRef,
};
use crate::{CachedTool, ExecutedToolCall, LLMResponse, OrchestratorError, ToolCall};
use aria_core::{
    AdapterFamily, CapabilitySourceKind, CapabilitySupport, ModelCapabilityProbeRecord,
    ModelCapabilityProfile, ModelRef, ToolCallingMode, ToolRuntimePolicy,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct AnthropicBackend {
    pub api_key: SecretRef,
    pub model: String,
    pub base_url: String,
    capability_profile: ModelCapabilityProfile,
    client: reqwest::Client,
}

impl AnthropicBackend {
    pub fn new(api_key: SecretRef, model: impl Into<String>, base_url: impl Into<String>) -> Self {
        let model = model.into();
        Self {
            api_key,
            capability_profile: default_model_capability_profile(
                "anthropic",
                &model,
                AdapterFamily::Anthropic,
                0,
            ),
            model,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn with_capability_profile(
        api_key: SecretRef,
        profile: ModelCapabilityProfile,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            api_key,
            model: profile.model_ref.model_id.clone(),
            capability_profile: profile,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap_or_default(),
        }
    }

    fn translated_tool_definitions(&self, tools: &[CachedTool]) -> Vec<Value> {
        let adapter = adapter_for_family(self.capability_profile.adapter_family);
        adapter
            .filter_tools(&self.capability_profile, tools)
            .iter()
            .filter_map(|tool| {
                let mut value = adapter
                    .translate_tool_definition(&self.capability_profile, tool)
                    .ok()?;
                let function = value.get_mut("function")?.take();
                Some(json!({
                    "name": function["name"],
                    "description": function["description"],
                    "input_schema": function["parameters"]
                }))
            })
            .collect::<Vec<_>>()
    }

    fn build_tool_follow_up_body(
        &self,
        prompt: &str,
        tool_defs: &[Value],
        executed_tools: &[ExecutedToolCall],
    ) -> Value {
        let assistant_content = executed_tools
            .iter()
            .map(|entry| {
                json!({
                    "type": "tool_use",
                    "id": entry.call.invocation_id.clone().unwrap_or_else(|| format!("toolu_{}", entry.call.name)),
                    "name": entry.call.name,
                    "input": serde_json::from_str::<Value>(&entry.call.arguments).unwrap_or_else(|_| json!({}))
                })
            })
            .collect::<Vec<_>>();
        let user_results = executed_tools
            .iter()
            .map(|entry| {
                json!({
                    "type": "tool_result",
                    "tool_use_id": entry.call.invocation_id.clone().unwrap_or_else(|| format!("toolu_{}", entry.call.name)),
                    "content": entry.result.as_model_provider_payload(&entry.call.name).to_string()
                })
            })
            .collect::<Vec<_>>();
        let mut body = json!({
            "model": self.model,
            "max_tokens": 2048,
            "messages": [
                { "role": "user", "content": prompt },
                { "role": "assistant", "content": assistant_content },
                { "role": "user", "content": user_results }
            ]
        });
        if !tool_defs.is_empty() {
            body["tools"] = Value::Array(tool_defs.to_vec());
            body["tool_choice"] = json!({"type":"auto"});
        }
        body
    }
}

#[async_trait]
impl LLMBackend for AnthropicBackend {
    async fn query(
        &self,
        prompt: &str,
        tools: &[CachedTool],
    ) -> Result<LLMResponse, OrchestratorError> {
        self.query_with_policy(prompt, tools, &ToolRuntimePolicy::default())
            .await
    }

    async fn query_with_policy(
        &self,
        prompt: &str,
        tools: &[CachedTool],
        policy: &ToolRuntimePolicy,
    ) -> Result<LLMResponse, OrchestratorError> {
        let api_key = self.api_key.resolve("api.anthropic.com")?;
        let url = format!("{}/messages", self.base_url);
        let adapter = adapter_for_family(self.capability_profile.adapter_family);
        let filtered_tools = adapter.filter_tools(&self.capability_profile, tools);
        let tool_mode = adapter.tool_calling_mode(&self.capability_profile);
        let tool_defs = filtered_tools
            .iter()
            .filter_map(|tool| {
                let mut value = adapter
                    .translate_tool_definition(&self.capability_profile, tool)
                    .ok()?;
                let function = value.get_mut("function")?.take();
                Some(json!({
                    "name": function["name"],
                    "description": function["description"],
                    "input_schema": function["parameters"]
                }))
            })
            .collect::<Vec<_>>();

        let mut body = json!({
            "model": self.model,
            "max_tokens": 2048,
            "messages": [{ "role": "user", "content": prompt }]
        });
        if matches!(
            tool_mode,
            ToolCallingMode::NativeTools | ToolCallingMode::CompatTools
        ) && !tool_defs.is_empty()
        {
            super::apply_anthropic_tool_policy(&mut body, &tool_defs, policy);
        }

        let resp = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
            .map_err(|e| OrchestratorError::LLMError(format!("Anthropic request failed: {}", e)))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(OrchestratorError::LLMError(format!(
                "Anthropic returned {}: {}",
                status, text
            )));
        }
        let res_json: serde_json::Value = resp.json().await.map_err(|e| {
            OrchestratorError::LLMError(format!("Anthropic JSON parse failed: {}", e))
        })?;
        if let Some(content) = res_json["content"].as_array() {
            let mut tool_calls = Vec::new();
            let mut text_parts = Vec::new();
            for block in content {
                match block["type"].as_str() {
                    Some("tool_use") => {
                        if let Some(name) = block["name"].as_str() {
                            tool_calls.push(ToolCall {
                                invocation_id: block["id"].as_str().map(|v| v.to_string()),
                                name: name.to_string(),
                                arguments: block["input"].to_string(),
                            });
                        }
                    }
                    Some("text") => {
                        if let Some(text) = block["text"].as_str() {
                            text_parts.push(text.to_string());
                        }
                    }
                    _ => {}
                }
            }
            if !tool_calls.is_empty() {
                return Ok(LLMResponse::ToolCalls(tool_calls));
            }
            if !text_parts.is_empty() {
                return Ok(LLMResponse::TextAnswer(text_parts.join("\n")));
            }
        }
        Err(OrchestratorError::LLMError(
            "Anthropic returned no content".into(),
        ))
    }

    async fn query_stream_with_policy(
        &self,
        prompt: &str,
        tools: &[CachedTool],
        policy: &ToolRuntimePolicy,
    ) -> Result<LLMResponse, OrchestratorError> {
        let api_key = self.api_key.resolve("api.anthropic.com")?;
        let url = format!("{}/messages", self.base_url);
        let adapter = adapter_for_family(self.capability_profile.adapter_family);
        let filtered_tools = adapter.filter_tools(&self.capability_profile, tools);
        let tool_mode = adapter.tool_calling_mode(&self.capability_profile);
        let tool_defs = filtered_tools
            .iter()
            .filter_map(|tool| {
                let mut value = adapter
                    .translate_tool_definition(&self.capability_profile, tool)
                    .ok()?;
                let function = value.get_mut("function")?.take();
                Some(json!({
                    "name": function["name"],
                    "description": function["description"],
                    "input_schema": function["parameters"]
                }))
            })
            .collect::<Vec<_>>();
        let mut body = json!({
            "model": self.model,
            "max_tokens": 2048,
            "messages": [{ "role": "user", "content": prompt }],
            "stream": true
        });
        if matches!(
            tool_mode,
            ToolCallingMode::NativeTools | ToolCallingMode::CompatTools
        ) && !tool_defs.is_empty()
        {
            super::apply_anthropic_tool_policy(&mut body, &tool_defs, policy);
        }
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                OrchestratorError::LLMError(format!("Anthropic streaming request failed: {}", e))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(OrchestratorError::LLMError(format!(
                "Anthropic streaming returned {}: {}",
                status, text
            )));
        }
        collect_sse_like_stream(resp, adapter).await
    }

    async fn query_with_tool_results(
        &self,
        prompt: &str,
        tools: &[CachedTool],
        executed_tools: &[ExecutedToolCall],
    ) -> Result<LLMResponse, OrchestratorError> {
        self.query_with_tool_results_and_policy(
            prompt,
            tools,
            executed_tools,
            &ToolRuntimePolicy::default(),
        )
        .await
    }

    async fn query_with_tool_results_and_policy(
        &self,
        prompt: &str,
        tools: &[CachedTool],
        executed_tools: &[ExecutedToolCall],
        policy: &ToolRuntimePolicy,
    ) -> Result<LLMResponse, OrchestratorError> {
        let api_key = self.api_key.resolve("api.anthropic.com")?;
        let url = format!("{}/messages", self.base_url);
        let tool_defs = self.translated_tool_definitions(tools);
        let mut body = self.build_tool_follow_up_body(prompt, &tool_defs, executed_tools);
        super::apply_anthropic_tool_policy(&mut body, &tool_defs, policy);
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
            .map_err(|e| OrchestratorError::LLMError(format!("Anthropic request failed: {}", e)))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(OrchestratorError::LLMError(format!(
                "Anthropic returned {}: {}",
                status, text
            )));
        }
        let res_json: serde_json::Value = resp.json().await.map_err(|e| {
            OrchestratorError::LLMError(format!("Anthropic JSON parse failed: {}", e))
        })?;
        if let Some(content) = res_json["content"].as_array() {
            let mut tool_calls = Vec::new();
            let mut text_parts = Vec::new();
            for block in content {
                match block["type"].as_str() {
                    Some("tool_use") => {
                        if let Some(name) = block["name"].as_str() {
                            tool_calls.push(ToolCall {
                                invocation_id: block["id"].as_str().map(|v| v.to_string()),
                                name: name.to_string(),
                                arguments: block["input"].to_string(),
                            });
                        }
                    }
                    Some("text") => {
                        if let Some(text) = block["text"].as_str() {
                            text_parts.push(text.to_string());
                        }
                    }
                    _ => {}
                }
            }
            if !tool_calls.is_empty() {
                return Ok(LLMResponse::ToolCalls(tool_calls));
            }
            if !text_parts.is_empty() {
                return Ok(LLMResponse::TextAnswer(text_parts.join("\n")));
            }
        }
        Err(OrchestratorError::LLMError(
            "Anthropic returned no content".into(),
        ))
    }

    async fn query_stream_with_tool_results_and_policy(
        &self,
        prompt: &str,
        tools: &[CachedTool],
        executed_tools: &[ExecutedToolCall],
        policy: &ToolRuntimePolicy,
    ) -> Result<LLMResponse, OrchestratorError> {
        let api_key = self.api_key.resolve("api.anthropic.com")?;
        let url = format!("{}/messages", self.base_url);
        let tool_defs = self.translated_tool_definitions(tools);
        let mut body = self.build_tool_follow_up_body(prompt, &tool_defs, executed_tools);
        body["stream"] = json!(true);
        super::apply_anthropic_tool_policy(&mut body, &tool_defs, policy);
        let resp = self
            .client
            .post(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                OrchestratorError::LLMError(format!("Anthropic streaming request failed: {}", e))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(OrchestratorError::LLMError(format!(
                "Anthropic streaming returned {}: {}",
                status, text
            )));
        }
        let adapter = adapter_for_family(self.capability_profile.adapter_family);
        collect_sse_like_stream(resp, adapter).await
    }

    fn model_ref(&self) -> Option<ModelRef> {
        Some(self.capability_profile.model_ref.clone())
    }

    fn capability_profile(&self) -> Option<ModelCapabilityProfile> {
        Some(self.capability_profile.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ToolCall, ToolExecutionResult};
    use aria_core::{
        CapabilitySourceKind, ToolChoicePolicy, ToolResultMode, ToolRuntimePolicy, ToolSchemaMode,
    };

    fn backend() -> AnthropicBackend {
        AnthropicBackend::with_capability_profile(
            SecretRef::Literal(String::from("test-key")),
            ModelCapabilityProfile {
                model_ref: ModelRef::new("anthropic", "claude-sonnet-4-20250514"),
                adapter_family: AdapterFamily::Anthropic,
                tool_calling: CapabilitySupport::Supported,
                parallel_tool_calling: CapabilitySupport::Unknown,
                streaming: CapabilitySupport::Supported,
                vision: CapabilitySupport::Unknown,
                json_mode: CapabilitySupport::Supported,
                max_context_tokens: Some(200_000),
                tool_schema_mode: ToolSchemaMode::StrictJsonSchema,
                tool_result_mode: ToolResultMode::NativeStructured,
                supports_images: CapabilitySupport::Unknown,
                supports_audio: CapabilitySupport::Unsupported,
                source: CapabilitySourceKind::LocalOverride,
                source_detail: Some(String::from("test")),
                observed_at_us: 1,
                expires_at_us: None,
            },
            "https://api.anthropic.com/v1",
        )
    }

    fn tool() -> CachedTool {
        CachedTool {
            name: String::from("write_file"),
            description: String::from("Write a file"),
            parameters_schema: String::from(
                r#"{"path":{"type":"string"},"content":{"type":"string"}}"#,
            ),
            embedding: Vec::new(),
            requires_strict_schema: false,
            streaming_safe: false,
            parallel_safe: true,
            modalities: vec![aria_core::ToolModality::Text],
        }
    }

    fn executed_tool() -> ExecutedToolCall {
        ExecutedToolCall {
            call: ToolCall {
                invocation_id: Some(String::from("toolu_123")),
                name: String::from("write_file"),
                arguments: String::from(r#"{"path":"notes.txt","content":"hello"}"#),
            },
            result: ToolExecutionResult::structured(
                "write succeeded",
                "write_file",
                json!({"ok": true, "bytes": 5}),
            ),
        }
    }

    #[test]
    fn anthropic_follow_up_body_uses_tool_use_and_tool_result_blocks() {
        let backend = backend();
        let tool_defs = backend.translated_tool_definitions(&[tool()]);
        let body = backend.build_tool_follow_up_body("save this", &tool_defs, &[executed_tool()]);

        assert_eq!(body["messages"][0]["role"], json!("user"));
        assert_eq!(body["messages"][1]["role"], json!("assistant"));
        assert_eq!(body["messages"][1]["content"][0]["type"], json!("tool_use"));
        assert_eq!(body["messages"][1]["content"][0]["id"], json!("toolu_123"));
        assert_eq!(body["messages"][2]["role"], json!("user"));
        assert_eq!(
            body["messages"][2]["content"][0]["type"],
            json!("tool_result")
        );
        assert_eq!(
            body["messages"][2]["content"][0]["tool_use_id"],
            json!("toolu_123")
        );
        assert_eq!(body["tools"][0]["name"], json!("write_file"));
    }

    #[test]
    fn anthropic_tool_policy_can_force_specific_tool() {
        let backend = backend();
        let tool_defs = backend.translated_tool_definitions(&[tool()]);
        let mut body =
            backend.build_tool_follow_up_body("save this", &tool_defs, &[executed_tool()]);
        super::super::apply_anthropic_tool_policy(
            &mut body,
            &tool_defs,
            &ToolRuntimePolicy {
                tool_choice: ToolChoicePolicy::Specific(String::from("write_file")),
                allow_parallel_tool_calls: true,
            },
        );

        assert_eq!(body["tool_choice"]["type"], json!("tool"));
        assert_eq!(body["tool_choice"]["name"], json!("write_file"));
    }
}

pub struct AnthropicProvider {
    pub api_key: SecretRef,
    pub base_url: String,
}

#[derive(Debug, Deserialize)]
struct AnthropicModel {
    id: String,
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnthropicModelsResponse {
    data: Vec<AnthropicModel>,
}

#[async_trait]
impl ModelProvider for AnthropicProvider {
    fn id(&self) -> &str {
        "anthropic"
    }

    fn name(&self) -> &str {
        "Anthropic"
    }

    fn adapter_family(&self) -> AdapterFamily {
        AdapterFamily::Anthropic
    }

    fn provider_capability_profile(
        &self,
        observed_at_us: u64,
    ) -> aria_core::ProviderCapabilityProfile {
        aria_core::ProviderCapabilityProfile {
            provider_id: self.id().to_string(),
            adapter_family: self.adapter_family(),
            supports_model_listing: CapabilitySupport::Supported,
            supports_runtime_probe: CapabilitySupport::Supported,
            source: CapabilitySourceKind::ProviderCatalog,
            observed_at_us,
        }
    }

    async fn list_models(&self) -> Result<Vec<ModelMetadata>, OrchestratorError> {
        let api_key = self.api_key.resolve("api.anthropic.com")?;
        let url = format!("{}/models", self.base_url.trim_end_matches('/'));
        let resp = reqwest::Client::new()
            .get(&url)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .send()
            .await
            .map_err(|e| {
                OrchestratorError::LLMError(format!("Anthropic list models failed: {}", e))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(OrchestratorError::LLMError(format!(
                "Anthropic models list returned {}: {}",
                status, text
            )));
        }
        let json: AnthropicModelsResponse = resp.json().await.map_err(|e| {
            OrchestratorError::LLMError(format!("Anthropic models JSON failed: {}", e))
        })?;
        Ok(json
            .data
            .into_iter()
            .map(|m| ModelMetadata {
                name: m.display_name.unwrap_or_else(|| m.id.clone()),
                id: m.id,
                description: None,
                context_length: None,
            })
            .collect())
    }

    fn create_backend(&self, model_id: &str) -> Result<Box<dyn LLMBackend>, OrchestratorError> {
        Ok(Box::new(AnthropicBackend::new(
            self.api_key.clone(),
            model_id,
            self.base_url.clone(),
        )))
    }

    fn create_backend_with_profile(
        &self,
        profile: &ModelCapabilityProfile,
    ) -> Result<Box<dyn LLMBackend>, OrchestratorError> {
        Ok(Box::new(AnthropicBackend::with_capability_profile(
            self.api_key.clone(),
            profile.clone(),
            self.base_url.clone(),
        )))
    }

    async fn probe_model_capabilities(
        &self,
        model_id: &str,
        observed_at_us: u64,
    ) -> Result<ModelCapabilityProbeRecord, OrchestratorError> {
        let lower = model_id.to_ascii_lowercase();
        let supports_images = if lower.contains("claude-3") || lower.contains("claude-sonnet-4") {
            CapabilitySupport::Supported
        } else {
            CapabilitySupport::Unknown
        };
        Ok(ModelCapabilityProbeRecord {
            probe_id: format!("probe-anthropic-{}-{}", model_id, observed_at_us),
            model_ref: ModelRef::new("anthropic", model_id),
            adapter_family: AdapterFamily::Anthropic,
            tool_calling: CapabilitySupport::Supported,
            parallel_tool_calling: CapabilitySupport::Unknown,
            streaming: CapabilitySupport::Supported,
            vision: supports_images,
            json_mode: CapabilitySupport::Supported,
            max_context_tokens: None,
            supports_images,
            supports_audio: CapabilitySupport::Unknown,
            schema_acceptance: Some(CapabilitySupport::Supported),
            native_tool_probe: Some(CapabilitySupport::Supported),
            modality_probe: Some(supports_images),
            source: CapabilitySourceKind::RuntimeProbe,
            probe_method: Some(String::from("models_api+heuristic")),
            probe_status: Some(String::from("success")),
            probe_error: None,
            raw_summary: Some(format!("anthropic probe for '{}'", model_id)),
            observed_at_us,
            expires_at_us: Some(observed_at_us + 86_400_000_000),
        })
    }
}
