fn native_tool_vault_store(
) -> &'static std::sync::Mutex<Option<Arc<aria_vault::CredentialVault>>> {
    &app_runtime().native_tool_vault
}

fn set_native_tool_vault(vault: Arc<aria_vault::CredentialVault>) {
    if let Ok(mut slot) = native_tool_vault_store().lock() {
        *slot = Some(vault);
    }
}

fn native_tool_vault() -> Result<Arc<aria_vault::CredentialVault>, OrchestratorError> {
    native_tool_vault_store()
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
        .ok_or_else(|| {
            OrchestratorError::ToolError(
                "credential vault unavailable for native browser login tools".into(),
            )
        })
}

fn decode_tool_args<T>(call: &ToolCall) -> Result<T, OrchestratorError>
where
    T: serde::de::DeserializeOwned,
{
    let mut deserializer = serde_json::Deserializer::from_str(&call.arguments);
    serde_path_to_error::deserialize(&mut deserializer).map_err(|err| {
        OrchestratorError::ToolError(format!(
            "Invalid args for '{}': {} at {}",
            call.name,
            err.inner(),
            err.path()
        ))
    })
}

fn normalize_mcp_endpoint_for_policy(endpoint: &str) -> String {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return endpoint.to_string();
    }
    let parts = trimmed.split_whitespace().collect::<Vec<_>>();
    if parts.len() >= 2 {
        let candidate = parts[parts.len() - 1].trim_matches(|c: char| "\"'".contains(c));
        if candidate.starts_with('/')
            || candidate.starts_with("./")
            || candidate.starts_with("../")
            || candidate.ends_with(".sh")
        {
            return candidate.to_string();
        }
    }
    trimmed.to_string()
}

fn structured_payload<T>(
    summary: impl Into<String>,
    kind: impl Into<String>,
    payload: &T,
) -> ToolExecutionResult
where
    T: serde::Serialize,
{
    ToolExecutionResult::structured(
        summary,
        kind,
        serde_json::to_value(payload).unwrap_or_else(|_| serde_json::json!({})),
    )
}

fn required_trimmed(value: &str, field: &str) -> Result<String, OrchestratorError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(OrchestratorError::ToolError(format!(
            "Missing '{}'",
            field
        )));
    }
    Ok(trimmed.to_string())
}

#[cfg(not(feature = "mcp-runtime"))]
fn disabled_feature_tool_error(call: &ToolCall, feature: &str) -> OrchestratorError {
    OrchestratorError::ToolError(format!(
        "Tool '{}' requires the '{}' feature, which is disabled in this build",
        call.name, feature
    ))
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ReadFileRequest {
    path: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct WriteFileRequest {
    path: String,
    content: String,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RunShellRequest {
    command: String,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    max_output_bytes: Option<u64>,
    #[serde(default)]
    cpu_seconds: Option<u64>,
    #[serde(default)]
    memory_kb: Option<u64>,
    #[serde(default)]
    os_containment: Option<bool>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ScheduleMessageRequest {
    task: String,
    #[serde(default)]
    schedule: Option<ToolSchedule>,
    #[serde(default)]
    delay: Option<serde_json::Value>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    execution_mode: Option<String>,
    #[serde(default)]
    deferred_prompt: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RunIdRequest {
    run_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SkillScaffoldRequest {
    skill_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    target_dir: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SkillDirRequest {
    skill_dir: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SkillExportRequest {
    skill_id: String,
    #[serde(default)]
    output_dir: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SignedSkillExportRequest {
    skill_id: String,
    signing_key_hex: String,
    #[serde(default)]
    output_dir: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct InstallSignedSkillDirRequest {
    skill_dir: String,
    #[serde(default)]
    expected_public_key_hex: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct InstallSkillRequest {
    #[serde(default)]
    manifest_toml: Option<String>,
    #[serde(default)]
    manifest: Option<SkillPackageManifest>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BindSkillRequest {
    skill_id: String,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    activation_policy: Option<String>,
    #[serde(default)]
    required_version: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ActivateSkillRequest {
    skill_id: String,
    #[serde(default)]
    run_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ExecuteSkillRequest {
    skill_id: String,
    #[serde(default)]
    function_name: Option<String>,
    #[serde(default)]
    input: Option<String>,
}

#[cfg(feature = "mcp-runtime")]
#[derive(Debug, Clone, serde::Deserialize)]
struct BindMcpImportRequest {
    server_id: String,
    primitive_kind: String,
    target_name: String,
    #[serde(default)]
    agent_id: Option<String>,
}

#[cfg(feature = "mcp-runtime")]
#[derive(Debug, Clone, serde::Deserialize)]
struct InvokeMcpToolRequest {
    server_id: String,
    tool_name: String,
    #[serde(default)]
    input: serde_json::Value,
}

#[cfg(feature = "mcp-runtime")]
#[derive(Debug, Clone, serde::Deserialize)]
struct RenderMcpPromptRequest {
    server_id: String,
    prompt_name: String,
    #[serde(default)]
    arguments: serde_json::Value,
}

#[cfg(feature = "mcp-runtime")]
#[derive(Debug, Clone, serde::Deserialize)]
struct ReadMcpResourceRequest {
    server_id: String,
    resource_uri: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SearchCodebaseRequest {
    query: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RunTestsRequest {
    #[serde(default)]
    target: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ManageCronRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    schedule: Option<ToolSchedule>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct CompactSessionRequest {
    #[serde(default)]
    threshold: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct GrantAccessRequest {
    principal: String,
    action: String,
    resource: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ManagePromptsRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    template: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SetDomainAccessDecisionRequest {
    domain: String,
    decision: aria_core::DomainDecisionKind,
    #[serde(default)]
    action_family: Option<aria_core::WebActionFamily>,
    #[serde(default)]
    scope: Option<aria_core::DomainDecisionScope>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserProfileCreateRequest {
    #[serde(default, alias = "id")]
    profile_id: Option<String>,
    #[serde(default)]
    #[serde(alias = "name")]
    display_name: Option<String>,
    #[serde(default)]
    engine: Option<aria_core::BrowserEngine>,
    #[serde(default)]
    mode: Option<aria_core::BrowserProfileMode>,
    #[serde(default)]
    allowed_domains: Vec<String>,
    #[serde(default)]
    auth_enabled: Option<bool>,
    #[serde(default)]
    write_enabled: Option<bool>,
    #[serde(default)]
    persistent: Option<bool>,
    #[serde(default)]
    set_as_default: Option<bool>,
    #[serde(default)]
    attached_source: Option<String>,
    #[serde(default)]
    extension_binding_id: Option<String>,
}

fn derive_browser_profile_id(
    explicit_profile_id: Option<&str>,
    display_name: Option<&str>,
) -> Result<String, OrchestratorError> {
    if let Some(profile_id) = explicit_profile_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(profile_id.to_string());
    }
    if let Some(name) = display_name.map(str::trim).filter(|value| !value.is_empty()) {
        let slug = name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
            .split('-')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("-");
        if !slug.is_empty() {
            return Ok(slug);
        }
    }
    Err(OrchestratorError::ToolError(
        "browser_profile_create requires profile_id or name".into(),
    ))
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserProfileUseRequest {
    profile_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserSessionStartRequest {
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserSessionIdRequest {
    browser_session_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserSessionPersistStateRequest {
    browser_session_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserChallengeRequest {
    browser_session_id: String,
    challenge: aria_core::BrowserChallengeKind,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserLoginStatusRequest {
    #[serde(default)]
    browser_session_id: Option<String>,
    #[serde(default)]
    domain: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserSessionStatusRequest {
    #[serde(default)]
    browser_session_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserLoginCompleteRequest {
    #[serde(default)]
    browser_session_id: Option<String>,
    domain: String,
    #[serde(default)]
    credential_key_names: Vec<String>,
    #[serde(default)]
    state: Option<aria_core::BrowserLoginStateKind>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserLoginCredentialEntry {
    key_name: String,
    #[serde(default)]
    selector: Option<serde_json::Value>,
    #[serde(default)]
    field: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserLoginFillCredentialsRequest {
    #[serde(default)]
    browser_session_id: Option<String>,
    domain: String,
    credentials: Vec<BrowserLoginCredentialEntry>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserSessionCleanupRequest {
    #[serde(default)]
    browser_session_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct BrowserSessionResponse {
    #[serde(flatten)]
    session: aria_core::BrowserSessionRecord,
    #[serde(default)]
    reused_login_state: Option<aria_core::BrowserLoginStateRecord>,
    transport: aria_core::BrowserTransportKind,
}

#[derive(Debug, Clone, serde::Serialize)]
struct BrowserExtractResponse {
    artifact: aria_core::BrowserArtifactRecord,
    text: String,
    #[serde(default)]
    title: Option<String>,
    headings: Vec<String>,
    #[serde(default)]
    excerpt: Option<String>,
    extraction_profile: String,
    #[serde(default)]
    site_adapter: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct WebExtractResponse {
    url: String,
    content_type: String,
    text: String,
    #[serde(default)]
    title: Option<String>,
    headings: Vec<String>,
    #[serde(default)]
    excerpt: Option<String>,
    extraction_profile: String,
    #[serde(default)]
    site_adapter: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct WebFetchResponse {
    url: String,
    content_type: String,
    body: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct UrlRequest {
    url: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserOpenRequest {
    url: String,
    #[serde(default)]
    profile_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserArtifactRequest {
    url: String,
    #[serde(default)]
    browser_session_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct BrowserDownloadRequest {
    url: String,
    #[serde(default)]
    browser_session_id: Option<String>,
    #[serde(default)]
    filename: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct CrawlRequest {
    url: String,
    #[serde(default)]
    scope: Option<aria_core::CrawlScope>,
    #[serde(default)]
    max_depth: Option<u64>,
    #[serde(default)]
    max_pages: Option<u64>,
    #[serde(default)]
    allowed_domains: Vec<String>,
    #[serde(default)]
    render_js: Option<bool>,
    #[serde(default)]
    capture_screenshots: Option<bool>,
    #[serde(default)]
    change_detection: Option<bool>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct WatchRequest {
    url: String,
    schedule: ToolSchedule,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    capture_screenshots: Option<bool>,
    #[serde(default)]
    change_detection: Option<bool>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

impl NativeToolExecutor {
    fn scoped_idempotency_key(&self, tool_name: &str, raw_key: &str) -> String {
        let session = self
            .session_id
            .map(uuid::Uuid::from_bytes)
            .map(|id| id.to_string())
            .unwrap_or_else(|| "global".to_string());
        format!("{}:{}:{}", session, tool_name, raw_key)
    }

    fn append_shell_exec_audit(&self, record: ShellExecutionAuditRecord) {
        let Some(sessions_dir) = self.sessions_dir.as_ref() else {
            return;
        };
        let _ = RuntimeStore::for_sessions_dir(&sessions_dir).append_shell_exec_audit(&record);
    }

    fn sessions_dir_required(&self, tool_name: &str) -> Result<&Path, OrchestratorError> {
        self.sessions_dir.as_deref().ok_or_else(|| {
            OrchestratorError::ToolError(format!("{} requires session store availability", tool_name))
        })
    }

    fn session_and_agent_required(
        &self,
        tool_name: &str,
    ) -> Result<(aria_core::Uuid, String), OrchestratorError> {
        let session_id = self.session_id.ok_or_else(|| {
            OrchestratorError::ToolError(format!("{} requires session context", tool_name))
        })?;
        let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
            OrchestratorError::ToolError(format!(
                "{} requires invoking agent context",
                tool_name
            ))
        })?;
        Ok((session_id, agent_id))
    }

    fn resolve_browser_profile(
        &self,
        tool_name: &str,
        explicit_profile_id: Option<&str>,
    ) -> Result<(aria_core::Uuid, String, PathBuf, aria_core::BrowserProfile), OrchestratorError> {
        let sessions_dir = self.sessions_dir_required(tool_name)?.to_path_buf();
        let (session_id, agent_id) = self.session_and_agent_required(tool_name)?;
        let profile = if let Some(profile_id) = explicit_profile_id.map(str::trim).filter(|v| !v.is_empty()) {
            RuntimeStore::for_sessions_dir(&sessions_dir)
                .list_browser_profiles()
                .map_err(OrchestratorError::ToolError)?
                .into_iter()
                .find(|profile| profile.profile_id == profile_id)
                .ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "browser profile '{}' does not exist",
                        profile_id
                    ))
                })?
        } else {
            current_browser_profile_for_agent(&sessions_dir, session_id, &agent_id)?.ok_or_else(
                || {
                    OrchestratorError::ToolError(format!(
                        "{} requires a bound browser profile or explicit profile_id",
                        tool_name
                    ))
                },
            )?
        };
        Ok((session_id, agent_id, sessions_dir, profile))
    }

    fn resolve_browser_session(
        &self,
        tool_name: &str,
        explicit_browser_session_id: Option<&str>,
    ) -> Result<
        (
            aria_core::Uuid,
            String,
            PathBuf,
            aria_core::BrowserSessionRecord,
        ),
        OrchestratorError,
    > {
        let sessions_dir = self.sessions_dir_required(tool_name)?.to_path_buf();
        let (session_id, agent_id) = self.session_and_agent_required(tool_name)?;
        let mut sessions = RuntimeStore::for_sessions_dir(&sessions_dir)
            .list_browser_sessions(Some(session_id), Some(&agent_id))
            .map_err(OrchestratorError::ToolError)?;
        sessions.sort_by_key(|record| std::cmp::Reverse(record.updated_at_us));
        let browser_session = if let Some(browser_session_id) =
            explicit_browser_session_id.map(str::trim).filter(|value| !value.is_empty())
        {
            sessions
                .into_iter()
                .find(|record| record.browser_session_id == browser_session_id)
                .ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "browser session '{}' not found",
                        browser_session_id
                    ))
                })?
        } else {
            sessions
                .into_iter()
                .find(|record| {
                    matches!(
                        record.status,
                        aria_core::BrowserSessionStatus::Launched
                            | aria_core::BrowserSessionStatus::Paused
                    )
                })
                .ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "{} requires an active browser session or explicit browser_session_id",
                        tool_name
                    ))
                })?
        };
        Ok((session_id, agent_id, sessions_dir, browser_session))
    }

    async fn acquire_call_lease(
        &self,
        call: &ToolCall,
    ) -> Result<Option<ResourceLeaseClaim>, OrchestratorError> {
        let Some(resource_key) = self.resource_lease_key_for_call(call) else {
            return Ok(None);
        };
        let sessions_dir = match self.sessions_dir.as_deref() {
            Some(path) => path,
            None => return Ok(None),
        };
        let holder_id = self.resource_lease_holder_id(call);
        acquire_resource_lease_with_retry(
            sessions_dir,
            &resource_key,
            &holder_id,
            30,
            3,
            25,
            &format!("tool '{}' busy for {}", call.name, resource_key),
        )
        .await
        .map(Some)
    }

    fn resource_lease_holder_id(&self, call: &ToolCall) -> String {
        let session = self
            .session_id
            .map(uuid::Uuid::from_bytes)
            .map(|id| id.to_string())
            .unwrap_or_else(|| "global".to_string());
        let agent = self
            .invoking_agent_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("unknown-agent");
        let invocation = call
            .invocation_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("direct");
        format!(
            "tool:{}:{}:{}:{}",
            crate::runtime_instance_id(),
            session,
            agent,
            invocation
        )
    }

    fn resource_lease_key_for_call(&self, call: &ToolCall) -> Option<String> {
        let args: serde_json::Value = serde_json::from_str(&call.arguments).ok()?;
        let arg_str = |key: &str| -> Option<String> {
            args.get(key)
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
        };
        let session_key = self
            .session_id
            .map(uuid::Uuid::from_bytes)
            .map(|id| id.to_string())
            .unwrap_or_else(|| "global".to_string());
        let agent_key = self
            .invoking_agent_id
            .clone()
            .unwrap_or_else(|| "unknown-agent".to_string());
        match call.name.as_str() {
            "write_file" => arg_str("path").map(|path| format!("fs:{}", path)),
            "run_shell" => {
                let command = arg_str("command")?;
                let cwd = arg_str("cwd").unwrap_or_else(|| ".".to_string());
                Some(format!("shell:{}:{}", cwd, command))
            }
            "schedule_message" | "set_reminder" | "manage_cron" => {
                Some(format!("scheduler:{}:{}", session_key, agent_key))
            }
            "spawn_agent" => Some(format!("agent-run:{}:{}", session_key, agent_key)),
            "set_domain_access_decision" => {
                let domain = arg_str("domain")?;
                let target_agent = arg_str("agent_id").unwrap_or_else(|| agent_key.clone());
                Some(format!("domain-policy:{}:{}", target_agent, domain))
            }
            "install_skill" => arg_str("skill_dir")
                .map(|skill_dir| format!("skill-install:{}", skill_dir)),
            "bind_skill" | "unbind_skill" | "activate_skill" | "deactivate_skill"
            | "execute_skill" => {
                let skill_id = arg_str("skill_id")?;
                let target_agent = arg_str("agent_id").unwrap_or_else(|| agent_key.clone());
                Some(format!("skill-binding:{}:{}", target_agent, skill_id))
            }
            "import_mcp_tool" | "import_mcp_prompt" | "import_mcp_resource" => {
                let server_id = arg_str("server_id")?;
                Some(format!("mcp-import:{}:{}", server_id, call.name))
            }
            "read_mcp_resource" => {
                let server_id = arg_str("server_id")?;
                let resource_uri = arg_str("resource_uri")?;
                Some(format!("mcp-resource:{}:{}", server_id, resource_uri))
            }
            tool if tool.starts_with("browser_") => {
                if let Some(session_id) = arg_str("browser_session_id") {
                    Some(format!("browser-session:{}", session_id))
                } else if let Some(profile_id) = arg_str("profile_id") {
                    Some(format!("browser-profile:{}", profile_id))
                } else {
                    Some(format!("browser-session:{}:{}", session_key, agent_key))
                }
            }
            tool if tool.starts_with("crawl_") || tool.starts_with("watch_") => {
                let url = arg_str("url")?;
                Some(format!("web-target:{}", url))
            }
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl ToolExecutor for NativeToolExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        let _lease = self.acquire_call_lease(call).await?;
        match call.name.as_str() {
            "read_file" => {
                let request: ReadFileRequest = decode_tool_args(call)?;
                std::fs::read_to_string(&request.path)
                    .map(tool_text_result)
                    .map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "Failed to read {}: {}",
                            request.path, e
                        ))
                    })
            }
            "write_file" => {
                let request: WriteFileRequest = decode_tool_args(call)?;
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .map(|key| self.scoped_idempotency_key(&call.name, key));
                if let Some(key) = idempotency_key.as_deref() {
                    if let Some(cached) = idempotency_lookup(key) {
                        return Ok(cached);
                    }
                }
                if let Some(parent) = std::path::Path::new(&request.path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                std::fs::write(&request.path, &request.content).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "Failed to write {}: {}",
                        request.path, e
                    ))
                })?;
                let result = structured_payload(
                    format!(
                        "Successfully wrote {} bytes to {}",
                        request.content.len(),
                        request.path
                    ),
                    "file_write",
                    &serde_json::json!({"path": request.path, "bytes": request.content.len()}),
                );
                if let Some(key) = idempotency_key {
                    idempotency_store_result(key, result.clone());
                }
                Ok(result)
            }
            "run_shell" => {
                let request: RunShellRequest = decode_tool_args(call)?;
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .map(|key| self.scoped_idempotency_key(&call.name, key));
                if let Some(key) = idempotency_key.as_deref() {
                    if let Some(cached) = idempotency_lookup(key) {
                        return Ok(cached);
                    }
                }
                let command = request.command.as_str();
                let timeout_seconds = request
                    .timeout_seconds
                    .unwrap_or(15)
                    .min(60);
                let max_output_bytes = request
                    .max_output_bytes
                    .unwrap_or(8192)
                    .clamp(512, 65536) as usize;
                let cpu_seconds = request
                    .cpu_seconds
                    .unwrap_or(5)
                    .clamp(1, 30);
                let memory_kb = request
                    .memory_kb
                    .unwrap_or(262_144)
                    .clamp(32_768, 1_048_576);
                let os_containment = request.os_containment.unwrap_or(false);
                let cwd = request.cwd.as_deref();
                let containment_backend = if os_containment {
                    Some(shell_containment_backend_name().to_string())
                } else {
                    None
                };
                let started_at = std::time::Instant::now();
                let created_at_us = chrono::Utc::now().timestamp_micros() as u64;
                #[cfg(unix)]
                let wrapped_command = format!(
                    "ulimit -t {}; ulimit -v {}; {}",
                    cpu_seconds, memory_kb, command
                );
                #[cfg(not(unix))]
                let wrapped_command = command.to_string();
                let mut cmd = if os_containment {
                    match build_os_contained_shell_command(&wrapped_command, cwd) {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            self.append_shell_exec_audit(ShellExecutionAuditRecord {
                                audit_id: format!("shell-{}", uuid::Uuid::new_v4()),
                                session_id: self
                                    .session_id
                                    .map(uuid::Uuid::from_bytes)
                                    .map(|id| id.to_string()),
                                agent_id: self.invoking_agent_id.clone(),
                                command: command.to_string(),
                                cwd: cwd.map(|value| value.to_string()),
                                os_containment_requested: os_containment,
                                containment_backend,
                                timeout_seconds,
                                cpu_seconds,
                                memory_kb,
                                exit_code: None,
                                timed_out: false,
                                output_truncated: false,
                                error: Some(format!("{}", err)),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                created_at_us,
                            });
                            return Err(err);
                        }
                    }
                } else {
                    let mut cmd = tokio::process::Command::new("sh");
                    cmd.arg("-c").arg(wrapped_command);
                    if let Some(cwd) = cwd {
                        cmd.current_dir(cwd);
                    }
                    cmd
                };
                let output = match tokio::time::timeout(Duration::from_secs(timeout_seconds), cmd.output()).await {
                    Err(_) => {
                        self.append_shell_exec_audit(ShellExecutionAuditRecord {
                            audit_id: format!("shell-{}", uuid::Uuid::new_v4()),
                            session_id: self
                                .session_id
                                .map(uuid::Uuid::from_bytes)
                                .map(|id| id.to_string()),
                            agent_id: self.invoking_agent_id.clone(),
                            command: command.to_string(),
                            cwd: cwd.map(|value| value.to_string()),
                            os_containment_requested: os_containment,
                            containment_backend,
                            timeout_seconds,
                            cpu_seconds,
                            memory_kb,
                            exit_code: None,
                            timed_out: true,
                            output_truncated: false,
                            error: Some(format!("run_shell timed out after {} seconds", timeout_seconds)),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            created_at_us,
                        });
                        return Err(OrchestratorError::ToolError(format!(
                            "run_shell timed out after {} seconds",
                            timeout_seconds
                        )));
                    }
                    Ok(Err(e)) => {
                        self.append_shell_exec_audit(ShellExecutionAuditRecord {
                            audit_id: format!("shell-{}", uuid::Uuid::new_v4()),
                            session_id: self
                                .session_id
                                .map(uuid::Uuid::from_bytes)
                                .map(|id| id.to_string()),
                            agent_id: self.invoking_agent_id.clone(),
                            command: command.to_string(),
                            cwd: cwd.map(|value| value.to_string()),
                            os_containment_requested: os_containment,
                            containment_backend,
                            timeout_seconds,
                            cpu_seconds,
                            memory_kb,
                            exit_code: None,
                            timed_out: false,
                            output_truncated: false,
                            error: Some(format!("Failed to execute shell: {}", e)),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            created_at_us,
                        });
                        return Err(OrchestratorError::ToolError(format!(
                            "Failed to execute shell: {}",
                            e
                        )));
                    }
                    Ok(Ok(output)) => output,
                };
                let mut res = String::new();
                if !output.stdout.is_empty() {
                    let bytes = &output.stdout[..output.stdout.len().min(max_output_bytes)];
                    res.push_str(&String::from_utf8_lossy(bytes));
                }
                if !output.stderr.is_empty() {
                    if !res.is_empty() {
                        res.push('\n');
                    }
                    res.push_str("STDERR:\n");
                    let bytes = &output.stderr[..output.stderr.len().min(max_output_bytes)];
                    res.push_str(&String::from_utf8_lossy(bytes));
                }
                let output_truncated =
                    output.stdout.len() > max_output_bytes || output.stderr.len() > max_output_bytes;
                if output_truncated {
                    res.push_str("\n[output truncated]");
                }
                if res.is_empty() {
                    res.push_str("Command executed successfully with no output.");
                }
                self.append_shell_exec_audit(ShellExecutionAuditRecord {
                    audit_id: format!("shell-{}", uuid::Uuid::new_v4()),
                    session_id: self
                        .session_id
                        .map(uuid::Uuid::from_bytes)
                        .map(|id| id.to_string()),
                    agent_id: self.invoking_agent_id.clone(),
                    command: command.to_string(),
                    cwd: cwd.map(|value| value.to_string()),
                    os_containment_requested: os_containment,
                    containment_backend,
                    timeout_seconds,
                    cpu_seconds,
                    memory_kb,
                    exit_code: output.status.code(),
                    timed_out: false,
                    output_truncated,
                    error: None,
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    created_at_us,
                });
                let result = tool_text_result(res);
                if let Some(key) = idempotency_key {
                    idempotency_store_result(key, result.clone());
                }
                Ok(result)
            }
            "schedule_message" | "set_reminder" => {
                let request: ScheduleMessageRequest = decode_tool_args(call)?;
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .map(|key| self.scoped_idempotency_key(&call.name, key));
                if let Some(key) = idempotency_key.as_deref() {
                    if let Some(cached) = idempotency_lookup(key) {
                        return Ok(cached);
                    }
                }
                let task = request.task.trim().to_string();

                if task.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'task'".into()));
                }
                if request.delay.is_some() {
                    return Err(OrchestratorError::ToolError(
                        "Legacy 'delay' is not supported. Use a structured 'schedule' object."
                            .into(),
                    ));
                }

                let classified_schedule = self
                    .scheduling_intent
                    .as_ref()
                    .and_then(|intent| intent.normalized_schedule.clone());
                let schedule_input = if let Some(schedule_value) = request.schedule.clone() {
                    schedule_value
                } else {
                    classified_schedule.clone().ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "Missing 'schedule'. Provide a structured schedule object.".into(),
                        )
                    })?
                };
                let (normalized_delay, spec) = schedule_input
                    .to_schedule_parts(self.user_timezone)
                    .map_err(OrchestratorError::ToolError)?;
                let explicit_agent_id = request.agent_id.as_deref();
                let agent_id = resolve_scheduled_agent_id(
                    explicit_agent_id,
                    self.invoking_agent_id.as_deref(),
                    "this scheduled action",
                )?;
                let creator_agent = self.invoking_agent_id.clone();
                let mode = request
                    .mode
                    .as_deref()
                    .or(request.execution_mode.as_deref())
                    .or_else(|| {
                        self.scheduling_intent
                            .as_ref()
                            .map(|intent| intent.mode.as_tool_mode())
                    })
                    .unwrap_or("notify")
                    .trim()
                    .to_ascii_lowercase();
                let deferred_prompt = request
                    .deferred_prompt
                    .as_deref()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        self.scheduling_intent
                            .as_ref()
                            .and_then(|intent| intent.deferred_task.clone())
                    });
                let mut mode = mode;
                if deferred_prompt.is_some() && mode == "notify" {
                    // If a deferred prompt is present but mode is notify, prefer executing deferred work.
                    // This avoids losing deferred actions due to model inconsistency.
                    mode = self
                        .scheduling_intent
                        .as_ref()
                        .map(|intent| intent.mode.as_tool_mode().to_string())
                        .unwrap_or_else(|| "defer".to_string());
                }
                let deferred_task = deferred_prompt.unwrap_or_else(|| task.clone());
                if !matches!(
                    mode.as_str(),
                    "notify" | "defer" | "deferred" | "execute_later" | "both"
                ) {
                    return Err(OrchestratorError::ToolError(
                        "Invalid mode. Use 'notify', 'defer', or 'both'.".into(),
                    ));
                }

                // De-duplicate identical reminders to prevent repeated LLM tool calls
                // from creating multiple equivalent jobs in the same session.
                let jobs =
                    load_authoritative_scheduler_jobs(&self.tx_cron, self.sessions_dir.as_deref())
                        .await
                        .map_err(|_| {
                            OrchestratorError::ToolError(
                                "Scheduler is unavailable; failed to inspect existing reminders"
                                    .into(),
                            )
                        })?;
                let mut created = Vec::new();
                let mut existing = Vec::new();

                let wants_notify = matches!(mode.as_str(), "notify" | "both");
                let wants_defer = matches!(
                    mode.as_str(),
                    "defer" | "deferred" | "execute_later" | "both"
                );

                if wants_notify {
                    if let Some(found) = jobs.iter().find(|j| {
                        j.kind == aria_intelligence::ScheduledJobKind::Notify
                            && j.agent_id == agent_id
                            && j.prompt == task
                            && j.schedule_str == normalized_delay
                            && j.session_id == self.session_id
                            && j.user_id == self.user_id
                            && j.channel == self.channel
                    }) {
                        existing.push(format!("notify:{}", found.id));
                    } else {
                        let id = format!("reminder-{}", uuid::Uuid::new_v4());
                        let job = aria_intelligence::ScheduledPromptJob {
                            id: id.clone(),
                            agent_id: agent_id.clone(),
                            creator_agent: creator_agent.clone(),
                            executor_agent: None,
                            notifier_agent: Some(agent_id.clone()),
                            prompt: task.clone(),
                            schedule_str: normalized_delay.clone(),
                            kind: aria_intelligence::ScheduledJobKind::Notify,
                            schedule: spec.clone(),
                            session_id: self.session_id,
                            user_id: self.user_id.clone(),
                            channel: self.channel,
                            status: aria_intelligence::ScheduledJobStatus::Scheduled,
                            last_run_at_us: None,
                            last_error: None,
                            audit_log: Vec::new(),
                        };
                        self.tx_cron
                            .send(aria_intelligence::CronCommand::Add(job.clone()))
                            .await
                            .map_err(|_| {
                                OrchestratorError::ToolError(
                                    "Scheduler is unavailable; reminder was not queued".into(),
                                )
                            })?;
                        if let Some(sessions_dir) = &self.sessions_dir {
                            let _ = RuntimeStore::for_sessions_dir(&sessions_dir)
                                .upsert_job_snapshot(
                                    &id,
                                    &job,
                                    chrono::Utc::now().timestamp_micros() as u64,
                                );
                        }
                        created.push(format!("notify:{}", id));
                    }
                }

                if wants_defer {
                    if let Some(found) = jobs.iter().find(|j| {
                        j.kind == aria_intelligence::ScheduledJobKind::Orchestrate
                            && j.agent_id == agent_id
                            && j.prompt == deferred_task
                            && j.schedule_str == normalized_delay
                            && j.session_id == self.session_id
                            && j.user_id == self.user_id
                            && j.channel == self.channel
                    }) {
                        existing.push(format!("defer:{}", found.id));
                    } else {
                        let id = format!("deferred-{}", uuid::Uuid::new_v4());
                        let job = aria_intelligence::ScheduledPromptJob {
                            id: id.clone(),
                            agent_id: agent_id.clone(),
                            creator_agent: creator_agent.clone(),
                            executor_agent: Some(agent_id.clone()),
                            notifier_agent: None,
                            prompt: deferred_task.clone(),
                            schedule_str: normalized_delay.clone(),
                            kind: aria_intelligence::ScheduledJobKind::Orchestrate,
                            schedule: spec.clone(),
                            session_id: self.session_id,
                            user_id: self.user_id.clone(),
                            channel: self.channel,
                            status: aria_intelligence::ScheduledJobStatus::Scheduled,
                            last_run_at_us: None,
                            last_error: None,
                            audit_log: Vec::new(),
                        };
                        self.tx_cron
                            .send(aria_intelligence::CronCommand::Add(job.clone()))
                            .await
                            .map_err(|_| {
                                OrchestratorError::ToolError(
                                    "Scheduler is unavailable; deferred task was not queued".into(),
                                )
                            })?;
                        if let Some(sessions_dir) = &self.sessions_dir {
                            let _ = RuntimeStore::for_sessions_dir(&sessions_dir)
                                .upsert_job_snapshot(
                                    &id,
                                    &job,
                                    chrono::Utc::now().timestamp_micros() as u64,
                                );
                        }
                        created.push(format!("defer:{}", id));
                    }
                }

                if created.is_empty() && !existing.is_empty() {
                    let result = format!("Already scheduled ({})", existing.join(", "));
                    let result = ToolExecutionResult::structured(
                        result,
                        "scheduled_action",
                        serde_json::json!({"existing": existing}),
                    );
                    if let Some(key) = idempotency_key {
                        idempotency_store_result(key, result.clone());
                    }
                    return Ok(result);
                }

                let mode_text = if mode == "both" {
                    "notify + deferred execution"
                } else if wants_defer {
                    "deferred execution"
                } else {
                    "reminder notification"
                };
                let mut msg = format!(
                    "Scheduled {} for '{}' at '{}' (agent: {}).",
                    mode_text, task, normalized_delay, agent_id
                );
                if !created.is_empty() {
                    msg.push_str(&format!(" created=[{}]", created.join(", ")));
                }
                if !existing.is_empty() {
                    msg.push_str(&format!(" existing=[{}]", existing.join(", ")));
                }
                let result = ToolExecutionResult::structured(
                    msg,
                    "scheduled_action",
                    serde_json::json!({
                        "created": created,
                        "existing": existing,
                        "agent_id": agent_id,
                        "mode": mode
                    }),
                );
                if let Some(key) = idempotency_key {
                    idempotency_store_result(key, result.clone());
                }
                Ok(result)
            }
            "spawn_agent" => {
                let request: AgentSpawnRequest = decode_tool_args(call)?;
                if request.agent_id.trim().is_empty() {
                    return Err(OrchestratorError::ToolError(
                        "Missing 'agent_id' for spawn_agent".into(),
                    ));
                }
                if request.prompt.trim().is_empty() {
                    return Err(OrchestratorError::ToolError(
                        "Missing 'prompt' for spawn_agent".into(),
                    ));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "spawn_agent requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError("spawn_agent requires session context".into())
                })?;
                let user_id = self.user_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("spawn_agent requires user context".into())
                })?;

                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let run_id = format!("run-{}", uuid::Uuid::new_v4());
                let parent_run_id = request.parent_run_id.clone().or_else(|| {
                    self.session_id
                        .map(|sid| format!("session:{}", uuid::Uuid::from_bytes(sid)))
                });
                let run = AgentRunRecord {
                    run_id: run_id.clone(),
                    parent_run_id,
                    session_id,
                    user_id,
                    requested_by_agent: self.invoking_agent_id.clone(),
                    agent_id: request.agent_id.clone(),
                    status: AgentRunStatus::Queued,
                    request_text: request.prompt.clone(),
                    inbox_on_completion: true,
                    max_runtime_seconds: request.max_runtime_seconds.or(Some(600)),
                    created_at_us: now_us,
                    started_at_us: None,
                    finished_at_us: None,
                    result: None,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                store
                    .upsert_agent_run(&run, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_agent_run_event(&AgentRunEvent {
                        event_id: format!("evt-{}", uuid::Uuid::new_v4()),
                        run_id: run_id.clone(),
                        kind: AgentRunEventKind::Queued,
                        summary: format!(
                            "queued child agent '{}' for async execution",
                            request.agent_id
                        ),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Queued async child agent '{}' as run '{}'.",
                        request.agent_id, run_id
                    ),
                    "agent_run",
                    serde_json::json!({
                        "run_id": run_id,
                        "status": "queued",
                        "agent_id": request.agent_id,
                    }),
                ))
            }
            "cancel_agent_run" => {
                let request: RunIdRequest = decode_tool_args(call)?;
                let run_id = request.run_id.trim();
                if run_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'run_id'".into()));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "cancel_agent_run requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let existing = store
                    .read_agent_run(run_id)
                    .map_err(OrchestratorError::ToolError)?;
                if let Some(user_id) = &self.user_id {
                    if &existing.user_id != user_id {
                        return Err(OrchestratorError::ToolError(format!(
                            "cancel_agent_run not permitted for run '{}'",
                            run_id
                        )));
                    }
                }
                if let Some(invoking_agent_id) = &self.invoking_agent_id {
                    if existing.requested_by_agent.as_deref() != Some(invoking_agent_id.as_str()) {
                        return Err(OrchestratorError::ToolError(format!(
                            "cancel_agent_run not permitted for run '{}' by agent '{}'",
                            run_id, invoking_agent_id
                        )));
                    }
                }

                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let summary = format!(
                    "cancelled by '{}'",
                    self.invoking_agent_id.as_deref().unwrap_or("user")
                );
                let updated = store
                    .cancel_agent_run(run_id, &summary, now_us)
                    .map_err(OrchestratorError::ToolError)?
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "cancel_agent_run target '{}' not found",
                            run_id
                        ))
                    })?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Cancelled child run '{}' with status '{:?}'.",
                        run_id, updated.status
                    ),
                    "agent_run",
                    serde_json::json!({
                        "run_id": run_id,
                        "status": format!("{:?}", updated.status).to_ascii_lowercase(),
                    }),
                ))
            }
            "retry_agent_run" => {
                let request: RunIdRequest = decode_tool_args(call)?;
                let run_id = request.run_id.trim();
                if run_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'run_id'".into()));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "retry_agent_run requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let existing = store
                    .read_agent_run(run_id)
                    .map_err(OrchestratorError::ToolError)?;
                if let Some(user_id) = &self.user_id {
                    if &existing.user_id != user_id {
                        return Err(OrchestratorError::ToolError(format!(
                            "retry_agent_run not permitted for run '{}'",
                            run_id
                        )));
                    }
                }
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let new_run_id = format!("run-{}", uuid::Uuid::new_v4());
                let retried = AgentRunRecord {
                    run_id: new_run_id.clone(),
                    parent_run_id: existing
                        .parent_run_id
                        .clone()
                        .or_else(|| Some(existing.run_id.clone())),
                    session_id: existing.session_id,
                    user_id: existing.user_id.clone(),
                    requested_by_agent: self.invoking_agent_id.clone(),
                    agent_id: existing.agent_id.clone(),
                    status: AgentRunStatus::Queued,
                    request_text: existing.request_text.clone(),
                    inbox_on_completion: existing.inbox_on_completion,
                    max_runtime_seconds: existing.max_runtime_seconds,
                    created_at_us: now_us,
                    started_at_us: None,
                    finished_at_us: None,
                    result: None,
                };
                store
                    .upsert_agent_run(&retried, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_agent_run_event(&AgentRunEvent {
                        event_id: format!("evt-{}", uuid::Uuid::new_v4()),
                        run_id: retried.run_id.clone(),
                        kind: AgentRunEventKind::Queued,
                        summary: format!("Run retried from '{}'", existing.run_id),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Queued retry run '{}' from '{}'.",
                        retried.run_id, existing.run_id
                    ),
                    "agent_run",
                    serde_json::json!({
                        "run_id": retried.run_id,
                        "status": "queued",
                        "retried_from": existing.run_id,
                    }),
                ))
            }
            "list_agent_runs" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "list_agent_runs requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError("list_agent_runs requires session context".into())
                })?;
                let runs = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_agent_runs_for_session(uuid::Uuid::from_bytes(session_id))
                    .map_err(OrchestratorError::ToolError)?;
                let rows: Vec<serde_json::Value> = runs
                    .iter()
                    .map(|run| {
                        serde_json::json!({
                            "run_id": run.run_id,
                            "status": format!("{:?}", run.status).to_ascii_lowercase(),
                            "agent_id": run.agent_id,
                            "created_at_us": run.created_at_us,
                            "parent_run_id": run.parent_run_id,
                        })
                    })
                    .collect();
                Ok(ToolExecutionResult::structured(
                    format!("Found {} runs for current session.", rows.len()),
                    "agent_run_list",
                    serde_json::json!({ "runs": rows }),
                ))
            }
            "get_agent_run" => {
                let request: RunIdRequest = decode_tool_args(call)?;
                let run_id = request.run_id.trim();
                if run_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'run_id'".into()));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "get_agent_run requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let run = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .read_agent_run(run_id)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Fetched run '{}' ({:?}).", run.run_id, run.status),
                    "agent_run",
                    serde_json::to_value(run).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "get_agent_run_events" => {
                let request: RunIdRequest = decode_tool_args(call)?;
                let run_id = request.run_id.trim();
                if run_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'run_id'".into()));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "get_agent_run_events requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let events = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_agent_run_events(run_id)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Found {} events for run '{}'.", events.len(), run_id),
                    "agent_run_events",
                    serde_json::json!({ "run_id": run_id, "events": events }),
                ))
            }
            "get_agent_mailbox" => {
                let request: RunIdRequest = decode_tool_args(call)?;
                let run_id = request.run_id.trim();
                if run_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'run_id'".into()));
                }
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "get_agent_mailbox requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let messages = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_agent_mailbox_messages(run_id)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Found {} mailbox messages for run '{}'.", messages.len(), run_id),
                    "agent_mailbox",
                    serde_json::json!({ "run_id": run_id, "messages": messages }),
                ))
            }
            "scaffold_skill" => {
                let request: SkillScaffoldRequest = decode_tool_args(call)?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let name = request.name.as_deref().map(str::trim).filter(|v| !v.is_empty()).unwrap_or(skill_id);
                let description = request
                    .description
                    .as_deref()
                    .unwrap_or("Skill scaffold generated by aria-x");
                let version = request.version.as_deref().map(str::trim).filter(|v| !v.is_empty()).unwrap_or("0.1.0");
                let target_dir = request.target_dir.as_deref().map(str::trim).filter(|v| !v.is_empty()).unwrap_or("./skills");
                let skill_dir = Path::new(target_dir).join(skill_id);
                std::fs::create_dir_all(&skill_dir).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "scaffold_skill failed to create '{}': {}",
                        skill_dir.display(),
                        e
                    ))
                })?;
                let manifest_path = skill_dir.join("skill.toml");
                let entry_doc = "SKILL.md";
                let manifest = format!(
                    "skill_id = \"{}\"\nname = \"{}\"\ndescription = \"{}\"\nversion = \"{}\"\nentry_document = \"{}\"\nenabled = true\n",
                    skill_id, name, description, version, entry_doc
                );
                std::fs::write(&manifest_path, manifest).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "scaffold_skill failed to write '{}': {}",
                        manifest_path.display(),
                        e
                    ))
                })?;
                let entry_path = skill_dir.join(entry_doc);
                let entry = format!(
                    "# {}\n\n{}\n\n## Usage\n- Bound and activated per-agent.\n",
                    name, description
                );
                std::fs::write(&entry_path, entry).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "scaffold_skill failed to write '{}': {}",
                        entry_path.display(),
                        e
                    ))
                })?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Scaffolded skill '{}' at '{}'.",
                        skill_id,
                        skill_dir.display()
                    ),
                    "skill_scaffold",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "skill_dir": skill_dir.display().to_string(),
                        "manifest_path": manifest_path.display().to_string(),
                        "entry_path": entry_path.display().to_string()
                    }),
                ))
            }
            "install_skill_from_dir" => {
                let request: SkillDirRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "install_skill_from_dir requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let skill_dir = request.skill_dir.trim();
                if skill_dir.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_dir'".into()));
                }
                let manifest =
                    aria_skill_runtime::load_skill_manifest_from_dir(Path::new(skill_dir))
                        .map_err(|e| OrchestratorError::ToolError(e.to_string()))?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_skill_package(&manifest, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Installed skill '{}' from '{}'.",
                        manifest.skill_id, skill_dir
                    ),
                    "skill_package",
                    serde_json::json!({
                        "skill_id": manifest.skill_id,
                        "skill_dir": skill_dir,
                        "version": manifest.version
                    }),
                ))
            }
            "export_skill_manifest" => {
                let request: SkillExportRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "export_skill_manifest requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let output_dir = request.output_dir.as_deref().map(str::trim).filter(|v| !v.is_empty()).unwrap_or("./skills");
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let manifest = store
                    .list_skill_packages()
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|m| m.skill_id == skill_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "export_skill_manifest unknown skill '{}'",
                            skill_id
                        ))
                    })?;
                let skill_dir = Path::new(output_dir).join(skill_id);
                std::fs::create_dir_all(&skill_dir).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_skill_manifest failed to create '{}': {}",
                        skill_dir.display(),
                        e
                    ))
                })?;
                let toml = toml::to_string_pretty(&manifest).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_skill_manifest serialize failed: {}",
                        e
                    ))
                })?;
                let out_path = skill_dir.join("skill.toml");
                std::fs::write(&out_path, toml).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_skill_manifest failed to write '{}': {}",
                        out_path.display(),
                        e
                    ))
                })?;
                Ok(ToolExecutionResult::structured(
                    format!("Exported skill '{}' to '{}'.", skill_id, out_path.display()),
                    "skill_package",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "output_path": out_path.display().to_string()
                    }),
                ))
            }
            "export_signed_skill_manifest" => {
                let request: SignedSkillExportRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "export_signed_skill_manifest requires runtime persistence (sessions_dir)"
                            .into(),
                    )
                })?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let signing_key_hex = request.signing_key_hex.trim();
                if signing_key_hex.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'signing_key_hex'".into()));
                }
                let output_dir = request.output_dir.as_deref().map(str::trim).filter(|v| !v.is_empty()).unwrap_or("./skills");
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let manifest = store
                    .list_skill_packages()
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|m| m.skill_id == skill_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "export_signed_skill_manifest unknown skill '{}'",
                            skill_id
                        ))
                    })?;
                let signing_key = parse_signing_key_hex(signing_key_hex)?;
                let manifest_toml = toml::to_string_pretty(&manifest).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_signed_skill_manifest serialize failed: {}",
                        e
                    ))
                })?;
                let signature = sign_skill_manifest_bytes(&manifest, manifest_toml.as_bytes(), &signing_key);
                let created_at_us = chrono::Utc::now().timestamp_micros() as u64;
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .append_skill_signature(&SkillSignatureRecord {
                        record_id: format!("sig-{}", uuid::Uuid::new_v4()),
                        skill_id: manifest.skill_id.clone(),
                        version: manifest.version.clone(),
                        algorithm: signature.algorithm.clone(),
                        payload_sha256_hex: signature.payload_sha256_hex.clone(),
                        public_key_hex: signature.public_key_hex.clone(),
                        signature_hex: signature.signature_hex.clone(),
                        source: "export_signed_skill_manifest".into(),
                        verified: true,
                        created_at_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                let skill_dir = Path::new(output_dir).join(skill_id);
                std::fs::create_dir_all(&skill_dir).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_signed_skill_manifest failed to create '{}': {}",
                        skill_dir.display(),
                        e
                    ))
                })?;
                let manifest_path = skill_dir.join("skill.toml");
                std::fs::write(&manifest_path, &manifest_toml).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_signed_skill_manifest failed to write '{}': {}",
                        manifest_path.display(),
                        e
                    ))
                })?;
                let signature_path = skill_dir.join("skill.sig.json");
                let signature_json = serde_json::to_vec_pretty(&signature).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_signed_skill_manifest signature serialize failed: {}",
                        e
                    ))
                })?;
                std::fs::write(&signature_path, signature_json).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "export_signed_skill_manifest failed to write '{}': {}",
                        signature_path.display(),
                        e
                    ))
                })?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Exported and signed skill '{}' at '{}'.",
                        skill_id,
                        skill_dir.display()
                    ),
                    "skill_signed_package",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "manifest_path": manifest_path.display().to_string(),
                        "signature_path": signature_path.display().to_string(),
                        "public_key_hex": signature.public_key_hex
                    }),
                ))
            }
            "install_signed_skill_from_dir" => {
                let request: InstallSignedSkillDirRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "install_signed_skill_from_dir requires runtime persistence (sessions_dir)"
                            .into(),
                    )
                })?;
                let skill_dir = request.skill_dir.trim();
                if skill_dir.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_dir'".into()));
                }
                let expected_public_key_hex = request
                    .expected_public_key_hex
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let skill_dir_path = Path::new(skill_dir);
                let manifest_path = skill_dir_path.join("skill.toml");
                let signature_path = skill_dir_path.join("skill.sig.json");
                let manifest_bytes = std::fs::read(&manifest_path).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "install_signed_skill_from_dir failed to read '{}': {}",
                        manifest_path.display(),
                        e
                    ))
                })?;
                let signature_bytes = std::fs::read(&signature_path).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "install_signed_skill_from_dir failed to read '{}': {}",
                        signature_path.display(),
                        e
                    ))
                })?;
                let signature: SkillManifestSignature =
                    serde_json::from_slice(&signature_bytes).map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "install_signed_skill_from_dir invalid signature envelope: {}",
                            e
                        ))
                    })?;
                verify_signed_skill_manifest(
                    &manifest_bytes,
                    &signature,
                    expected_public_key_hex,
                )?;
                let manifest =
                    aria_skill_runtime::load_skill_manifest_from_dir(skill_dir_path)
                        .map_err(|e| OrchestratorError::ToolError(e.to_string()))?;
                if signature.skill_id != manifest.skill_id || signature.version != manifest.version {
                    return Err(OrchestratorError::ToolError(
                        "install_signed_skill_from_dir signature metadata does not match manifest"
                            .into(),
                    ));
                }
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                store
                    .upsert_skill_package(&manifest, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_skill_signature(&SkillSignatureRecord {
                        record_id: format!("sig-{}", uuid::Uuid::new_v4()),
                        skill_id: manifest.skill_id.clone(),
                        version: manifest.version.clone(),
                        algorithm: signature.algorithm.clone(),
                        payload_sha256_hex: signature.payload_sha256_hex.clone(),
                        public_key_hex: signature.public_key_hex.clone(),
                        signature_hex: signature.signature_hex.clone(),
                        source: "install_signed_skill_from_dir".into(),
                        verified: true,
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Installed signed skill '{}' from '{}'.",
                        manifest.skill_id, skill_dir
                    ),
                    "skill_signed_package",
                    serde_json::json!({
                        "skill_id": manifest.skill_id,
                        "skill_dir": skill_dir,
                        "version": manifest.version,
                        "public_key_hex": signature.public_key_hex
                    }),
                ))
            }
            "install_skill" => {
                let request: InstallSkillRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "install_skill requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let manifest = if let Some(manifest_toml) = request.manifest_toml.as_deref() {
                        aria_skill_runtime::parse_skill_manifest_toml(manifest_toml)
                            .map_err(|e| OrchestratorError::ToolError(e.to_string()))?
                    } else if let Some(manifest) = request.manifest {
                        manifest
                    } else {
                        return Err(OrchestratorError::ToolError(
                            "install_skill requires 'manifest_toml' or 'manifest'".into(),
                        ));
                    };
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_skill_package(&manifest, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Installed skill '{}' (version {}).",
                        manifest.skill_id, manifest.version
                    ),
                    "skill_package",
                    serde_json::json!({
                        "skill_id": manifest.skill_id,
                        "enabled": manifest.enabled,
                        "version": manifest.version,
                    }),
                ))
            }
            "bind_skill" => {
                let request: BindSkillRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "bind_skill requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let agent_id = request
                    .agent_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .or_else(|| self.invoking_agent_id.clone())
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "bind_skill requires 'agent_id' or invoking agent context".into(),
                        )
                    })?;
                let activation_policy = request
                    .activation_policy
                    .as_deref()
                    .map(parse_skill_activation_policy)
                    .transpose()?
                    .unwrap_or(SkillActivationPolicy::Manual);
                let required_version = request
                    .required_version
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|v| v.to_string());
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let manifests = store
                    .list_skill_packages()
                    .map_err(OrchestratorError::ToolError)?;
                let manifest = manifests.into_iter().find(|manifest| manifest.skill_id == skill_id);
                let manifest = if let Some(manifest) = manifest {
                    manifest
                } else {
                    return Err(OrchestratorError::ToolError(format!(
                        "bind_skill unknown skill '{}'",
                        skill_id
                    )));
                };
                if let Some(required_version) = &required_version {
                    if !version_satisfies_requirement(&manifest.version, required_version) {
                        return Err(OrchestratorError::ToolError(format!(
                            "bind_skill version mismatch: installed '{}' does not satisfy '{}'",
                            manifest.version, required_version
                        )));
                    }
                }
                let binding = SkillBinding {
                    binding_id: format!("skill-binding-{}", uuid::Uuid::new_v4()),
                    agent_id: agent_id.clone(),
                    skill_id: skill_id.to_string(),
                    activation_policy,
                    created_at_us: chrono::Utc::now().timestamp_micros() as u64,
                };
                store
                    .upsert_skill_binding(&binding)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Bound skill '{}' to agent '{}'.", skill_id, agent_id),
                    "skill_binding",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "agent_id": agent_id,
                        "activation_policy": format!("{:?}", activation_policy).to_ascii_lowercase(),
                    }),
                ))
            }
            "activate_skill" => {
                let request: ActivateSkillRequest = decode_tool_args(call)?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("activate_skill requires agent context".into())
                })?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "activate_skill requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let manifests = store
                    .list_skill_packages()
                    .map_err(OrchestratorError::ToolError)?;
                let manifest = manifests
                    .into_iter()
                    .find(|manifest| manifest.skill_id == skill_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "activate_skill unknown skill '{}'",
                            skill_id
                        ))
                    })?;
                if !manifest.enabled {
                    return Err(OrchestratorError::ToolError(format!(
                        "activate_skill denied because skill '{}' is disabled",
                        skill_id
                    )));
                }
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let activation = SkillActivationRecord {
                    activation_id: format!("skill-activation-{}", uuid::Uuid::new_v4()),
                    skill_id: skill_id.to_string(),
                    agent_id: agent_id.clone(),
                    run_id: request.run_id.clone(),
                    session_id: self.session_id,
                    active: true,
                    activated_at_us: now_us,
                    deactivated_at_us: None,
                };
                store
                    .append_skill_activation(&activation)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Activated skill '{}' for agent '{}'.", skill_id, agent_id),
                    "skill_activation",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "agent_id": agent_id,
                        "activation_id": activation.activation_id,
                    }),
                ))
            }
            "execute_skill" => {
                let request: ExecuteSkillRequest = decode_tool_args(call)?;
                let skill_id = request.skill_id.trim();
                if skill_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'skill_id'".into()));
                }
                let function_name = request
                    .function_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("run");
                let input = request.input.as_deref().unwrap_or("");
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("execute_skill requires agent context".into())
                })?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "execute_skill requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let manifest = store
                    .list_skill_packages()
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|manifest| manifest.skill_id == skill_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "execute_skill unknown skill '{}'",
                            skill_id
                        ))
                    })?;
                let wasm_ref = manifest.wasm_module_ref.clone().ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "execute_skill skill '{}' has no wasm_module_ref",
                        skill_id
                    ))
                })?;
                let module_path = resolve_skill_module_path(&wasm_ref)?;
                let module = std::fs::read(&module_path).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "execute_skill failed to read module '{}': {}",
                        module_path.display(),
                        e
                    ))
                })?;
                use aria_skill_runtime::WasmExecutor;
                let output = aria_skill_runtime::WasmtimeBackend::new()
                    .execute(&module, function_name, input)
                    .map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "execute_skill wasm execution failed for '{}': {}",
                            skill_id, e
                        ))
                    })?;
                Ok(ToolExecutionResult::structured(
                    format!("Executed skill '{}' for agent '{}'.", skill_id, agent_id),
                    "skill_execution",
                    serde_json::json!({
                        "skill_id": skill_id,
                        "agent_id": agent_id,
                        "function_name": function_name,
                        "output": output,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "register_mcp_server" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "register_mcp_server requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let profile: McpServerProfile = decode_tool_args(call)?;
                if aria_mcp::reserved_native_mcp_target(&profile.server_id)
                    || aria_mcp::reserved_native_mcp_target(&profile.display_name)
                {
                    return Err(OrchestratorError::ToolError(format!(
                        "MCP server '{}' is reserved for a native/internal subsystem boundary",
                        profile.server_id
                    )));
                }
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_mcp_server(&profile, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Registered MCP server '{}'.", profile.server_id),
                    "mcp_server",
                    serde_json::json!({
                        "server_id": profile.server_id,
                        "transport": profile.transport,
                        "enabled": profile.enabled,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "import_mcp_tool" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "import_mcp_tool requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let tool: McpImportedTool = decode_tool_args(call)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                ensure_mcp_server_exists(&store, &tool.server_id)?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                store
                    .upsert_mcp_imported_tool(&tool, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                refresh_mcp_import_cache(&store, &tool.server_id, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Imported MCP tool '{}' from server '{}'.",
                        tool.tool_name, tool.server_id
                    ),
                    "mcp_import",
                    serde_json::json!({
                        "server_id": tool.server_id,
                        "tool_name": tool.tool_name,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "import_mcp_prompt" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "import_mcp_prompt requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let prompt: McpImportedPrompt = decode_tool_args(call)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                ensure_mcp_server_exists(&store, &prompt.server_id)?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                store
                    .upsert_mcp_imported_prompt(&prompt, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                refresh_mcp_import_cache(&store, &prompt.server_id, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Imported MCP prompt '{}' from server '{}'.",
                        prompt.prompt_name, prompt.server_id
                    ),
                    "mcp_import",
                    serde_json::json!({
                        "server_id": prompt.server_id,
                        "prompt_name": prompt.prompt_name,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "import_mcp_resource" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "import_mcp_resource requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let resource: McpImportedResource = decode_tool_args(call)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                ensure_mcp_server_exists(&store, &resource.server_id)?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                store
                    .upsert_mcp_imported_resource(&resource, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                refresh_mcp_import_cache(&store, &resource.server_id, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Imported MCP resource '{}' from server '{}'.",
                        resource.resource_uri, resource.server_id
                    ),
                    "mcp_import",
                    serde_json::json!({
                        "server_id": resource.server_id,
                        "resource_uri": resource.resource_uri,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "bind_mcp_import" => {
                let request: BindMcpImportRequest = decode_tool_args(call)?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "bind_mcp_import requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let server_id = request.server_id.trim();
                let target_name = request.target_name.trim();
                if server_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'server_id'".into()));
                }
                if target_name.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'target_name'".into()));
                }
                let agent_id = request
                    .agent_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .or_else(|| self.invoking_agent_id.clone())
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "bind_mcp_import requires 'agent_id' or invoking agent context".into(),
                        )
                    })?;
                let primitive_kind = parse_mcp_primitive_kind(&request.primitive_kind)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                ensure_mcp_binding_target_exists(&store, server_id, primitive_kind, target_name)?;
                let binding = McpBindingRecord {
                    binding_id: format!("mcp-binding-{}", uuid::Uuid::new_v4()),
                    agent_id: agent_id.clone(),
                    server_id: server_id.to_string(),
                    primitive_kind,
                    target_name: target_name.to_string(),
                    created_at_us: chrono::Utc::now().timestamp_micros() as u64,
                };
                store
                    .upsert_mcp_binding(&binding)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Bound MCP {:?} '{}' on server '{}' to agent '{}'.",
                        primitive_kind, target_name, server_id, agent_id
                    ),
                    "mcp_binding",
                    serde_json::json!({
                        "agent_id": agent_id,
                        "server_id": server_id,
                        "primitive_kind": format!("{:?}", primitive_kind).to_ascii_lowercase(),
                        "target_name": target_name,
                    }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "invoke_mcp_tool" => {
                let request: InvokeMcpToolRequest = decode_tool_args(call)?;
                let server_id = request.server_id.trim();
                let tool_name = request.tool_name.trim();
                if server_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'server_id'".into()));
                }
                if tool_name.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'tool_name'".into()));
                }
                let input = request.input;
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("invoke_mcp_tool requires agent context".into())
                })?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "invoke_mcp_tool requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let client = pooled_mcp_client(&store).map_err(OrchestratorError::ToolError)?;
                let mut client = client.lock().await;
                let payload = client
                    .call_tool_for_agent(
                        &native_mcp_profile(
                            self.invoking_agent_id
                                .clone()
                                .unwrap_or_else(|| "native".into()),
                            server_id,
                            Some(tool_name),
                            None,
                            None,
                        ),
                        server_id,
                        tool_name,
                        input.clone(),
                    )
                    .map_err(|e| OrchestratorError::ToolError(e.to_string()))?
                    .payload;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Invoked MCP tool '{}::{}' for agent '{}'.",
                        server_id, tool_name, agent_id
                    ),
                    "mcp_tool_invocation",
                    payload
                        .as_object()
                        .cloned()
                        .map(serde_json::Value::Object)
                        .unwrap_or_else(|| {
                            serde_json::json!({
                                "server_id": server_id,
                                "tool_name": tool_name,
                                "agent_id": agent_id,
                                "input": input,
                            })
                        }),
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "render_mcp_prompt" => {
                let request: RenderMcpPromptRequest = decode_tool_args(call)?;
                let server_id = request.server_id.trim();
                let prompt_name = request.prompt_name.trim();
                if server_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'server_id'".into()));
                }
                if prompt_name.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'prompt_name'".into()));
                }
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("render_mcp_prompt requires agent context".into())
                })?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "render_mcp_prompt requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let client = pooled_mcp_client(&store).map_err(OrchestratorError::ToolError)?;
                let mut client = client.lock().await;
                let payload = client
                    .render_prompt_for_agent(
                        &native_mcp_profile(
                            self.invoking_agent_id
                                .clone()
                                .unwrap_or_else(|| "native".into()),
                            server_id,
                            None,
                            Some(prompt_name),
                            None,
                        ),
                        server_id,
                        prompt_name,
                        request.arguments,
                    )
                    .map_err(|e| OrchestratorError::ToolError(e.to_string()))?
                    .payload;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Rendered MCP prompt '{}::{}' for agent '{}'.",
                        server_id, prompt_name, agent_id
                    ),
                    "mcp_prompt",
                    payload,
                ))
            }
            #[cfg(feature = "mcp-runtime")]
            "read_mcp_resource" => {
                let request: ReadMcpResourceRequest = decode_tool_args(call)?;
                let server_id = request.server_id.trim();
                let resource_uri = request.resource_uri.trim();
                if server_id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'server_id'".into()));
                }
                if resource_uri.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'resource_uri'".into()));
                }
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError("read_mcp_resource requires agent context".into())
                })?;
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "read_mcp_resource requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let client = pooled_mcp_client(&store).map_err(OrchestratorError::ToolError)?;
                let mut client = client.lock().await;
                let payload = client
                    .read_resource_for_agent(
                        &native_mcp_profile(
                            self.invoking_agent_id
                                .clone()
                                .unwrap_or_else(|| "native".into()),
                            server_id,
                            None,
                            None,
                            Some(resource_uri),
                        ),
                        server_id,
                        resource_uri,
                    )
                    .map_err(|e| OrchestratorError::ToolError(e.to_string()))?
                    .payload;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Read MCP resource '{}::{}' for agent '{}'.",
                        server_id, resource_uri, agent_id
                    ),
                    "mcp_resource",
                    payload,
                ))
            }
            "search_codebase" => {
                let request: SearchCodebaseRequest = decode_tool_args(call)?;
                let query = request.query.trim();
                if query.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'query'".into()));
                }
                let output = std::process::Command::new("grep")
                    .arg("-rIn")
                    .arg("--exclude-dir=.git")
                    .arg("--max-count=20")
                    .arg(query)
                    .arg(".")
                    .output()
                    .map_err(|e| {
                        OrchestratorError::ToolError(format!("Failed to execute grep: {}", e))
                    })?;
                let res = String::from_utf8_lossy(&output.stdout).to_string();
                if res.is_empty() {
                    Ok(tool_text_result("No matches found."))
                } else {
                    Ok(tool_text_result(res))
                }
            }
            "run_tests" => {
                let request: RunTestsRequest = decode_tool_args(call)?;
                let target = request.target.as_deref().map(str::trim).unwrap_or("");
                let mut cmd = std::process::Command::new("cargo");
                cmd.arg("test");
                if !target.is_empty() {
                    cmd.arg(target);
                }
                let output = cmd.output().map_err(|e| {
                    OrchestratorError::ToolError(format!("Failed to execute cargo test: {}", e))
                })?;
                let mut res = String::from_utf8_lossy(&output.stdout).to_string();
                res.push_str(&String::from_utf8_lossy(&output.stderr));
                Ok(tool_text_result(res))
            }
            "manage_cron" => {
                let request: ManageCronRequest = decode_tool_args(call)?;
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .map(|key| self.scoped_idempotency_key(&call.name, key));
                if let Some(key) = idempotency_key.as_deref() {
                    if let Some(cached) = idempotency_lookup(key) {
                        return Ok(cached);
                    }
                }
                let action = request.action.as_deref().unwrap_or("list");

                if action == "list" {
                    let jobs = load_authoritative_scheduler_jobs(
                        &self.tx_cron,
                        self.sessions_dir.as_deref(),
                    )
                    .await
                    .map_err(|_| {
                        OrchestratorError::ToolError(
                            "Scheduler is unavailable; cannot list jobs".into(),
                        )
                    })?;
                    let json = serde_json::to_string(&jobs).unwrap_or_default();
                    return Ok(tool_text_result(format!("Active crons: {}", json)));
                }

                let mut id = request.id.as_deref().unwrap_or("").to_string();
                if action == "add" && id.is_empty() {
                    id = format!("cron-{}", uuid::Uuid::new_v4());
                }
                if (action == "delete" || action == "update") && id.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing 'id'".into()));
                }

                if action == "delete" {
                    self.tx_cron
                        .send(aria_intelligence::CronCommand::Remove(id.clone()))
                        .await
                        .map_err(|_| {
                            OrchestratorError::ToolError(
                                "Scheduler is unavailable; cannot delete job".into(),
                            )
                        })?;
                    if let Some(sessions_dir) = &self.sessions_dir {
                        let _ =
                            RuntimeStore::for_sessions_dir(&sessions_dir).delete_job_snapshot(&id);
                    }
                    let config_path = active_config_path();
                    if let Ok(content) = std::fs::read_to_string(&config_path) {
                            if let Ok(mut doc) = content.parse::<toml_edit::DocumentMut>() {
                                if let Some(jobs) = doc
                                    .get_mut("scheduler")
                                    .and_then(|i| i.get_mut("jobs"))
                                    .and_then(|i| i.as_array_of_tables_mut())
                                {
                                    jobs.retain(|table| {
                                        table.get("id").and_then(|v| v.as_str()) != Some(&id)
                                    });
                                    let _ = std::fs::write(&config_path, doc.to_string());
                                }
                            }
                    }
                    let result = format!("Cron {} deleted natively and from config.toml", id);
                    let result = ToolExecutionResult::structured(
                        result,
                        "cron_update",
                        serde_json::json!({"job_id": id, "action": "delete"}),
                    );
                    if let Some(key) = idempotency_key {
                        idempotency_store_result(key, result.clone());
                    }
                    return Ok(result);
                }

                if action == "add" || action == "update" {
                    let prompt = request.prompt.as_deref().unwrap_or("").to_string();
                    let explicit_agent_id = request.agent_id.as_deref();
                    let agent_id = resolve_scheduled_agent_id(
                        explicit_agent_id,
                        self.invoking_agent_id.as_deref(),
                        "this cron job",
                    )?;
                    let schedule_input = request.schedule.clone().ok_or_else(|| {
                            OrchestratorError::ToolError("Missing 'schedule' object".into())
                        })?;

                    if prompt.is_empty() {
                        return Err(OrchestratorError::ToolError("Missing 'prompt'".into()));
                    }
                    let (normalized_schedule, spec) = schedule_input
                        .to_schedule_parts(self.user_timezone)
                        .map_err(OrchestratorError::ToolError)?;

                    let job = aria_intelligence::ScheduledPromptJob {
                        id: id.clone(),
                        agent_id: agent_id.clone(),
                        creator_agent: self.invoking_agent_id.clone(),
                        executor_agent: Some(agent_id.clone()),
                        notifier_agent: None,
                        prompt: prompt.clone(),
                        schedule_str: normalized_schedule.clone(),
                        kind: aria_intelligence::ScheduledJobKind::Orchestrate,
                        schedule: spec,
                        session_id: self.session_id,
                        user_id: self.user_id.clone(),
                        channel: self.channel,
                        status: aria_intelligence::ScheduledJobStatus::Scheduled,
                        last_run_at_us: None,
                        last_error: None,
                        audit_log: Vec::new(),
                    };
                    self.tx_cron
                        .send(aria_intelligence::CronCommand::Add(job.clone()))
                        .await
                        .map_err(|_| {
                            OrchestratorError::ToolError(
                                "Scheduler is unavailable; cannot add or update job".into(),
                            )
                        })?;
                    if let Some(sessions_dir) = &self.sessions_dir {
                        let _ = RuntimeStore::for_sessions_dir(&sessions_dir).upsert_job_snapshot(
                            &id,
                            &job,
                            chrono::Utc::now().timestamp_micros() as u64,
                        );
                    }

                    let config_path = active_config_path();
                    if let Ok(content) = std::fs::read_to_string(&config_path) {
                            if let Ok(mut doc) = content.parse::<toml_edit::DocumentMut>() {
                                let mut updated = false;
                                if let Some(jobs) = doc
                                    .get_mut("scheduler")
                                    .and_then(|i| i.get_mut("jobs"))
                                    .and_then(|i| i.as_array_of_tables_mut())
                                {
                                    for table in jobs.iter_mut() {
                                        if table.get("id").and_then(|v| v.as_str()) == Some(&id) {
                                            table["agent_id"] = toml_edit::value(agent_id.clone());
                                            table["prompt"] = toml_edit::value(prompt.clone());
                                            table["schedule"] =
                                                toml_edit::value(normalized_schedule.clone());
                                            updated = true;
                                            break;
                                        }
                                    }
                                    if !updated {
                                        let mut new_table = toml_edit::Table::new();
                                        new_table.insert("id", toml_edit::value(id.clone()));
                                        new_table
                                            .insert("agent_id", toml_edit::value(agent_id.clone()));
                                        new_table
                                            .insert("prompt", toml_edit::value(prompt.clone()));
                                        new_table.insert(
                                            "schedule",
                                            toml_edit::value(normalized_schedule.clone()),
                                        );
                                        if let Some(sid) = self.session_id {
                                            new_table.insert(
                                                "session_id",
                                                toml_edit::value(hex::encode(sid)),
                                            );
                                        }
                                        if let Some(uid) = &self.user_id {
                                            new_table
                                                .insert("user_id", toml_edit::value(uid.clone()));
                                        }
                                        if let Some(ch) = self.channel {
                                            new_table.insert(
                                                "channel",
                                                toml_edit::value(format!("{:?}", ch)),
                                            );
                                        }
                                        jobs.push(new_table);
                                    }
                                    let _ = std::fs::write(&config_path, doc.to_string());
                                }
                            }
                    }
                    let result = format!("Cron {} set and pushed to config.toml", id);
                    let result = ToolExecutionResult::structured(
                        result,
                        "cron_update",
                        serde_json::json!({"job_id": id, "action": action}),
                    );
                    if let Some(key) = idempotency_key {
                        idempotency_store_result(key, result.clone());
                    }
                    return Ok(result);
                }
                Err(OrchestratorError::ToolError("Invalid action".into()))
            }
            "compact_session" => {
                let request: CompactSessionRequest = decode_tool_args(call)?;
                let threshold = request.threshold.unwrap_or(20) as usize;
                let session_id = self
                    .session_id
                    .ok_or_else(|| OrchestratorError::ToolError("No session context".into()))?;

                if let Some(mem) = &self.session_memory {
                    let summarized = mem
                        .summarize_if_over_threshold(
                            uuid::Uuid::from_bytes(session_id),
                            threshold,
                            chrono::Utc::now().timestamp_micros() as u64,
                            |msgs| {
                                let mut full_text = String::new();
                                for m in msgs {
                                    full_text.push_str(&m.content);
                                    full_text.push('\n');
                                }
                                format!(
                                    "Summary of previous session: {}",
                                    full_text.chars().take(500).collect::<String>()
                                )
                            },
                        )
                        .map_err(|e| {
                            OrchestratorError::ToolError(format!("Compaction failed: {}", e))
                        })?;

                    if summarized {
                        Ok(tool_text_result("Session compacted successfully."))
                    } else {
                        Ok(tool_text_result(
                            "Session under threshold, no compaction needed.",
                        ))
                    }
                } else {
                    Err(OrchestratorError::ToolError(
                        "Session storage unavailable".into(),
                    ))
                }
            }
            "grant_access" => {
                let request: GrantAccessRequest = decode_tool_args(call)?;
                let principal = request.principal.trim();
                let action = request.action.trim();
                let resource = request.resource.trim();
                if principal.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing principal".into()));
                }
                if action.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing action".into()));
                }
                if resource.is_empty() {
                    return Err(OrchestratorError::ToolError("Missing resource".into()));
                }

                let rule = format!("\npermit(principal == Agent::\"{}\", action == Action::\"{}\", resource == Resource::\"{}\");", principal, action, resource);

                let config_path = active_config_path();
                if let Ok(content) = std::fs::read_to_string(&config_path) {
                        if let Ok(doc) = content.parse::<toml_edit::DocumentMut>() {
                            if let Some(policy_path) = doc
                                .get("policy")
                                .and_then(|p| p.get("path"))
                                .and_then(|v| v.as_str())
                            {
                                let mut policy_content = std::fs::read_to_string(policy_path)
                                    .map_err(|e| {
                                        OrchestratorError::ToolError(format!(
                                            "Failed to read policy: {}",
                                            e
                                        ))
                                    })?;
                                policy_content.push_str(&rule);
                                std::fs::write(policy_path, &policy_content).map_err(|e| {
                                    OrchestratorError::ToolError(format!(
                                        "Failed to write policy: {}",
                                        e
                                    ))
                                })?;
                                return Ok(tool_text_result(format!("Access granted: {}", rule)));
                            }
                        }
                }
                Err(OrchestratorError::ToolError(
                    "Policy configuration unavailable".into(),
                ))
            }
            "manage_prompts" => {
                let request: ManagePromptsRequest = decode_tool_args(call)?;
                let action = request.action.as_deref().unwrap_or("list");
                let name = request.name.as_deref().unwrap_or("");
                let template = request.template.as_deref().unwrap_or("");

                let config_path = active_config_path();
                if let Ok(content) = std::fs::read_to_string(&config_path) {
                        if let Ok(mut doc) = content.parse::<toml_edit::DocumentMut>() {
                            if action == "list" {
                                let prompts = doc
                                    .get("prompts")
                                    .and_then(|p| p.as_table())
                                    .map(|t| t.to_string())
                                    .unwrap_or_else(|| "No prompts found.".to_string());
                                return Ok(tool_text_result(prompts));
                            }
                            if action == "add" {
                                if name.is_empty() || template.is_empty() {
                                    return Err(OrchestratorError::ToolError(
                                        "Missing name or template".into(),
                                    ));
                                }
                                doc["prompts"][name] = toml_edit::value(template);
                                let _ = std::fs::write(&config_path, doc.to_string());
                                return Ok(tool_text_result(format!(
                                    "Prompt '{}' added successfully.",
                                    name
                                )));
                            }
                            if action == "remove" {
                                if name.is_empty() {
                                    return Err(OrchestratorError::ToolError(
                                        "Missing name".into(),
                                    ));
                                }
                                if let Some(prompts) =
                                    doc.get_mut("prompts").and_then(|v| v.as_table_mut())
                                {
                                    prompts.remove(name);
                                    let _ = std::fs::write(&config_path, doc.to_string());
                                    return Ok(tool_text_result(format!(
                                        "Prompt '{}' removed successfully.",
                                        name
                                    )));
                                }
                            }
                        }
                }
                Err(OrchestratorError::ToolError("Config unavailable".into()))
            }
            "set_domain_access_decision" => {
                let request: SetDomainAccessDecisionRequest = decode_tool_args(call)?;
                let domain = normalize_domain_value(&request.domain)?;
                let decision = request.decision;
                let action_family = request.action_family.unwrap_or(aria_core::WebActionFamily::Fetch);
                let scope = request.scope.unwrap_or(match decision {
                        aria_core::DomainDecisionKind::AllowAlways
                        | aria_core::DomainDecisionKind::DenyAlways => {
                            aria_core::DomainDecisionScope::Domain
                        }
                        aria_core::DomainDecisionKind::AllowForSession => {
                            aria_core::DomainDecisionScope::Session
                        }
                        aria_core::DomainDecisionKind::AllowOnce
                        | aria_core::DomainDecisionKind::DenyOnce => {
                            aria_core::DomainDecisionScope::Request
                        }
                    });
                let session_bound = matches!(
                    decision,
                    aria_core::DomainDecisionKind::AllowForSession
                        | aria_core::DomainDecisionKind::AllowOnce
                        | aria_core::DomainDecisionKind::DenyOnce
                );
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                if session_bound && self.session_id.is_none() {
                    return Err(OrchestratorError::ToolError(
                        "Session-bound domain decisions require session context".into(),
                    ));
                }
                let target_agent_id = request.agent_id.clone().or_else(|| self.invoking_agent_id.clone());
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let record = aria_core::DomainAccessDecision {
                    decision_id: format!("domain-decision-{}", uuid::Uuid::new_v4()),
                    domain: domain.clone(),
                    agent_id: target_agent_id.clone(),
                    session_id: if session_bound { self.session_id } else { None },
                    action_family,
                    decision,
                    scope,
                    created_by_user_id: self
                        .user_id
                        .clone()
                        .unwrap_or_else(|| "system".to_string()),
                    created_at_us: now_us,
                    expires_at_us: None,
                    reason: request.reason.clone(),
                };
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_domain_access_decision(&record, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Stored domain access decision '{:?}' for '{}' on domain '{}'.",
                        decision,
                        target_agent_id
                            .clone()
                            .unwrap_or_else(|| "all agents".to_string()),
                        domain
                    ),
                    "domain_access_decision",
                    serde_json::to_value(&record).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            #[cfg(not(feature = "mcp-runtime"))]
            "register_mcp_server"
            | "import_mcp_tool"
            | "import_mcp_prompt"
            | "import_mcp_resource"
            | "bind_mcp_import"
            | "invoke_mcp_tool"
            | "render_mcp_prompt"
            | "read_mcp_resource" => Err(disabled_feature_tool_error(call, "mcp-runtime")),
            "browser_profile_create" => {
                let request: BrowserProfileCreateRequest = decode_tool_args(call)?;
                let profile_id = derive_browser_profile_id(
                    request.profile_id.as_deref(),
                    request.display_name.as_deref(),
                )?;
                let display_name = request
                    .display_name
                    .as_deref()
                    .map(str::trim)
                    .map(str::to_string)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| profile_id.clone());
                let engine = request
                    .engine
                    .unwrap_or(aria_core::BrowserEngine::Chromium);
                let mode = request
                    .mode
                    .unwrap_or(aria_core::BrowserProfileMode::ManagedPersistent);
                let allowed_domains = request
                    .allowed_domains
                    .into_iter()
                    .map(|value| value.trim().to_ascii_lowercase())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                let auth_enabled = request.auth_enabled.unwrap_or(false);
                let write_enabled = request.write_enabled.unwrap_or(false);
                let persistent = request
                    .persistent
                    .unwrap_or(!matches!(
                        mode,
                        aria_core::BrowserProfileMode::Ephemeral
                    ));
                let attached_source = request
                    .attached_source
                    .as_deref()
                    .map(str::trim)
                    .map(str::to_string)
                    .filter(|value| !value.is_empty());
                let extension_binding_id = request
                    .extension_binding_id
                    .as_deref()
                    .map(str::trim)
                    .map(str::to_string)
                    .filter(|value| !value.is_empty());
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let existing_profiles = store
                    .list_browser_profiles()
                    .map_err(OrchestratorError::ToolError)?;
                let set_as_default = request
                    .set_as_default
                    .unwrap_or(existing_profiles.is_empty());
                let profile_dir = browser_profile_dir(sessions_dir, &profile_id);
                std::fs::create_dir_all(&profile_dir).map_err(|e| {
                    OrchestratorError::ToolError(format!(
                        "Failed to create browser profile directory '{}': {}",
                        profile_dir.display(),
                        e
                    ))
                })?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let profile = aria_core::BrowserProfile {
                    profile_id: profile_id.clone(),
                    display_name,
                    mode,
                    engine,
                    is_default: set_as_default,
                    persistent,
                    managed_by_aria: !matches!(
                        mode,
                        aria_core::BrowserProfileMode::AttachedExternal
                    ),
                    attached_source,
                    extension_binding_id,
                    allowed_domains,
                    auth_enabled,
                    write_enabled,
                    created_at_us: now_us,
                };
                if set_as_default {
                    for mut existing in existing_profiles {
                        if existing.profile_id != profile_id && existing.is_default {
                            existing.is_default = false;
                            store
                                .upsert_browser_profile(&existing, now_us)
                                .map_err(OrchestratorError::ToolError)?;
                        }
                    }
                }
                store
                    .upsert_browser_profile(&profile, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Created browser profile '{}'.", profile_id),
                    "browser_profile",
                    serde_json::to_value(&profile).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_profile_list" => {
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let profiles = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_browser_profiles()
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Found {} browser profile(s).", profiles.len()),
                    "browser_profiles",
                    serde_json::to_value(&profiles).unwrap_or_else(|_| serde_json::json!([])),
                ))
            }
            "browser_profile_use" => {
                let request: BrowserProfileUseRequest = decode_tool_args(call)?;
                let profile_id = required_trimmed(&request.profile_id, "profile_id")?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "browser_profile_use requires session context".into(),
                    )
                })?;
                let agent_id = self
                    .invoking_agent_id
                    .clone()
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "browser_profile_use requires invoking agent context".into(),
                        )
                    })?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let binding = aria_core::BrowserProfileBindingRecord {
                    binding_id: format!(
                        "browser-profile-binding-{}-{}",
                        uuid::Uuid::from_bytes(session_id),
                        agent_id
                    ),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: profile_id.clone(),
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_browser_profile_binding(&binding, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Bound browser profile '{}' to agent '{}' for the current session.",
                        profile_id, agent_id
                    ),
                    "browser_profile_binding",
                    serde_json::to_value(&binding).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_session_start" => {
                let request: BrowserSessionStartRequest = decode_tool_args(call)?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "browser_session_start requires session context".into(),
                    )
                })?;
                let agent_id = self
                    .invoking_agent_id
                    .clone()
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "browser_session_start requires invoking agent context".into(),
                        )
                    })?;
                let _ = cleanup_stale_browser_sessions(sessions_dir, Some(session_id), Some(&agent_id));
                let profile = if let Some(profile_id) = request.profile_id.as_deref().map(str::trim) {
                    RuntimeStore::for_sessions_dir(&sessions_dir)
                        .list_browser_profiles()
                        .map_err(OrchestratorError::ToolError)?
                        .into_iter()
                        .find(|profile| profile.profile_id == profile_id)
                        .ok_or_else(|| {
                            OrchestratorError::ToolError(format!(
                                "browser profile '{}' does not exist",
                                profile_id
                            ))
                        })?
                } else {
                    current_browser_profile_for_agent(sessions_dir, session_id, &agent_id)?
                        .ok_or_else(|| {
                            OrchestratorError::ToolError(
                                "browser_session_start requires a bound browser profile or explicit profile_id"
                                    .into(),
                            )
                        })?
                };
                let start_url = request.url.as_deref();
                if let Some(url) = start_url {
                    validate_web_url_target_syntactic(url, private_network_override_enabled())?;
                }
                let profile_dir = browser_profile_dir(sessions_dir, &profile.profile_id);
                create_dir_all_async(profile_dir.clone(), "browser profile dir").await?;
                let transport = browser_transport_for_profile(&profile);
                let launch = transport.start_session(&profile, &profile_dir, start_url).await?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let browser_session = aria_core::BrowserSessionRecord {
                    browser_session_id: format!("browser-session-{}", uuid::Uuid::new_v4()),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: profile.profile_id.clone(),
                    engine: profile.engine,
                    transport: transport.kind(),
                    status: aria_core::BrowserSessionStatus::Launched,
                    pid: launch.pid,
                    profile_dir: profile_dir.to_string_lossy().to_string(),
                    start_url: start_url.map(|value| value.to_string()),
                    launch_command: launch.launch_command.clone(),
                    error: None,
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let binding = aria_core::BrowserProfileBindingRecord {
                    binding_id: format!(
                        "browser-profile-binding-{}-{}",
                        uuid::Uuid::from_bytes(session_id),
                        agent_id
                    ),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: profile.profile_id.clone(),
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                store
                    .upsert_browser_profile_binding(&binding, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                let reused_login_state = start_url
                    .and_then(|value| resolve_browser_login_domain(value).ok())
                    .and_then(|domain| {
                        latest_browser_login_state_for_profile(
                            sessions_dir,
                            &agent_id,
                            &profile.profile_id,
                            &domain,
                        )
                        .ok()
                        .flatten()
                    })
                    .filter(|state| {
                        matches!(
                            state.state,
                            aria_core::BrowserLoginStateKind::Authenticated
                        )
                    });
                store
                    .upsert_browser_session(&browser_session, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                let artifact_dir =
                    browser_session_artifacts_root(&sessions_dir, &browser_session.browser_session_id);
                create_dir_all_async(artifact_dir.clone(), "browser launch artifact dir").await?;
                let launch_artifact_path = artifact_dir.join("launch.json");
                let launch_artifact = aria_core::BrowserArtifactRecord {
                    artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: profile.profile_id.clone(),
                    kind: aria_core::BrowserArtifactKind::LaunchMetadata,
                    mime_type: "application/json".into(),
                    storage_path: launch_artifact_path.to_string_lossy().to_string(),
                    metadata: serde_json::json!({
                        "launch": launch.metadata,
                        "reused_login_state": reused_login_state,
                    }),
                    created_at_us: now_us,
                };
                write_bytes_async(
                    launch_artifact_path.clone(),
                    serde_json::to_vec_pretty(&launch_artifact.metadata)
                        .unwrap_or_else(|_| b"{}".to_vec()),
                    "browser launch artifact",
                )
                .await?;
                store
                    .append_browser_artifact(&launch_artifact)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id: agent_id.clone(),
                        profile_id: Some(profile.profile_id.clone()),
                        action: aria_core::BrowserActionKind::SessionStart,
                        target: start_url.map(|value| value.to_string()),
                        metadata: serde_json::json!({
                            "pid": browser_session.pid,
                            "launch_command": browser_session.launch_command,
                            "transport": browser_session.transport,
                        }),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                let response = BrowserSessionResponse {
                    session: browser_session,
                    reused_login_state,
                    transport: transport.kind(),
                };
                Ok(structured_payload(
                    format!(
                        "Started browser session '{}' for profile '{}' using {:?} transport.",
                        response.session.browser_session_id,
                        response.session.profile_id,
                        response.transport
                    ),
                    "browser_session",
                    &response,
                ))
            }
            "browser_session_list" => {
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let _ = cleanup_stale_browser_sessions(
                    sessions_dir,
                    self.session_id,
                    self.invoking_agent_id.as_deref(),
                );
                let sessions = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_browser_sessions(self.session_id, self.invoking_agent_id.as_deref())
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!("Found {} browser session(s).", sessions.len()),
                    "browser_sessions",
                    serde_json::to_value(&sessions).unwrap_or_else(|_| serde_json::json!([])),
                ))
            }
            "browser_session_status" => {
                let request: BrowserSessionStatusRequest = decode_tool_args(call)?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let _ = cleanup_stale_browser_sessions(
                    sessions_dir,
                    self.session_id,
                    self.invoking_agent_id.as_deref(),
                );
                let browser_session_id = resolve_browser_session_id_or_current(
                    sessions_dir,
                    self.session_id.ok_or_else(|| {
                        OrchestratorError::ToolError("Session store unavailable".into())
                    })?,
                    self.invoking_agent_id.as_deref().ok_or_else(|| {
                        OrchestratorError::ToolError("Session store unavailable".into())
                    })?,
                    request.browser_session_id.as_deref(),
                )?;
                let browser_session = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_browser_sessions(self.session_id, self.invoking_agent_id.as_deref())
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|record| record.browser_session_id == browser_session_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "browser session '{}' not found",
                            browser_session_id
                        ))
                    })?;
                let active_login_states = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_browser_login_states(
                        self.session_id,
                        self.invoking_agent_id.as_deref(),
                        None,
                    )
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .filter(|state| {
                        state.browser_session_id == browser_session.browser_session_id
                            || state.profile_id == browser_session.profile_id
                    })
                    .collect::<Vec<_>>();
                let mut payload =
                    serde_json::to_value(&browser_session).unwrap_or_else(|_| serde_json::json!({}));
                if let serde_json::Value::Object(ref mut map) = payload {
                    map.insert(
                        "active_login_states".into(),
                        serde_json::to_value(&active_login_states)
                            .unwrap_or_else(|_| serde_json::json!([])),
                    );
                }
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Browser session '{}' is '{}'.",
                        browser_session.browser_session_id,
                        serde_json::to_string(&browser_session.status)
                            .unwrap_or_else(|_| "\"unknown\"".into())
                    ),
                    "browser_session",
                    payload,
                ))
            }
            "browser_session_cleanup" => {
                let request: BrowserSessionCleanupRequest = decode_tool_args(call)?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let session_filter = self.session_id;
                let agent_filter = self.invoking_agent_id.as_deref();
                let browser_session_id = request.browser_session_id.as_deref().map(str::trim);
                let mut cleaned =
                    cleanup_stale_browser_sessions(sessions_dir, session_filter, agent_filter)?;
                if let Some(browser_session_id) = browser_session_id {
                    cleaned.retain(|record| record.browser_session_id == browser_session_id);
                }
                Ok(ToolExecutionResult::structured(
                    format!("Cleaned up {} stale browser session(s).", cleaned.len()),
                    "browser_sessions",
                    serde_json::to_value(&cleaned).unwrap_or_else(|_| serde_json::json!([])),
                ))
            }
            "browser_session_persist_state" => {
                let request: BrowserSessionPersistStateRequest = decode_tool_args(call)?;
                let browser_session_id =
                    required_trimmed(&request.browser_session_id, "browser_session_id")?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_session_persist_state")?;
                let sessions_dir = self.sessions_dir_required("browser_session_persist_state")?;
                let browser_session = current_browser_session_for_agent(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    &browser_session_id,
                )?;
                let transport = browser_transport_for_session(&browser_session);
                let export_state = transport.persist_state(&browser_session).await?;
                let plaintext =
                    serde_json::to_vec(&export_state).map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "Failed to serialize exported browser state: {}",
                            e
                        ))
                    })?;
                let encrypted = encrypt_browser_session_state_payload(&plaintext)?;
                let state_dir =
                    browser_session_state_root(sessions_dir, &browser_session.profile_id);
                create_dir_all_async(state_dir.clone(), "browser session state dir").await?;
                let state_path = state_dir.join(format!("{}.enc.json", browser_session.browser_session_id));
                write_bytes_async(
                    state_path.clone(),
                    serde_json::to_vec_pretty(&encrypted).unwrap_or_else(|_| b"{}".to_vec()),
                    "browser session state",
                )
                .await?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let state = aria_core::BrowserSessionStateRecord {
                    state_id: format!("browser-session-state-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    storage_path: state_path.to_string_lossy().to_string(),
                    content_sha256_hex: format!("{:x}", Sha256::digest(&plaintext)),
                    last_restored_at_us: None,
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                store
                    .upsert_browser_session_state(&state, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::SessionStatePersist,
                        target: browser_session.start_url.clone(),
                        metadata: serde_json::json!({
                            "state_id": state.state_id,
                            "content_sha256_hex": state.content_sha256_hex,
                        }),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Persisted encrypted browser session state for '{}'.",
                        browser_session.browser_session_id
                    ),
                    "browser_session_state",
                    serde_json::to_value(&state).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_session_restore_state" => {
                let request: BrowserSessionPersistStateRequest = decode_tool_args(call)?;
                let browser_session_id =
                    required_trimmed(&request.browser_session_id, "browser_session_id")?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_session_restore_state")?;
                let sessions_dir = self.sessions_dir_required("browser_session_restore_state")?;
                let browser_session = current_browser_session_for_agent(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    &browser_session_id,
                )?;
                let mut state = latest_browser_session_state_for_profile(
                    sessions_dir,
                    &agent_id,
                    &browser_session.profile_id,
                )?
                .ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "no persisted browser session state found for profile '{}'",
                        browser_session.profile_id
                    ))
                })?;
                let encrypted_bytes =
                    read_bytes_async(PathBuf::from(&state.storage_path), "browser session state")
                        .await?;
                let encrypted: EncryptedBrowserSessionState = serde_json::from_slice(&encrypted_bytes)
                    .map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "Failed to parse encrypted browser session state '{}': {}",
                            state.storage_path, e
                        ))
                    })?;
                let plaintext = decrypt_browser_session_state_payload(&encrypted)?;
                let storage_state: serde_json::Value =
                    serde_json::from_slice(&plaintext).map_err(|e| {
                        OrchestratorError::ToolError(format!(
                            "Failed to decode decrypted browser session state: {}",
                            e
                        ))
                    })?;
                let transport = browser_transport_for_session(&browser_session);
                let bridge_result = transport
                    .restore_state(&browser_session, storage_state)
                    .await?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                state.last_restored_at_us = Some(now_us);
                state.updated_at_us = now_us;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                store
                    .upsert_browser_session_state(&state, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::SessionStateRestore,
                        target: browser_session.start_url.clone(),
                        metadata: serde_json::json!({
                            "state_id": state.state_id,
                            "result": bridge_result,
                        }),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Restored encrypted browser session state for '{}'.",
                        browser_session.browser_session_id
                    ),
                    "browser_session_state",
                    serde_json::to_value(&state).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_session_pause" | "browser_session_resume" => {
                let request: BrowserSessionIdRequest = decode_tool_args(call)?;
                let browser_session_id =
                    required_trimmed(&request.browser_session_id, "browser_session_id")?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "{} requires session context",
                        call.name
                    ))
                })?;
                let agent_id = self.invoking_agent_id.clone().ok_or_else(|| {
                    OrchestratorError::ToolError(format!(
                        "{} requires invoking agent context",
                        call.name
                    ))
                })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let mut browser_session = store
                    .list_browser_sessions(Some(session_id), Some(&agent_id))
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|record| record.browser_session_id == browser_session_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "browser session '{}' not found",
                            browser_session_id
                        ))
                    })?;
                browser_session.status = if call.name == "browser_session_pause" {
                    aria_core::BrowserSessionStatus::Paused
                } else {
                    aria_core::BrowserSessionStatus::Launched
                };
                browser_session.updated_at_us = chrono::Utc::now().timestamp_micros() as u64;
                store
                    .upsert_browser_session(&browser_session, browser_session.updated_at_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: if call.name == "browser_session_pause" {
                            aria_core::BrowserActionKind::SessionPause
                        } else {
                            aria_core::BrowserActionKind::SessionResume
                        },
                        target: browser_session.start_url.clone(),
                        metadata: serde_json::json!({}),
                        created_at_us: browser_session.updated_at_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Browser session '{}' is now {:?}.",
                        browser_session.browser_session_id, browser_session.status
                    ),
                    "browser_session",
                    serde_json::to_value(&browser_session)
                        .unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_session_record_challenge" => {
                let request: BrowserChallengeRequest = decode_tool_args(call)?;
                let browser_session_id =
                    required_trimmed(&request.browser_session_id, "browser_session_id")?;
                let Some(sessions_dir) = self.sessions_dir.as_deref() else {
                    return Err(OrchestratorError::ToolError(
                        "Session store unavailable".into(),
                    ));
                };
                let session_id = self.session_id.ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "browser_session_record_challenge requires session context".into(),
                    )
                })?;
                let agent_id = self
                    .invoking_agent_id
                    .clone()
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(
                            "browser_session_record_challenge requires invoking agent context"
                                .into(),
                        )
                    })?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let mut browser_session = store
                    .list_browser_sessions(Some(session_id), Some(&agent_id))
                    .map_err(OrchestratorError::ToolError)?
                    .into_iter()
                    .find(|record| record.browser_session_id == browser_session_id)
                    .ok_or_else(|| {
                        OrchestratorError::ToolError(format!(
                            "browser session '{}' not found",
                            browser_session_id
                        ))
                    })?;
                let challenge = request.challenge;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let event = aria_core::BrowserChallengeEvent {
                    event_id: format!("browser-challenge-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    challenge,
                    url: request.url.clone(),
                    message: request.message.clone(),
                    created_at_us: now_us,
                };
                browser_session.status = aria_core::BrowserSessionStatus::Paused;
                browser_session.updated_at_us = now_us;
                store
                    .upsert_browser_session(&browser_session, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_challenge_event(&event)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::ChallengeDetected,
                        target: event.url.clone(),
                        metadata: serde_json::json!({
                            "challenge": event.challenge,
                            "message": event.message,
                        }),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Recorded browser challenge '{:?}' for session '{}'.",
                        event.challenge, browser_session.browser_session_id
                    ),
                    "browser_challenge_event",
                    serde_json::to_value(&event).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_login_status" => {
                let request: BrowserLoginStatusRequest = decode_tool_args(call)?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_login_status")?;
                let sessions_dir = self.sessions_dir_required("browser_login_status")?;
                let browser_session_id = resolve_browser_session_id_or_current(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    request.browser_session_id.as_deref(),
                )?;
                let domain = request
                    .domain
                    .as_deref()
                    .map(resolve_browser_login_domain)
                    .transpose()?;
                let mut login_states = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_browser_login_states(Some(session_id), Some(&agent_id), domain.as_deref())
                    .map_err(OrchestratorError::ToolError)?;
                login_states.retain(|state| state.browser_session_id == browser_session_id);
                login_states.sort_by_key(|state| std::cmp::Reverse(state.updated_at_us));
                Ok(ToolExecutionResult::structured(
                    format!("Found {} browser login state record(s).", login_states.len()),
                    "browser_login_states",
                    serde_json::to_value(&login_states).unwrap_or_else(|_| serde_json::json!([])),
                ))
            }
            "browser_login_begin_manual" => {
                let request: BrowserLoginStatusRequest = decode_tool_args(call)?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_login_begin_manual")?;
                let sessions_dir = self.sessions_dir_required("browser_login_begin_manual")?;
                let browser_session_id = resolve_browser_session_id_or_current(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    request.browser_session_id.as_deref(),
                )?;
                let domain = resolve_browser_login_domain(&required_trimmed(
                    request.domain.as_deref().ok_or_else(|| {
                        OrchestratorError::ToolError("domain is required".into())
                    })?,
                    "domain",
                )?)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let mut browser_session = current_browser_session_for_agent(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    &browser_session_id,
                )?;
                let profile = browser_profile_by_id(sessions_dir, &browser_session.profile_id)?;
                if !profile.auth_enabled {
                    return Err(OrchestratorError::ToolError(format!(
                        "browser profile '{}' is not enabled for authenticated flows",
                        profile.profile_id
                    )));
                }
                let existing = latest_browser_login_state_for_profile(
                    sessions_dir,
                    &agent_id,
                    &browser_session.profile_id,
                    &domain,
                )?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                browser_session.status = aria_core::BrowserSessionStatus::Paused;
                browser_session.updated_at_us = now_us;
                store
                    .upsert_browser_session(&browser_session, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .upsert_browser_login_state(
                        &aria_core::BrowserLoginStateRecord {
                            login_state_id: existing
                                .as_ref()
                                .map(|state| state.login_state_id.clone())
                                .unwrap_or_else(|| {
                                    format!("browser-login-state-{}", uuid::Uuid::new_v4())
                                }),
                            browser_session_id: browser_session.browser_session_id.clone(),
                            session_id,
                            agent_id: agent_id.clone(),
                            profile_id: browser_session.profile_id.clone(),
                            domain: domain.clone(),
                            state: aria_core::BrowserLoginStateKind::ManualPending,
                            credential_key_names: existing
                                .as_ref()
                                .map(|state| state.credential_key_names.clone())
                                .unwrap_or_default(),
                            notes: request
                                .notes
                                .clone()
                                .or_else(|| existing.as_ref().and_then(|state| state.notes.clone())),
                            last_validated_at_us: existing
                                .as_ref()
                                .and_then(|state| state.last_validated_at_us),
                            created_at_us: existing
                                .as_ref()
                                .map(|state| state.created_at_us)
                                .unwrap_or(now_us),
                            updated_at_us: now_us,
                        },
                        now_us,
                    )
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::SessionPause,
                        target: Some(domain.clone()),
                        metadata: serde_json::json!({"reason":"manual_login_pending"}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                let login_state = latest_browser_login_state_for_profile(
                    sessions_dir,
                    &browser_session.agent_id,
                    &browser_session.profile_id,
                    &domain,
                )?
                .ok_or_else(|| {
                    OrchestratorError::ToolError("manual login state was not persisted".into())
                })?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Manual login started for domain '{}' on profile '{}'.",
                        domain, browser_session.profile_id
                    ),
                    "browser_login_state",
                    serde_json::to_value(&login_state).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_login_complete_manual" => {
                let request: BrowserLoginCompleteRequest = decode_tool_args(call)?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_login_complete_manual")?;
                let sessions_dir = self.sessions_dir_required("browser_login_complete_manual")?;
                let browser_session_id = resolve_browser_session_id_or_current(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    request.browser_session_id.as_deref(),
                )?;
                let domain = resolve_browser_login_domain(&request.domain)?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let mut browser_session = current_browser_session_for_agent(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    &browser_session_id,
                )?;
                let profile = browser_profile_by_id(sessions_dir, &browser_session.profile_id)?;
                if !profile.auth_enabled {
                    return Err(OrchestratorError::ToolError(format!(
                        "browser profile '{}' is not enabled for authenticated flows",
                        profile.profile_id
                    )));
                }
                let existing = latest_browser_login_state_for_profile(
                    sessions_dir,
                    &agent_id,
                    &browser_session.profile_id,
                    &domain,
                )?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let credential_key_names = Some(
                    request
                        .credential_key_names
                        .iter()
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>(),
                )
                    .filter(|values| !values.is_empty())
                    .or_else(|| existing.as_ref().map(|state| state.credential_key_names.clone()))
                    .unwrap_or_default();
                let login_state = aria_core::BrowserLoginStateRecord {
                    login_state_id: existing
                        .as_ref()
                        .map(|state| state.login_state_id.clone())
                        .unwrap_or_else(|| format!("browser-login-state-{}", uuid::Uuid::new_v4())),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    domain: domain.clone(),
                    state: request
                        .state
                        .unwrap_or(aria_core::BrowserLoginStateKind::Authenticated),
                    credential_key_names,
                    notes: request
                        .notes
                        .clone()
                        .or_else(|| existing.as_ref().and_then(|state| state.notes.clone())),
                    last_validated_at_us: Some(now_us),
                    created_at_us: existing
                        .as_ref()
                        .map(|state| state.created_at_us)
                        .unwrap_or(now_us),
                    updated_at_us: now_us,
                };
                store
                    .upsert_browser_login_state(&login_state, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                browser_session.status = aria_core::BrowserSessionStatus::Launched;
                browser_session.updated_at_us = now_us;
                store
                    .upsert_browser_session(&browser_session, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::SessionResume,
                        target: Some(domain),
                        metadata: serde_json::json!({"reason":"manual_login_completed"}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Manual login completed for profile '{}' on session '{}'.",
                        browser_session.profile_id, browser_session.browser_session_id
                    ),
                    "browser_login_state",
                    serde_json::to_value(&login_state).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_login_fill_credentials" => {
                let request: BrowserLoginFillCredentialsRequest = decode_tool_args(call)?;
                let (session_id, agent_id) =
                    self.session_and_agent_required("browser_login_fill_credentials")?;
                let sessions_dir = self.sessions_dir_required("browser_login_fill_credentials")?;
                let browser_session_id = resolve_browser_session_id_or_current(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    request.browser_session_id.as_deref(),
                )?;
                let domain = resolve_browser_login_domain(&request.domain)?;
                let credentials = &request.credentials;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let browser_session = current_browser_session_for_agent(
                    sessions_dir,
                    session_id,
                    &agent_id,
                    &browser_session_id,
                )?;
                let vault = native_tool_vault()?;
                let profile = browser_profile_by_id(sessions_dir, &browser_session.profile_id)?;
                if !profile.auth_enabled {
                    return Err(OrchestratorError::ToolError(format!(
                        "browser profile '{}' is not enabled for authenticated flows",
                        profile.profile_id
                    )));
                }
                let existing = latest_browser_login_state_for_profile(
                    sessions_dir,
                    &agent_id,
                    &browser_session.profile_id,
                    &domain,
                )?;
                let mut credential_key_names = existing
                    .as_ref()
                    .map(|state| state.credential_key_names.clone())
                    .unwrap_or_default();
                let mut secret_values = Vec::new();
                let mut bridge_credentials = Vec::new();
                for credential in credentials {
                    let key_name = credential.key_name.trim();
                    if key_name.is_empty() {
                        return Err(OrchestratorError::ToolError(
                            "credential entry missing 'key_name'".into(),
                        ));
                    }
                    let secret = vault
                        .retrieve_for_egress(&agent_id, key_name, &domain)
                        .map_err(|e| {
                            append_secret_usage_audit_record(
                                Some(sessions_dir),
                                &agent_id,
                                Some(session_id),
                                "browser_login_fill_credentials",
                                key_name,
                                &domain,
                                aria_core::SecretUsageOutcome::Denied,
                                format!("{}", e),
                            );
                            OrchestratorError::ToolError(format!(
                                "Failed to retrieve vault secret '{}': {}",
                                key_name, e
                            ))
                        })?;
                    append_secret_usage_audit_record(
                        Some(sessions_dir),
                        &agent_id,
                        Some(session_id),
                        "browser_login_fill_credentials",
                        key_name,
                        &domain,
                        aria_core::SecretUsageOutcome::Allowed,
                        "vault secret retrieved for browser credential fill",
                    );
                    if !credential_key_names.iter().any(|value| value == key_name) {
                        credential_key_names.push(key_name.to_string());
                    }
                    secret_values.push(secret.clone());
                    bridge_credentials.push(serde_json::json!({
                        "key_name": key_name,
                        "selector": credential.selector.clone().unwrap_or(serde_json::Value::Null),
                        "field": credential.field.clone().unwrap_or(serde_json::Value::Null),
                        "value": secret,
                    }));
                }
                let transport = browser_transport_for_session(&browser_session);
                let bridge_result = transport
                    .fill_credentials(
                        &browser_session,
                        &domain,
                        serde_json::Value::Array(bridge_credentials),
                        &secret_values,
                    )
                    .await?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let authenticated = bridge_result
                    .get("authenticated")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let login_state = aria_core::BrowserLoginStateRecord {
                    login_state_id: existing
                        .as_ref()
                        .map(|state| state.login_state_id.clone())
                        .unwrap_or_else(|| format!("browser-login-state-{}", uuid::Uuid::new_v4())),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    domain: domain.clone(),
                    state: if authenticated {
                        aria_core::BrowserLoginStateKind::Authenticated
                    } else {
                        aria_core::BrowserLoginStateKind::ManualPending
                    },
                    credential_key_names,
                    notes: existing.as_ref().and_then(|state| state.notes.clone()),
                    last_validated_at_us: if authenticated { Some(now_us) } else { None },
                    created_at_us: existing
                        .as_ref()
                        .map(|state| state.created_at_us)
                        .unwrap_or(now_us),
                    updated_at_us: now_us,
                };
                store
                    .upsert_browser_login_state(&login_state, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::Type,
                        target: Some(domain),
                        metadata: serde_json::json!({
                            "credential_key_names": login_state.credential_key_names.clone(),
                            "result": bridge_result,
                        }),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Filled {} credential field(s) for domain '{}'.",
                        credentials.len(),
                        login_state.domain
                    ),
                    "browser_login_result",
                    serde_json::json!({
                        "login_state": login_state,
                        "result": bridge_result,
                    }),
                ))
            }
            "browser_open" => {
                let request: BrowserOpenRequest = decode_tool_args(call)?;
                let profile_id = request.profile_id.as_deref();
                let url = request.url.as_str();
                let (_session_id, _agent_id, _sessions_dir, _profile) =
                    self.resolve_browser_profile("browser_open", profile_id)?;
                self.execute(&ToolCall {
                    invocation_id: call.invocation_id.clone(),
                    name: "browser_session_start".into(),
                    arguments: serde_json::json!({
                        "profile_id": profile_id,
                        "url": url,
                    })
                    .to_string(),
                })
                .await
            }
            "browser_snapshot" => {
                let request: BrowserArtifactRequest = decode_tool_args(call)?;
                let url = &request.url;
                validate_web_url_target_syntactic(url, private_network_override_enabled())?;
                let (session_id, agent_id, sessions_dir, browser_session) = self
                    .resolve_browser_session("browser_snapshot", request.browser_session_id.as_deref())?;
                let (html, content_type) = fetch_web_document(url).await?;
                let artifact_dir =
                    browser_session_artifacts_root(&sessions_dir, &browser_session.browser_session_id);
                create_dir_all_async(artifact_dir.clone(), "browser snapshot artifact dir").await?;
                let artifact_path =
                    artifact_dir.join(format!("snapshot-{}.html", uuid::Uuid::new_v4()));
                write_bytes_async(
                    artifact_path.clone(),
                    html.as_bytes().to_vec(),
                    "browser snapshot",
                )
                .await?;
                validate_artifact_size_limit(
                    aria_core::BrowserArtifactKind::DomSnapshot,
                    html.len() as u64,
                )?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let artifact = aria_core::BrowserArtifactRecord {
                    artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    kind: aria_core::BrowserArtifactKind::DomSnapshot,
                    mime_type: content_type,
                    storage_path: artifact_path.to_string_lossy().to_string(),
                    metadata: serde_json::json!({"url": url}),
                    created_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                run_artifact_scan_async(
                    artifact_path.clone(),
                    aria_core::BrowserArtifactKind::DomSnapshot,
                    artifact.mime_type.clone(),
                )
                .await?;
                store
                    .append_browser_artifact(&artifact)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::Navigate,
                        target: Some(url.to_string()),
                        metadata: serde_json::json!({"artifact_id": artifact.artifact_id}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Stored browser snapshot artifact '{}' for session '{}'.",
                        artifact.artifact_id, browser_session.browser_session_id
                    ),
                    "browser_artifact",
                    serde_json::to_value(&artifact).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_screenshot" => {
                let request: BrowserArtifactRequest = decode_tool_args(call)?;
                let url = &request.url;
                let (session_id, agent_id, sessions_dir, browser_session) = self
                    .resolve_browser_session("browser_screenshot", request.browser_session_id.as_deref())?;
                let artifact_dir =
                    browser_session_artifacts_root(&sessions_dir, &browser_session.browser_session_id);
                create_dir_all_async(artifact_dir.clone(), "browser screenshot artifact dir").await?;
                let artifact_path =
                    artifact_dir.join(format!("screenshot-{}.png", uuid::Uuid::new_v4()));
                let transport = browser_transport_for_session(&browser_session);
                let transport_result = transport
                    .screenshot(&browser_session, url, &artifact_path)
                    .await?;
                if !artifact_path.exists() {
                    return Err(OrchestratorError::ToolError(format!(
                        "browser screenshot command completed but no artifact was written to '{}'",
                        artifact_path.display()
                    )));
                }
                validate_artifact_size_limit(
                    aria_core::BrowserArtifactKind::Screenshot,
                    file_size_async(artifact_path.clone(), "browser screenshot metadata").await?,
                )?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let artifact = aria_core::BrowserArtifactRecord {
                    artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    kind: aria_core::BrowserArtifactKind::Screenshot,
                    mime_type: "image/png".into(),
                    storage_path: artifact_path.to_string_lossy().to_string(),
                    metadata: serde_json::json!({
                        "url": url,
                        "transport_result": transport_result,
                    }),
                    created_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                run_artifact_scan_async(
                    artifact_path.clone(),
                    aria_core::BrowserArtifactKind::Screenshot,
                    artifact.mime_type.clone(),
                )
                .await?;
                store
                    .append_browser_artifact(&artifact)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::Screenshot,
                        target: Some(url.to_string()),
                        metadata: serde_json::json!({"artifact_id": artifact.artifact_id}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Stored browser screenshot artifact '{}' for session '{}'.",
                        artifact.artifact_id, browser_session.browser_session_id
                    ),
                    "browser_artifact",
                    serde_json::to_value(&artifact).unwrap_or_else(|_| serde_json::json!({})),
                ))
            }
            "browser_download" => {
                let request: BrowserDownloadRequest = decode_tool_args(call)?;
                let url = &request.url;
                let filename = request
                    .filename
                    .as_deref()
                    .map(str::trim)
                    .map(str::to_string)
                    .filter(|value| !value.is_empty());
                let (session_id, agent_id, sessions_dir, browser_session) = self
                    .resolve_browser_session("browser_download", request.browser_session_id.as_deref())?;
                let (body, content_type) = fetch_web_bytes(url).await?;
                let artifact_dir =
                    browser_session_artifacts_root(&sessions_dir, &browser_session.browser_session_id);
                create_dir_all_async(artifact_dir.clone(), "browser download artifact dir").await?;
                let inferred_name = filename.unwrap_or_else(|| {
                    reqwest::Url::parse(url)
                        .ok()
                        .and_then(|parsed| {
                            parsed
                                .path_segments()
                                .and_then(|segments| segments.last())
                                .map(|value| value.to_string())
                        })
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| format!("download-{}", uuid::Uuid::new_v4()))
                });
                let effective_mime_type = validate_download_artifact_policy(
                    &inferred_name,
                    &content_type,
                    &body,
                    body.len() as u64,
                )?;
                let artifact_path = artifact_dir.join(inferred_name);
                write_bytes_async(
                    artifact_path.clone(),
                    body.clone(),
                    "browser download artifact",
                )
                .await?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let artifact = aria_core::BrowserArtifactRecord {
                    artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    kind: aria_core::BrowserArtifactKind::Download,
                    mime_type: effective_mime_type,
                    storage_path: artifact_path.to_string_lossy().to_string(),
                    metadata: serde_json::json!({"url": url}),
                    created_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                run_artifact_scan_async(
                    artifact_path.clone(),
                    aria_core::BrowserArtifactKind::Download,
                    artifact.mime_type.clone(),
                )
                .await?;
                store
                    .append_browser_artifact(&artifact)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::Download,
                        target: Some(url.to_string()),
                        metadata: serde_json::json!({"artifact_id": artifact.artifact_id}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(ToolExecutionResult::structured(
                    format!(
                        "Stored browser download artifact '{}' for session '{}' on profile '{}'.",
                        artifact.artifact_id,
                        browser_session.browser_session_id,
                        browser_session.profile_id
                    ),
                    "browser_artifact",
                    serde_json::json!({
                        "action": "download",
                        "kind": "download",
                        "storage_path": artifact.storage_path,
                        "artifact": artifact,
                        "browser_session_id": browser_session.browser_session_id,
                        "profile_id": browser_session.profile_id,
                    }),
                ))
            }
            "browser_act" => {
                let request = extract_browser_action_request(call)?.ok_or_else(|| {
                    OrchestratorError::ToolError("Invalid browser action request".into())
                })?;
                let (session_id, agent_id) = self.session_and_agent_required("browser_act")?;
                let sessions_dir = self.sessions_dir_required("browser_act")?;
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                let mut sessions = store
                    .list_browser_sessions(Some(session_id), Some(&agent_id))
                    .map_err(OrchestratorError::ToolError)?;
                sessions.sort_by_key(|record| std::cmp::Reverse(record.updated_at_us));
                let requested_session_id = request.browser_session_id.trim();
                let mut browser_session = if requested_session_id.is_empty() {
                    sessions
                        .iter()
                        .find(|record| {
                            matches!(
                                record.status,
                                aria_core::BrowserSessionStatus::Launched
                                    | aria_core::BrowserSessionStatus::Paused
                            )
                        })
                        .cloned()
                        .ok_or_else(|| {
                            OrchestratorError::ToolError(
                                "browser_act requires an active browser session or explicit browser_session_id"
                                    .into(),
                            )
                        })?
                } else {
                    sessions
                        .iter()
                        .find(|record| record.browser_session_id == requested_session_id)
                        .cloned()
                        .or_else(|| {
                            // Recovery path for model-hallucinated session ids:
                            // if requested id is unknown, fall back to the latest active session.
                            sessions
                                .iter()
                                .find(|record| {
                                    matches!(
                                        record.status,
                                        aria_core::BrowserSessionStatus::Launched
                                            | aria_core::BrowserSessionStatus::Paused
                                    )
                                })
                                .cloned()
                        })
                        .ok_or_else(|| {
                            OrchestratorError::ToolError(format!(
                                "browser session '{}' not found",
                                request.browser_session_id
                            ))
                        })?
                };
                if browser_session.status == aria_core::BrowserSessionStatus::Paused {
                    return Err(OrchestratorError::ToolError(format!(
                        "browser session '{}' is paused; resume it before performing actions",
                        browser_session.browser_session_id
                    )));
                }
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                match request.action {
                    aria_core::BrowserInteractionKind::Wait => {
                        let millis = request.millis.unwrap_or(1000).min(30_000);
                        tokio::time::sleep(Duration::from_millis(millis)).await;
                        store
                            .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                                audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                                browser_session_id: Some(browser_session.browser_session_id.clone()),
                                session_id,
                                agent_id,
                                profile_id: Some(browser_session.profile_id.clone()),
                                action: aria_core::BrowserActionKind::Wait,
                                target: browser_session.start_url.clone(),
                                metadata: serde_json::json!({"millis": millis}),
                                created_at_us: now_us,
                            })
                            .map_err(OrchestratorError::ToolError)?;
                        Ok(ToolExecutionResult::structured(
                            format!(
                                "Waited {} ms for browser session '{}' on profile '{}'.",
                                millis,
                                browser_session.browser_session_id,
                                browser_session.profile_id
                            ),
                            "browser_action",
                            serde_json::json!({
                                "browser_session_id": browser_session.browser_session_id,
                                "profile_id": browser_session.profile_id,
                                "action": "wait",
                                "millis": millis,
                            }),
                        ))
                    }
                    aria_core::BrowserInteractionKind::Navigate => {
                        let url = request.url.ok_or_else(|| {
                            OrchestratorError::ToolError(
                                "browser_act navigate requires 'url'".into(),
                            )
                        })?;
                        validate_web_url_target_syntactic(
                            &url,
                            private_network_override_enabled(),
                        )?;
                        browser_session.start_url = Some(url.clone());
                        browser_session.updated_at_us = now_us;
                        store
                            .upsert_browser_session(&browser_session, now_us)
                            .map_err(OrchestratorError::ToolError)?;
                        store
                            .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                                audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                                browser_session_id: Some(browser_session.browser_session_id.clone()),
                                session_id,
                                agent_id,
                                profile_id: Some(browser_session.profile_id.clone()),
                                action: aria_core::BrowserActionKind::Navigate,
                                target: Some(url.clone()),
                                metadata: serde_json::json!({}),
                                created_at_us: now_us,
                            })
                            .map_err(OrchestratorError::ToolError)?;
                        Ok(ToolExecutionResult::structured(
                            format!(
                                "Updated browser session '{}' on profile '{}' to '{}'.",
                                browser_session.browser_session_id,
                                browser_session.profile_id,
                                url
                            ),
                            "browser_action",
                            serde_json::json!({
                                "browser_session_id": browser_session.browser_session_id,
                                "profile_id": browser_session.profile_id,
                                "action": "navigate",
                                "url": url,
                            }),
                        ))
                    }
                    aria_core::BrowserInteractionKind::Click
                    | aria_core::BrowserInteractionKind::Type
                    | aria_core::BrowserInteractionKind::Select
                    | aria_core::BrowserInteractionKind::Scroll => {
                        let transport = browser_transport_for_session(&browser_session);
                        let bridge_payload = transport
                            .run_action(&browser_session, &request)
                            .await?;
                        let action_kind = browser_action_kind_for_interaction(request.action);
                        store
                            .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                                audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                                browser_session_id: Some(browser_session.browser_session_id.clone()),
                                session_id,
                                agent_id,
                                profile_id: Some(browser_session.profile_id.clone()),
                                action: action_kind,
                                target: request
                                    .selector
                                    .clone()
                                    .or_else(|| browser_session.start_url.clone()),
                                metadata: serde_json::json!({
                                    "bridge": bridge_payload,
                                }),
                                created_at_us: now_us,
                            })
                            .map_err(OrchestratorError::ToolError)?;
                        Ok(ToolExecutionResult::structured(
                            format!(
                                "Executed browser action '{:?}' for session '{}' on profile '{}'.",
                                request.action,
                                browser_session.browser_session_id,
                                browser_session.profile_id
                            ),
                            "browser_action",
                            serde_json::json!({
                                "browser_session_id": browser_session.browser_session_id,
                                "profile_id": browser_session.profile_id,
                                "action": request.action,
                                "result": bridge_payload,
                            }),
                        ))
                    }
                }
            }
            "browser_extract" => {
                let request: BrowserArtifactRequest = decode_tool_args(call)?;
                let url = &request.url;
                let (session_id, agent_id, sessions_dir, browser_session) = self
                    .resolve_browser_session("browser_extract", request.browser_session_id.as_deref())?;
                let (html, _content_type) = fetch_web_document(url).await?;
                let extracted = extract_html_content_for_url(Some(url), &html);
                let artifact_dir =
                    browser_session_artifacts_root(&sessions_dir, &browser_session.browser_session_id);
                create_dir_all_async(artifact_dir.clone(), "browser extract artifact dir").await?;
                let artifact_path =
                    artifact_dir.join(format!("extract-{}.txt", uuid::Uuid::new_v4()));
                write_bytes_async(
                    artifact_path.clone(),
                    extracted.text.as_bytes().to_vec(),
                    "browser extract artifact",
                )
                .await?;
                validate_artifact_size_limit(
                    aria_core::BrowserArtifactKind::ExtractedText,
                    extracted.text.len() as u64,
                )?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let artifact = aria_core::BrowserArtifactRecord {
                    artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
                    browser_session_id: browser_session.browser_session_id.clone(),
                    session_id,
                    agent_id: agent_id.clone(),
                    profile_id: browser_session.profile_id.clone(),
                    kind: aria_core::BrowserArtifactKind::ExtractedText,
                    mime_type: "text/plain; charset=utf-8".into(),
                    storage_path: artifact_path.to_string_lossy().to_string(),
                    metadata: serde_json::json!({
                        "url": url,
                        "title": extracted.title,
                        "headings": extracted.headings,
                        "excerpt": extracted.excerpt,
                        "extraction_profile": extracted.profile,
                        "site_adapter": extracted.site_adapter,
                    }),
                    created_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                run_artifact_scan_async(
                    artifact_path.clone(),
                    aria_core::BrowserArtifactKind::ExtractedText,
                    artifact.mime_type.clone(),
                )
                .await?;
                store
                    .append_browser_artifact(&artifact)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                store
                    .append_browser_action_audit(&aria_core::BrowserActionAuditRecord {
                        audit_id: format!("browser-audit-{}", uuid::Uuid::new_v4()),
                        browser_session_id: Some(browser_session.browser_session_id.clone()),
                        session_id,
                        agent_id,
                        profile_id: Some(browser_session.profile_id.clone()),
                        action: aria_core::BrowserActionKind::Extract,
                        target: Some(url.to_string()),
                        metadata: serde_json::json!({"artifact_id": artifact.artifact_id}),
                        created_at_us: now_us,
                    })
                    .map_err(OrchestratorError::ToolError)?;
                Ok(structured_payload(
                    format!(
                        "Stored browser extract artifact '{}' for session '{}'.",
                        artifact.artifact_id, browser_session.browser_session_id
                    ),
                    "browser_artifact",
                    &BrowserExtractResponse {
                        artifact,
                        text: extracted.text,
                        title: extracted.title,
                        headings: extracted.headings,
                        excerpt: extracted.excerpt,
                        extraction_profile: extracted.profile.to_string(),
                        site_adapter: extracted.site_adapter.map(str::to_string),
                    },
                ))
            }
            "fetch_url" => {
                let request: UrlRequest = decode_tool_args(call)?;
                let url = &request.url;
                let (body, content_type) = fetch_web_document(url).await?;
                Ok(structured_payload(
                    format!("Fetched '{}' ({} bytes).", url, body.len()),
                    "fetch_url",
                    &WebFetchResponse {
                        url: url.clone(),
                        content_type,
                        body,
                    },
                ))
            }
            "web_fetch" => {
                let request: UrlRequest = decode_tool_args(call)?;
                let url = &request.url;
                let (body, content_type) = fetch_web_document(url).await?;
                Ok(structured_payload(
                    format!("Fetched '{}' ({} bytes).", url, body.len()),
                    "web_fetch",
                    &WebFetchResponse {
                        url: url.clone(),
                        content_type,
                        body,
                    },
                ))
            }
            "web_extract" => {
                let request: UrlRequest = decode_tool_args(call)?;
                let url = &request.url;
                let (body, content_type) = fetch_web_document(url).await?;
                let extracted = extract_html_content_for_url(Some(url), &body);
                Ok(structured_payload(
                    format!("Extracted {} characters from '{}'.", extracted.text.len(), url),
                    "web_extract",
                    &WebExtractResponse {
                        url: url.clone(),
                        content_type,
                        text: extracted.text,
                        title: extracted.title,
                        headings: extracted.headings,
                        excerpt: extracted.excerpt,
                        extraction_profile: extracted.profile.to_string(),
                        site_adapter: extracted.site_adapter.map(str::to_string),
                    },
                ))
            }
            "crawl_page" | "crawl_site" => {
                let request: CrawlRequest = decode_tool_args(call)?;
                let target_url = &request.url;
                let seed = reqwest::Url::parse(target_url).map_err(|e| {
                    OrchestratorError::ToolError(format!("Invalid URL '{}': {}", target_url, e))
                })?;
                let scope = match call.name.as_str() {
                    "crawl_page" => aria_core::CrawlScope::SinglePage,
                    _ => request.scope.unwrap_or(aria_core::CrawlScope::SameOrigin),
                };
                let max_depth = request
                    .max_depth
                    .unwrap_or(if matches!(scope, aria_core::CrawlScope::SinglePage) {
                        0
                    } else {
                        1
                    }) as u16;
                let max_pages = request
                    .max_pages
                    .unwrap_or(if matches!(scope, aria_core::CrawlScope::SinglePage) {
                        1
                    } else {
                        10
                    }) as u32;
                let allowed_domains = Some(
                    request
                        .allowed_domains
                        .iter()
                        .map(|value| value.trim().to_ascii_lowercase())
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>(),
                )
                    .filter(|values| !values.is_empty())
                    .unwrap_or_else(|| vec![url_host_key(&seed).unwrap_or_default()]);
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "crawl tools require runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let initiated_by_agent = self
                    .invoking_agent_id
                    .clone()
                    .ok_or_else(|| OrchestratorError::ToolError("Missing invoking agent".into()))?;
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let crawl_id = format!("crawl-{}", uuid::Uuid::new_v4());
                let mut crawl_job = aria_core::CrawlJob {
                    crawl_id: crawl_id.clone(),
                    seed_url: target_url.to_string(),
                    scope,
                    allowed_domains: allowed_domains.clone(),
                    max_depth,
                    max_pages,
                    render_js: request.render_js.unwrap_or(false),
                    capture_screenshots: request.capture_screenshots.unwrap_or(false),
                    change_detection: request.change_detection.unwrap_or(true),
                    initiated_by_agent,
                    status: aria_core::CrawlJobStatus::Running,
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                let store = RuntimeStore::for_sessions_dir(&sessions_dir);
                store
                    .upsert_crawl_job(&crawl_job, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;
                let crawl_result =
                    crawl_urls(target_url, scope, max_depth, max_pages, &allowed_domains).await;
                match crawl_result {
                    Ok(pages) => {
                        crawl_job.status = aria_core::CrawlJobStatus::Completed;
                        crawl_job.updated_at_us = chrono::Utc::now().timestamp_micros() as u64;
                        store
                            .upsert_crawl_job(&crawl_job, crawl_job.updated_at_us)
                            .map_err(OrchestratorError::ToolError)?;
                        enforce_web_storage_policy(&sessions_dir)?;
                        let (website_memory, changed_paths) = update_website_memory_from_crawl(
                            &store,
                            sessions_dir,
                            &seed,
                            &pages,
                            call.name.as_str(),
                        )?;
                        let mut screenshot_artifacts = Vec::new();
                        if crawl_job.capture_screenshots
                            && !changed_paths.is_empty()
                            && self.session_id.is_some()
                        {
                            let session_id = self.session_id.expect("checked above");
                            for changed_path in &changed_paths {
                                if let Some((page_url, _)) = pages.iter().find(|(page_url, _)| {
                                    path_for_url(page_url) == *changed_path
                                }) {
                                    let artifact = capture_crawl_screenshot_artifact_async(
                                        sessions_dir.to_path_buf(),
                                        session_id,
                                        crawl_job.initiated_by_agent.clone(),
                                        crawl_job.crawl_id.clone(),
                                        page_url.as_str().to_string(),
                                    )
                                    .await?;
                                    store
                                        .append_browser_artifact(&artifact)
                                        .map_err(OrchestratorError::ToolError)?;
                                    enforce_web_storage_policy(&sessions_dir)?;
                                    store
                                        .append_browser_action_audit(
                                            &aria_core::BrowserActionAuditRecord {
                                                audit_id: format!(
                                                    "browser-audit-{}",
                                                    uuid::Uuid::new_v4()
                                                ),
                                                browser_session_id: Some(crawl_job.crawl_id.clone()),
                                                session_id,
                                                agent_id: crawl_job.initiated_by_agent.clone(),
                                                profile_id: Some("crawl-screenshot".into()),
                                                action: aria_core::BrowserActionKind::Screenshot,
                                                target: Some(page_url.as_str().to_string()),
                                                metadata: serde_json::json!({
                                                    "artifact_id": artifact.artifact_id,
                                                    "crawl_id": crawl_job.crawl_id,
                                                }),
                                                created_at_us: artifact.created_at_us,
                                            },
                                        )
                                        .map_err(OrchestratorError::ToolError)?;
                                    screenshot_artifacts.push(artifact);
                                }
                            }
                        }
                        let page_summaries = pages
                            .iter()
                            .map(|(url, body)| {
                                serde_json::json!({
                                    "url": url.as_str(),
                                    "text": extract_html_text(body),
                                })
                            })
                            .collect::<Vec<_>>();
                        Ok(ToolExecutionResult::structured(
                            format!("Crawled {} page(s) from '{}'.", page_summaries.len(), target_url),
                            "crawl_result",
                            serde_json::json!({
                                "crawl_job": crawl_job,
                                "pages": page_summaries,
                                "changed_paths": changed_paths,
                                "screenshot_artifacts": screenshot_artifacts,
                                "website_memory": website_memory,
                            }),
                        ))
                    }
                    Err(err) => {
                        crawl_job.status = aria_core::CrawlJobStatus::Failed;
                        crawl_job.updated_at_us = chrono::Utc::now().timestamp_micros() as u64;
                        let _ = store.upsert_crawl_job(&crawl_job, crawl_job.updated_at_us);
                        let _ = enforce_web_storage_policy(sessions_dir);
                        Err(err)
                    }
                }
            }
            "watch_page" | "watch_site" => {
                let request: WatchRequest = decode_tool_args(call)?;
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .map(|key| self.scoped_idempotency_key(&call.name, key));
                if let Some(key) = idempotency_key.as_deref() {
                    if let Some(cached) = idempotency_lookup(key) {
                        return Ok(cached);
                    }
                }
                let target_url = &request.url;
                let parsed = reqwest::Url::parse(target_url).map_err(|e| {
                    OrchestratorError::ToolError(format!("Invalid URL '{}': {}", target_url, e))
                })?;
                let agent_id = resolve_scheduled_agent_id(
                    request.agent_id.as_deref(),
                    self.invoking_agent_id.as_deref(),
                    if call.name == "watch_page" {
                        "this watch job"
                    } else {
                        "this site watch job"
                    },
                )?;
                let schedule_input = request.schedule.clone();
                let (normalized_schedule, spec) = schedule_input
                    .to_schedule_parts(self.user_timezone)
                    .map_err(OrchestratorError::ToolError)?;
                let watch_id = format!("watch-{}", uuid::Uuid::new_v4());
                let target_kind = if call.name == "watch_page" {
                    aria_core::WatchTargetKind::Page
                } else {
                    aria_core::WatchTargetKind::Site
                };
                let allowed_domains = vec![url_host_key(&parsed)?];
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                let watch_job = aria_core::WatchJobRecord {
                    watch_id: watch_id.clone(),
                    target_url: target_url.to_string(),
                    target_kind,
                    schedule_str: normalized_schedule.clone(),
                    agent_id: agent_id.clone(),
                    session_id: self.session_id,
                    user_id: self.user_id.clone(),
                    allowed_domains,
                    capture_screenshots: request.capture_screenshots.unwrap_or(false),
                    change_detection: request.change_detection.unwrap_or(true),
                    status: aria_core::WatchJobStatus::Scheduled,
                    last_checked_at_us: None,
                    next_check_at_us: None,
                    created_at_us: now_us,
                    updated_at_us: now_us,
                };
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "watch jobs require runtime persistence (sessions_dir)".into(),
                    )
                })?;
                enforce_watch_job_rate_limits(sessions_dir, &agent_id, target_url)?;
                RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_watch_job(&watch_job, now_us)
                    .map_err(OrchestratorError::ToolError)?;
                enforce_web_storage_policy(&sessions_dir)?;

                let prompt = match target_kind {
                    aria_core::WatchTargetKind::Page => format!(
                        "Watch page {}. Fetch and summarize meaningful changes since the previous check. Capture a screenshot only if configured or if the page changed materially.",
                        target_url
                    ),
                    aria_core::WatchTargetKind::Site => format!(
                        "Watch site {}. Crawl within the same site scope and summarize meaningful changes since the previous check. Capture screenshots only if configured or if the site changed materially.",
                        target_url
                    ),
                };
                let scheduled_job = aria_intelligence::ScheduledPromptJob {
                    id: watch_id.clone(),
                    agent_id: agent_id.clone(),
                    creator_agent: self.invoking_agent_id.clone(),
                    executor_agent: Some(agent_id.clone()),
                    notifier_agent: None,
                    prompt,
                    schedule_str: normalized_schedule.clone(),
                    kind: aria_intelligence::ScheduledJobKind::Orchestrate,
                    schedule: spec,
                    session_id: self.session_id,
                    user_id: self.user_id.clone(),
                    channel: self.channel,
                    status: aria_intelligence::ScheduledJobStatus::Scheduled,
                    last_run_at_us: None,
                    last_error: None,
                    audit_log: vec![],
                };
                self.tx_cron
                    .send(aria_intelligence::CronCommand::Add(scheduled_job.clone()))
                    .await
                    .map_err(|_| {
                        OrchestratorError::ToolError(
                            "Scheduler is unavailable; cannot add watch job".into(),
                        )
                    })?;
                let _ = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .upsert_job_snapshot(&watch_id, &scheduled_job, now_us);
                let result = ToolExecutionResult::structured(
                    format!("Scheduled watch job '{}' for '{}'.", watch_id, target_url),
                    "watch_job",
                    serde_json::to_value(&watch_job).unwrap_or_else(|_| serde_json::json!({})),
                );
                if let Some(key) = idempotency_key {
                    idempotency_store_result(key, result.clone());
                }
                Ok(result)
            }
            "list_watch_jobs" => {
                let sessions_dir = self.sessions_dir.as_ref().ok_or_else(|| {
                    OrchestratorError::ToolError(
                        "list_watch_jobs requires runtime persistence (sessions_dir)".into(),
                    )
                })?;
                let mut jobs = RuntimeStore::for_sessions_dir(&sessions_dir)
                    .list_watch_jobs()
                    .map_err(OrchestratorError::ToolError)?;
                if let Some(agent_id) = self.invoking_agent_id.as_deref() {
                    jobs.retain(|job| job.agent_id == agent_id);
                }
                Ok(ToolExecutionResult::structured(
                    format!("Found {} watch job(s).", jobs.len()),
                    "watch_jobs",
                    serde_json::to_value(&jobs).unwrap_or_else(|_| serde_json::json!([])),
                ))
            }
            _ => Err(OrchestratorError::ToolError(format!(
                "NativeToolExecutor does not support: {}",
                call.name
            ))),
        }
    }
}

pub struct MultiplexToolExecutor {
    wasm: WasmToolExecutor,
    native: NativeToolExecutor,
}

impl MultiplexToolExecutor {
    fn new(
        vault: Arc<aria_vault::CredentialVault>,
        agent_id: String,
        session_id: aria_core::Uuid,
        user_id: String,
        channel: aria_core::GatewayChannel,
        tx_cron: tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
        session_memory: aria_ssmu::SessionMemory,
        cedar: Arc<aria_policy::CedarEvaluator>,
        sessions_dir: PathBuf,
        capability_profile: Option<AgentCapabilityProfile>,
        scheduling_intent: Option<SchedulingIntent>,
        user_timezone: chrono_tz::Tz,
    ) -> Self {
        set_native_tool_vault(vault.clone());
        Self {
            wasm: WasmToolExecutor::new(
                vault,
                agent_id.clone(),
                uuid::Uuid::from_bytes(session_id),
                capability_profile.clone(),
            ),
            native: NativeToolExecutor {
                tx_cron,
                invoking_agent_id: Some(agent_id),
                session_id: Some(session_id),
                user_id: Some(user_id),
                channel: Some(channel),
                session_memory: Some(session_memory),
                cedar: Some(cedar),
                sessions_dir: Some(sessions_dir),
                scheduling_intent,
                user_timezone,
            },
        }
    }
}

#[async_trait::async_trait]
impl ToolExecutor for MultiplexToolExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        match call.name.as_str() {
            "read_file"
            | "write_file"
            | "run_shell"
            | "search_codebase"
            | "run_tests"
            | "manage_cron"
            | "schedule_message"
            | "set_reminder"
            | "compact_session"
            | "grant_access"
            | "manage_prompts"
            | "set_domain_access_decision"
            | "browser_profile_create"
            | "browser_profile_list"
            | "browser_profile_use"
            | "browser_session_start"
            | "browser_session_list"
            | "browser_session_status"
            | "browser_session_cleanup"
            | "browser_session_pause"
            | "browser_session_resume"
            | "browser_session_record_challenge"
            | "browser_login_status"
            | "browser_login_begin_manual"
            | "browser_login_complete_manual"
            | "browser_login_fill_credentials"
            | "browser_open"
            | "browser_act"
            | "browser_snapshot"
            | "browser_screenshot"
            | "browser_extract"
            | "browser_download"
            | "fetch_url"
            | "web_fetch"
            | "web_extract"
            | "crawl_page"
            | "crawl_site"
            | "watch_page"
            | "watch_site"
            | "list_watch_jobs"
            | "spawn_agent"
            | "cancel_agent_run"
            | "retry_agent_run"
            | "list_agent_runs"
            | "get_agent_run"
            | "get_agent_run_events"
            | "get_agent_mailbox"
            | "scaffold_skill"
            | "install_skill_from_dir"
            | "export_skill_manifest"
            | "export_signed_skill_manifest"
            | "install_signed_skill_from_dir"
            | "install_skill"
            | "bind_skill"
            | "activate_skill"
            | "execute_skill"
            | "register_mcp_server"
            | "import_mcp_tool"
            | "import_mcp_prompt"
            | "import_mcp_resource"
            | "bind_mcp_import"
            | "invoke_mcp_tool"
            | "render_mcp_prompt"
            | "read_mcp_resource" => self.native.execute(call).await,
            _ => self.wasm.execute(call).await,
        }
    }
}

struct PolicyCheckedExecutor<T: ToolExecutor> {
    inner: T,
    cedar: Arc<aria_policy::CedarEvaluator>,
    principal: String,
    channel: aria_core::GatewayChannel,
    whitelist: Vec<String>,
    forbid: Vec<String>,
    capability_profile: Option<AgentCapabilityProfile>,
    sessions_dir: Option<PathBuf>,
    session_id: Option<aria_core::Uuid>,
    firewall: Option<aria_safety::DfaFirewall>,
}

impl<T: ToolExecutor> PolicyCheckedExecutor<T> {
    fn new(
        inner: T,
        cedar: Arc<aria_policy::CedarEvaluator>,
        principal: String,
        channel: aria_core::GatewayChannel,
        whitelist: Vec<String>,
        forbid: Vec<String>,
        capability_profile: Option<AgentCapabilityProfile>,
        sessions_dir: Option<PathBuf>,
        session_id: Option<aria_core::Uuid>,
    ) -> Self {
        Self {
            inner,
            cedar,
            principal,
            channel,
            whitelist,
            forbid,
            capability_profile,
            sessions_dir,
            session_id,
            firewall: None,
        }
    }

    fn with_firewall(mut self, firewall: aria_safety::DfaFirewall) -> Self {
        self.firewall = Some(firewall);
        self
    }

    fn to_ast_call(call: &ToolCall) -> String {
        let mut ast_args = Vec::new();
        if let Ok(value) = decode_tool_args::<serde_json::Value>(call) {
            if let Some(obj) = value.as_object() {
                for (k, v) in obj {
                    let v_str = if let Some(s) = v.as_str() {
                        if matches!(call.name.as_str(), "read_file" | "write_file") && k == "path" {
                            resolve_scoped_path(s)
                                .map(|path| path.display().to_string())
                                .unwrap_or_else(|_| s.to_string())
                        } else if call.name == "run_shell" && k == "cwd" {
                            resolve_scoped_path(s)
                                .map(|path| path.display().to_string())
                                .unwrap_or_else(|_| s.to_string())
                        } else if call.name == "register_mcp_server" && k == "endpoint" {
                            normalize_mcp_endpoint_for_policy(s)
                        } else {
                            s.to_string()
                        }
                    } else {
                        v.to_string()
                    };
                    let escaped = v_str.replace('"', "\\\"");
                    ast_args.push(format!(r#"{}="{}""#, k, escaped));
                }
            }
        }
        format!("{}({})", call.name, ast_args.join(", "))
    }
}

fn append_scope_denial_record(
    sessions_dir: Option<&Path>,
    agent_id: &str,
    session_id: Option<aria_core::Uuid>,
    kind: ScopeDenialKind,
    target: impl Into<String>,
    reason: impl Into<String>,
) {
    let Some(sessions_dir) = sessions_dir else {
        return;
    };
    let record = ScopeDenialRecord {
        denial_id: uuid::Uuid::new_v4().to_string(),
        kind,
        agent_id: agent_id.to_string(),
        session_id,
        target: target.into(),
        reason: reason.into(),
        created_at_us: chrono::Utc::now().timestamp_micros() as u64,
    };
    let _ = RuntimeStore::for_sessions_dir(&sessions_dir).append_scope_denial(&record);
}

fn append_secret_usage_audit_record(
    sessions_dir: Option<&Path>,
    agent_id: &str,
    session_id: Option<aria_core::Uuid>,
    tool_name: &str,
    key_name: &str,
    target_domain: &str,
    outcome: aria_core::SecretUsageOutcome,
    detail: impl Into<String>,
) {
    let Some(sessions_dir) = sessions_dir else {
        return;
    };
    let record = aria_core::SecretUsageAuditRecord {
        audit_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        session_id,
        tool_name: tool_name.to_string(),
        key_name: key_name.to_string(),
        target_domain: target_domain.to_string(),
        outcome,
        detail: detail.into(),
        created_at_us: chrono::Utc::now().timestamp_micros() as u64,
    };
    let _ = RuntimeStore::for_sessions_dir(sessions_dir).append_secret_usage_audit(&record);
}

fn trust_profile_is_untrusted(profile: Option<&AgentCapabilityProfile>) -> bool {
    matches!(
        profile.and_then(|p| p.trust_profile),
        Some(aria_core::TrustProfile::UntrustedWeb | aria_core::TrustProfile::UntrustedSocial)
    )
}

fn validate_execution_profile(
    capability_profile: Option<&AgentCapabilityProfile>,
    channel: GatewayChannel,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    let Some(profile) = capability_profile else {
        return Ok(());
    };
    match call.name.as_str() {
        "run_shell" if trust_profile_is_untrusted(capability_profile) => {
            let request: RunShellRequest = decode_tool_args(call)?;
            if !request.os_containment.unwrap_or(false) {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::ExecutionProfile,
                    "run_shell",
                    "run_shell requires os_containment for untrusted agents",
                );
                return Err(OrchestratorError::ToolError(
                    "run_shell requires os_containment for untrusted agents".into(),
                ));
            }
        }
        "browser_download" | "browser_login_begin_manual" | "browser_login_complete_manual"
        | "browser_login_fill_credentials" => {
            if matches!(profile.trust_profile, Some(aria_core::TrustProfile::UntrustedSocial))
                && !matches!(channel, GatewayChannel::Cli | GatewayChannel::WebSocket)
            {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::ExecutionProfile,
                    call.name.as_str(),
                    format!(
                        "tool '{}' is blocked for untrusted social agents on channel {:?}",
                        call.name, channel
                    ),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "tool '{}' is blocked for untrusted social agents on channel {:?}",
                    call.name, channel
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_network_egress_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    channel: GatewayChannel,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    let Some(profile) = capability_profile else {
        return Ok(());
    };
    let Some((domain, action_family)) = extract_web_target(call)? else {
        return Ok(());
    };
    let high_risk_action = matches!(
        action_family,
        aria_core::WebActionFamily::InteractiveWrite
            | aria_core::WebActionFamily::Login
            | aria_core::WebActionFamily::Download
    );
    if high_risk_action && !capability_allows_external_network(capability_profile) {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::NetworkEgress,
            domain.clone(),
            format!(
                "external network egress is not permitted for agent '{}' targeting '{}'",
                profile.agent_id, domain
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "external network egress is not permitted for agent '{}' targeting '{}'",
            profile.agent_id, domain
        )));
    }
    if matches!(profile.trust_profile, Some(aria_core::TrustProfile::UntrustedSocial))
        && matches!(
            action_family,
            aria_core::WebActionFamily::InteractiveWrite
                | aria_core::WebActionFamily::Login
                | aria_core::WebActionFamily::Download
        )
        && !matches!(channel, GatewayChannel::Cli | GatewayChannel::WebSocket)
    {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::NetworkEgress,
            domain.clone(),
            format!(
                "channel {:?} is not allowed for high-risk web action '{:?}' on '{}'",
                channel, action_family, domain
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "channel {:?} is not allowed for high-risk web action '{:?}' on '{}'",
            channel, action_family, domain
        )));
    }
    Ok(())
}

fn validate_spawn_agent_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    if call.name != "spawn_agent" {
        return Ok(());
    }

    let request: AgentSpawnRequest = decode_tool_args(call)
        .map_err(|e| OrchestratorError::ToolError(format!("Invalid args: {}", e)))?;
    let profile = capability_profile.ok_or_else(|| {
        OrchestratorError::ToolError(
            "spawn_agent not permitted without an active delegation scope".into(),
        )
    })?;
    let Some(scope) = profile.delegation_scope.as_ref() else {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::DelegationScope,
            "spawn_agent",
            format!(
                "spawn_agent not permitted for agent '{}' without delegation scope",
                profile.agent_id
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "spawn_agent not permitted for agent '{}' without delegation scope",
            profile.agent_id
        )));
    };

    if !scope.can_spawn_children {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::DelegationScope,
            request.agent_id.clone(),
            format!(
                "spawn_agent not permitted for agent '{}' because child delegation is disabled",
                profile.agent_id
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "spawn_agent not permitted for agent '{}' because child delegation is disabled",
            profile.agent_id
        )));
    }

    if !scope.allowed_agents.is_empty()
        && !scope
            .allowed_agents
            .iter()
            .any(|allowed| allowed == &request.agent_id)
    {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::DelegationScope,
            request.agent_id.clone(),
            format!(
                "spawn_agent not permitted for child agent '{}'",
                request.agent_id
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "spawn_agent not permitted for child agent '{}'",
            request.agent_id
        )));
    }

    if let Some(requested_seconds) = request.max_runtime_seconds {
        if scope.max_runtime_seconds > 0 && requested_seconds > scope.max_runtime_seconds {
            append_scope_denial_record(
                sessions_dir,
                &profile.agent_id,
                session_id,
                ScopeDenialKind::DelegationScope,
                request.agent_id.clone(),
                format!(
                    "spawn_agent requested runtime {}s exceeds allowed {}s",
                    requested_seconds, scope.max_runtime_seconds
                ),
            );
            return Err(OrchestratorError::ToolError(format!(
                "spawn_agent requested runtime {}s exceeds allowed {}s",
                requested_seconds, scope.max_runtime_seconds
            )));
        }
    }

    if scope.max_fanout > 0 {
        let parent_run_id = request
            .parent_run_id
            .clone()
            .or_else(|| session_id.map(|sid| format!("session:{}", uuid::Uuid::from_bytes(sid))));
        if let (Some(dir), Some(parent_run_id)) = (sessions_dir, parent_run_id.as_deref()) {
            let active = RuntimeStore::for_sessions_dir(dir)
                .count_active_agent_runs_for_parent(parent_run_id)
                .map_err(OrchestratorError::ToolError)?;
            if active >= scope.max_fanout as usize {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::DelegationScope,
                    parent_run_id.to_string(),
                    format!(
                        "spawn_agent exceeds max fanout {} for parent '{}'",
                        scope.max_fanout, parent_run_id
                    ),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "spawn_agent exceeds max fanout {} for parent '{}'",
                    scope.max_fanout, parent_run_id
                )));
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum FilesystemAction {
    Read,
    Write,
    Execute,
}

fn normalize_filesystem_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn filesystem_action_allowed(scope: &aria_core::FilesystemScope, action: FilesystemAction) -> bool {
    match action {
        FilesystemAction::Read => scope.allow_read,
        FilesystemAction::Write => scope.allow_write,
        FilesystemAction::Execute => scope.allow_execute,
    }
}

fn resolve_scoped_path(path: &str) -> Result<PathBuf, OrchestratorError> {
    let raw = PathBuf::from(path);
    let absolute = if raw.is_absolute() {
        raw
    } else {
        std::env::current_dir()
            .map_err(|e| OrchestratorError::ToolError(format!("resolve path failed: {}", e)))?
            .join(raw)
    };
    Ok(normalize_filesystem_path(&absolute))
}

fn filesystem_path_allowed(
    profile: &AgentCapabilityProfile,
    path: &Path,
    action: FilesystemAction,
) -> bool {
    profile.filesystem_scopes.iter().any(|scope| {
        if !filesystem_action_allowed(scope, action) {
            return false;
        }
        let root = normalize_filesystem_path(Path::new(&scope.root_path));
        path.starts_with(&root)
    })
}

fn validate_run_shell_command_paths(
    profile: &AgentCapabilityProfile,
    command: &str,
) -> Result<(), OrchestratorError> {
    if command.contains("&&")
        || command.contains("||")
        || command.contains(';')
        || command.contains('|')
        || command.contains('>')
        || command.contains('<')
        || command.contains("$(")
        || command.contains('`')
    {
        return Err(OrchestratorError::ToolError(
            "run_shell command contains disallowed shell control operators".into(),
        ));
    }
    for token in command.split_whitespace() {
        if token.contains("..") {
            return Err(OrchestratorError::ToolError(
                "run_shell not permitted with parent-directory traversal".into(),
            ));
        }
        if token.starts_with('/') {
            let candidate = normalize_filesystem_path(Path::new(token));
            if !filesystem_path_allowed(profile, &candidate, FilesystemAction::Execute) {
                return Err(OrchestratorError::ToolError(format!(
                    "run_shell absolute path '{}' is outside execute scope",
                    token
                )));
            }
        }
    }
    Ok(())
}

fn command_exists_on_path(command: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path_var).any(|path| {
        let candidate = path.join(command);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            let candidate_exe = path.join(format!("{}.exe", command));
            if candidate_exe.is_file() {
                return true;
            }
        }
        false
    })
}

fn shell_containment_backend_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "bwrap"
    }
    #[cfg(target_os = "macos")]
    {
        "sandbox-exec"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        "unsupported"
    }
}

fn build_os_contained_shell_command(
    command: &str,
    cwd: Option<&str>,
) -> Result<tokio::process::Command, OrchestratorError> {
    #[cfg(target_os = "linux")]
    {
        let cwd = cwd.ok_or_else(|| {
            OrchestratorError::ToolError(
                "run_shell os_containment requires a scoped 'cwd'".into(),
            )
        })?;
        if !command_exists_on_path("bwrap") {
            return Err(OrchestratorError::ToolError(
                "run_shell os_containment requested but 'bwrap' is not installed".into(),
            ));
        }
        let mut cmd = tokio::process::Command::new("bwrap");
        cmd.arg("--die-with-parent")
            .arg("--new-session")
            .arg("--unshare-all")
            .arg("--share-net")
            .arg("--ro-bind")
            .arg("/")
            .arg("/")
            .arg("--bind")
            .arg(cwd)
            .arg(cwd)
            .arg("--chdir")
            .arg(cwd)
            .arg("sh")
            .arg("-c")
            .arg(command);
        return Ok(cmd);
    }
    #[cfg(target_os = "macos")]
    {
        let cwd = cwd.ok_or_else(|| {
            OrchestratorError::ToolError(
                "run_shell os_containment requires a scoped 'cwd'".into(),
            )
        })?;
        if !command_exists_on_path("sandbox-exec") {
            return Err(OrchestratorError::ToolError(
                "run_shell os_containment requested but 'sandbox-exec' is not installed".into(),
            ));
        }
        let escaped = cwd.replace('\\', "\\\\").replace('"', "\\\"");
        let profile = format!(
            "(version 1)\n(deny default)\n(allow process*)\n(allow file-read*)\n(allow file-write* (subpath \"{}\"))\n",
            escaped
        );
        let mut cmd = tokio::process::Command::new("sandbox-exec");
        cmd.arg("-p")
            .arg(profile)
            .arg("sh")
            .arg("-c")
            .arg(command);
        return Ok(cmd);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = command;
        let _ = cwd;
        Err(OrchestratorError::ToolError(
            "run_shell os_containment is not supported on this OS".into(),
        ))
    }
}

fn browser_bridge_containment_requested() -> bool {
    runtime_env().browser_automation_os_containment
}

fn browser_bridge_containment_backend_name() -> &'static str {
    shell_containment_backend_name()
}

fn browser_bridge_containment_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        command_exists_on_path("bwrap")
    }
    #[cfg(target_os = "macos")]
    {
        command_exists_on_path("sandbox-exec")
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

fn dedupe_normalized_paths(paths: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();
    for path in paths {
        let normalized = normalize_filesystem_path(&path);
        if seen.insert(normalized.clone()) {
            ordered.push(normalized);
        }
    }
    ordered
}

fn build_os_contained_process_command(
    program: &str,
    args: &[String],
    writable_dirs: &[PathBuf],
) -> Result<std::process::Command, OrchestratorError> {
    #[cfg(target_os = "linux")]
    {
        if !command_exists_on_path("bwrap") {
            return Err(OrchestratorError::ToolError(
                "browser bridge os_containment requested but 'bwrap' is not installed".into(),
            ));
        }
        let mut cmd = std::process::Command::new("bwrap");
        cmd.arg("--die-with-parent")
            .arg("--new-session")
            .arg("--unshare-all")
            .arg("--share-net")
            .arg("--ro-bind")
            .arg("/")
            .arg("/");
        for dir in writable_dirs {
            cmd.arg("--bind").arg(dir).arg(dir);
        }
        cmd.arg(program).args(args);
        return Ok(cmd);
    }
    #[cfg(target_os = "macos")]
    {
        if !command_exists_on_path("sandbox-exec") {
            return Err(OrchestratorError::ToolError(
                "browser bridge os_containment requested but 'sandbox-exec' is not installed"
                    .into(),
            ));
        }
        let mut profile = String::from("(version 1)\n(deny default)\n(allow process*)\n(allow file-read*)\n");
        for dir in writable_dirs {
            let escaped = dir.to_string_lossy().replace('\\', "\\\\").replace('"', "\\\"");
            profile.push_str(&format!(
                "(allow file-write* (subpath \"{}\"))\n",
                escaped
            ));
        }
        let mut cmd = std::process::Command::new("sandbox-exec");
        cmd.arg("-p").arg(profile).arg(program).args(args);
        return Ok(cmd);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = program;
        let _ = args;
        let _ = writable_dirs;
        Err(OrchestratorError::ToolError(
            "browser bridge os_containment is not supported on this OS".into(),
        ))
    }
}

fn validate_filesystem_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    let Some(profile) = capability_profile else {
        return Ok(());
    };
    if profile.filesystem_scopes.is_empty() {
        return Ok(());
    }

    match call.name.as_str() {
        "read_file" | "write_file" => {
            let action = if call.name == "read_file" {
                FilesystemAction::Read
            } else {
                FilesystemAction::Write
            };
            let path = if call.name == "read_file" {
                decode_tool_args::<ReadFileRequest>(call)?.path
            } else {
                decode_tool_args::<WriteFileRequest>(call)?.path
            };
            let resolved = resolve_scoped_path(&path)?;
            if !filesystem_path_allowed(profile, &resolved, action) {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::FilesystemScope,
                    path.to_string(),
                    format!("{} not permitted for path '{}'", call.name, path),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "{} not permitted for path '{}'",
                    call.name, path
                )));
            }
        }
        "run_shell" => {
            let request: RunShellRequest = decode_tool_args(call)?;
            let cwd = request.cwd.as_deref().ok_or_else(|| {
                OrchestratorError::ToolError(
                    "run_shell requires a scoped 'cwd' when filesystem scopes are active".into(),
                )
            })?;
            let command = request.command.as_str();
            let resolved_cwd = resolve_scoped_path(cwd)?;
            if !filesystem_path_allowed(profile, &resolved_cwd, FilesystemAction::Execute) {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::FilesystemScope,
                    cwd.to_string(),
                    format!("run_shell not permitted for cwd '{}'", cwd),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "run_shell not permitted for cwd '{}'",
                    cwd
                )));
            }
            if let Err(err) = validate_run_shell_command_paths(profile, command) {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::FilesystemScope,
                    command.to_string(),
                    format!("{}", err),
                );
                return Err(err);
            }
        }
        _ => {}
    }

    Ok(())
}

fn validate_skill_activation_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if !matches!(call.name.as_str(), "activate_skill" | "execute_skill") {
        return Ok(());
    }

    let skill_id = if call.name == "activate_skill" {
        decode_tool_args::<ActivateSkillRequest>(call)?.skill_id
    } else {
        decode_tool_args::<ExecuteSkillRequest>(call)?.skill_id
    };
    let profile = capability_profile.ok_or_else(|| {
        OrchestratorError::ToolError(format!(
            "{} not permitted without an active capability profile",
            call.name
        ))
    })?;

    if !profile.skill_allowlist.is_empty()
        && !profile
            .skill_allowlist
            .iter()
            .any(|allowed| allowed == &skill_id)
    {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            None,
            ScopeDenialKind::SkillScope,
            skill_id.to_string(),
            format!("{} not permitted for skill '{}'", call.name, skill_id),
        );
        return Err(OrchestratorError::ToolError(format!(
            "{} not permitted for skill '{}'",
            call.name, skill_id
        )));
    }

    let sessions_dir = sessions_dir.ok_or_else(|| {
        OrchestratorError::ToolError(format!("{} requires runtime persistence", call.name))
    })?;
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let manifests = store
        .list_skill_packages()
        .map_err(OrchestratorError::ToolError)?;
    let manifest = manifests
        .into_iter()
        .find(|manifest| manifest.skill_id == skill_id)
        .ok_or_else(|| {
            OrchestratorError::ToolError(format!("{} unknown skill '{}'", call.name, skill_id))
        })?;
    if !manifest.enabled {
        return Err(OrchestratorError::ToolError(format!(
            "{} denied because skill '{}' is disabled",
            call.name, skill_id
        )));
    }

    let binding_allowed = store
        .list_skill_bindings_for_agent(&profile.agent_id)
        .map_err(OrchestratorError::ToolError)?
        .into_iter()
        .any(|binding| binding.skill_id == skill_id);
    if !binding_allowed {
        append_scope_denial_record(
            Some(sessions_dir),
            &profile.agent_id,
            None,
            ScopeDenialKind::SkillScope,
            skill_id.to_string(),
            format!(
                "{} not permitted because skill '{}' is not bound to agent '{}'",
                call.name, skill_id, profile.agent_id
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "{} not permitted because skill '{}' is not bound to agent '{}'",
            call.name, skill_id, profile.agent_id
        )));
    }

    if call.name == "execute_skill" {
        if manifest.wasm_module_ref.is_none() {
            return Err(OrchestratorError::ToolError(format!(
                "execute_skill skill '{}' has no wasm_module_ref",
                skill_id
            )));
        }
        let active = store
            .list_skill_activations_for_agent(&profile.agent_id)
            .map_err(OrchestratorError::ToolError)?
            .into_iter()
            .any(|activation| activation.skill_id == skill_id && activation.active);
        if !active {
            append_scope_denial_record(
                Some(sessions_dir),
                &profile.agent_id,
                None,
                ScopeDenialKind::SkillScope,
                skill_id.to_string(),
                format!(
                    "execute_skill not permitted because skill '{}' is not active for agent '{}'",
                    skill_id, profile.agent_id
                ),
            );
            return Err(OrchestratorError::ToolError(format!(
                "execute_skill not permitted because skill '{}' is not active for agent '{}'",
                skill_id, profile.agent_id
            )));
        }
    }

    Ok(())
}

fn resolve_skill_module_path(wasm_ref: &str) -> Result<PathBuf, OrchestratorError> {
    let path = PathBuf::from(wasm_ref);
    if path.is_absolute() {
        return Ok(path);
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|e| {
            OrchestratorError::ToolError(format!("resolve skill module path failed: {}", e))
        })
}

#[cfg(feature = "mcp-runtime")]
fn validate_mcp_tool_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    principal_agent_id: &str,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name != "invoke_mcp_tool" {
        return Ok(());
    }

    let request: InvokeMcpToolRequest = decode_tool_args(call)?;
    let server_id = request.server_id;
    let tool_name = request.tool_name;
    let sessions_dir = sessions_dir.ok_or_else(|| {
        OrchestratorError::ToolError("invoke_mcp_tool requires runtime persistence".into())
    })?;
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    if let Some(profile) = capability_profile {
        let registry = load_mcp_registry_from_store(&store).map_err(OrchestratorError::ToolError)?;
        if !registry.tool_allowed_for_agent(profile, &server_id, &tool_name) {
            append_scope_denial_record(
                Some(sessions_dir),
                &profile.agent_id,
                None,
                ScopeDenialKind::McpToolScope,
                format!("{}::{}", server_id, tool_name),
                format!(
                    "invoke_mcp_tool not permitted for imported tool '{}::{}'",
                    server_id, tool_name
                ),
            );
            return Err(OrchestratorError::ToolError(format!(
                "invoke_mcp_tool not permitted for imported tool '{}::{}'",
                server_id, tool_name
            )));
        }
    }
    if !mcp_binding_exists(
        &store,
        principal_agent_id,
        &server_id,
        McpPrimitiveKind::Tool,
        &tool_name,
    )
    .map_err(OrchestratorError::ToolError)?
    {
        append_scope_denial_record(
            Some(sessions_dir),
            principal_agent_id,
            None,
            ScopeDenialKind::McpToolScope,
            format!("{}::{}", server_id, tool_name),
            format!(
                "invoke_mcp_tool not permitted because binding is missing for '{}::{}'",
                server_id, tool_name
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "invoke_mcp_tool not permitted because binding is missing for '{}::{}'",
            server_id, tool_name
        )));
    }

    Ok(())
}

#[cfg(not(feature = "mcp-runtime"))]
fn validate_mcp_tool_request(
    _capability_profile: Option<&AgentCapabilityProfile>,
    _principal_agent_id: &str,
    call: &ToolCall,
    _sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name == "invoke_mcp_tool" {
        return Err(disabled_feature_tool_error(call, "mcp-runtime"));
    }
    Ok(())
}

#[cfg(feature = "mcp-runtime")]
fn validate_mcp_prompt_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    principal_agent_id: &str,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name != "render_mcp_prompt" {
        return Ok(());
    }

    let request: RenderMcpPromptRequest = decode_tool_args(call)?;
    let server_id = request.server_id;
    let prompt_name = request.prompt_name;
    let sessions_dir = sessions_dir.ok_or_else(|| {
        OrchestratorError::ToolError("render_mcp_prompt requires runtime persistence".into())
    })?;
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    if let Some(profile) = capability_profile {
        let registry = load_mcp_registry_from_store(&store).map_err(OrchestratorError::ToolError)?;
        if !registry.prompt_allowed_for_agent(profile, &server_id, &prompt_name) {
            append_scope_denial_record(
                Some(sessions_dir),
                &profile.agent_id,
                None,
                ScopeDenialKind::McpPromptScope,
                format!("{}::{}", server_id, prompt_name),
                format!(
                    "render_mcp_prompt not permitted for imported prompt '{}::{}'",
                    server_id, prompt_name
                ),
            );
            return Err(OrchestratorError::ToolError(format!(
                "render_mcp_prompt not permitted for imported prompt '{}::{}'",
                server_id, prompt_name
            )));
        }
    }
    if !mcp_binding_exists(
        &store,
        principal_agent_id,
        &server_id,
        McpPrimitiveKind::Prompt,
        &prompt_name,
    )
    .map_err(OrchestratorError::ToolError)?
    {
        append_scope_denial_record(
            Some(sessions_dir),
            principal_agent_id,
            None,
            ScopeDenialKind::McpPromptScope,
            format!("{}::{}", server_id, prompt_name),
            format!(
                "render_mcp_prompt not permitted because binding is missing for '{}::{}'",
                server_id, prompt_name
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "render_mcp_prompt not permitted because binding is missing for '{}::{}'",
            server_id, prompt_name
        )));
    }

    Ok(())
}

#[cfg(not(feature = "mcp-runtime"))]
fn validate_mcp_prompt_request(
    _capability_profile: Option<&AgentCapabilityProfile>,
    _principal_agent_id: &str,
    call: &ToolCall,
    _sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name == "render_mcp_prompt" {
        return Err(disabled_feature_tool_error(call, "mcp-runtime"));
    }
    Ok(())
}

#[cfg(feature = "mcp-runtime")]
fn validate_mcp_resource_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    principal_agent_id: &str,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name != "read_mcp_resource" {
        return Ok(());
    }

    let request: ReadMcpResourceRequest = decode_tool_args(call)?;
    let server_id = request.server_id;
    let resource_uri = request.resource_uri;
    if let Some(profile) = capability_profile {
        if !profile.retrieval_scopes.is_empty()
            && !profile
                .retrieval_scopes
                .contains(&aria_core::RetrievalScope::McpResource)
        {
            append_scope_denial_record(
                sessions_dir,
                &profile.agent_id,
                None,
                ScopeDenialKind::RetrievalScope,
                format!("{}::{}", server_id, resource_uri),
                "read_mcp_resource not permitted without MCP resource retrieval scope",
            );
            return Err(OrchestratorError::ToolError(
                "read_mcp_resource not permitted without MCP resource retrieval scope".into(),
            ));
        }
    }
    let sessions_dir = sessions_dir.ok_or_else(|| {
        OrchestratorError::ToolError("read_mcp_resource requires runtime persistence".into())
    })?;
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    if let Some(profile) = capability_profile {
        let registry = load_mcp_registry_from_store(&store).map_err(OrchestratorError::ToolError)?;
        if !registry.resource_allowed_for_agent(profile, &server_id, &resource_uri) {
            append_scope_denial_record(
                Some(sessions_dir),
                &profile.agent_id,
                None,
                ScopeDenialKind::McpResourceScope,
                format!("{}::{}", server_id, resource_uri),
                format!(
                    "read_mcp_resource not permitted for imported resource '{}::{}'",
                    server_id, resource_uri
                ),
            );
            return Err(OrchestratorError::ToolError(format!(
                "read_mcp_resource not permitted for imported resource '{}::{}'",
                server_id, resource_uri
            )));
        }
    }
    if !mcp_binding_exists(
        &store,
        principal_agent_id,
        &server_id,
        McpPrimitiveKind::Resource,
        &resource_uri,
    )
    .map_err(OrchestratorError::ToolError)?
    {
        append_scope_denial_record(
            Some(sessions_dir),
            principal_agent_id,
            None,
            ScopeDenialKind::McpResourceScope,
            format!("{}::{}", server_id, resource_uri),
            format!(
                "read_mcp_resource not permitted because binding is missing for '{}::{}'",
                server_id, resource_uri
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "read_mcp_resource not permitted because binding is missing for '{}::{}'",
            server_id, resource_uri
        )));
    }

    Ok(())
}

#[cfg(not(feature = "mcp-runtime"))]
fn validate_mcp_resource_request(
    _capability_profile: Option<&AgentCapabilityProfile>,
    _principal_agent_id: &str,
    call: &ToolCall,
    _sessions_dir: Option<&Path>,
) -> Result<(), OrchestratorError> {
    if call.name == "read_mcp_resource" {
        return Err(disabled_feature_tool_error(call, "mcp-runtime"));
    }
    Ok(())
}

fn domain_matches_rule(domain: &str, rule: &str) -> bool {
    domain == rule || domain.ends_with(&format!(".{}", rule))
}

fn normalize_domain_value(input: &str) -> Result<String, OrchestratorError> {
    let trimmed = input.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(OrchestratorError::ToolError(
            "domain must not be empty".to_string(),
        ));
    }
    if trimmed.contains("://") {
        let parsed = reqwest::Url::parse(&trimmed).map_err(|e| {
            OrchestratorError::ToolError(format!("Invalid domain or URL '{}': {}", input, e))
        })?;
        let Some(domain) = parsed.domain() else {
            return Err(OrchestratorError::ToolError(format!(
                "URL '{}' does not contain a valid domain",
                input
            )));
        };
        return Ok(domain.to_ascii_lowercase());
    }
    if trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-'))
    {
        return Ok(trimmed);
    }
    Err(OrchestratorError::ToolError(format!(
        "Invalid domain '{}'",
        input
    )))
}

fn url_host_key(url: &reqwest::Url) -> Result<String, OrchestratorError> {
    url.domain()
        .map(|domain| domain.to_ascii_lowercase())
        .or_else(|| url.host_str().map(|host| host.to_ascii_lowercase()))
        .ok_or_else(|| OrchestratorError::ToolError(format!(
            "URL '{}' does not contain a valid host",
            url
        )))
}

fn extract_web_target(
    call: &ToolCall,
) -> Result<Option<(String, aria_core::WebActionFamily)>, OrchestratorError> {
    match call.name.as_str() {
        "fetch_url" | "web_fetch" | "web_extract" | "browser_open" | "browser_snapshot"
        | "browser_screenshot" | "browser_extract" | "browser_download" | "crawl_page"
        | "crawl_site" => {
            let url = match call.name.as_str() {
                "fetch_url" | "web_fetch" | "web_extract" => decode_tool_args::<UrlRequest>(call)?.url,
                "browser_open" => decode_tool_args::<BrowserOpenRequest>(call)?.url,
                "browser_snapshot" | "browser_screenshot" | "browser_extract" => {
                    decode_tool_args::<BrowserArtifactRequest>(call)?.url
                }
                "browser_download" => decode_tool_args::<BrowserDownloadRequest>(call)?.url,
                "crawl_page" | "crawl_site" => decode_tool_args::<CrawlRequest>(call)?.url,
                _ => return Ok(None),
            };
            let parsed = reqwest::Url::parse(&url)
                .map_err(|e| OrchestratorError::ToolError(format!("Invalid URL '{}': {}", url, e)))?;
            Ok(Some((
                url_host_key(&parsed)?,
                match call.name.as_str() {
                    "browser_open" => aria_core::WebActionFamily::InteractiveRead,
                    "browser_screenshot" => aria_core::WebActionFamily::Screenshot,
                    "browser_download" => aria_core::WebActionFamily::Download,
                    "crawl_page" | "crawl_site" => aria_core::WebActionFamily::Crawl,
                    _ => aria_core::WebActionFamily::Fetch,
                },
            )))
        }
        "browser_act" => {
            let Some(request) = extract_browser_action_request(call)? else {
                return Ok(None);
            };
            let Some(url) = request.url.as_deref() else {
                return Ok(None);
            };
            let parsed = reqwest::Url::parse(url)
                .map_err(|e| OrchestratorError::ToolError(format!("Invalid URL '{}': {}", url, e)))?;
            Ok(Some((
                url_host_key(&parsed)?,
                match request.action {
                    aria_core::BrowserInteractionKind::Navigate
                    | aria_core::BrowserInteractionKind::Wait
                    | aria_core::BrowserInteractionKind::Scroll => {
                        aria_core::WebActionFamily::InteractiveRead
                    }
                    aria_core::BrowserInteractionKind::Click
                    | aria_core::BrowserInteractionKind::Type
                    | aria_core::BrowserInteractionKind::Select => {
                        aria_core::WebActionFamily::InteractiveWrite
                    }
                },
            )))
        }
        "browser_login_status" | "browser_login_begin_manual" | "browser_login_complete_manual"
        | "browser_login_fill_credentials" => {
            let domain = match call.name.as_str() {
                "browser_login_begin_manual" | "browser_login_status" => {
                    if call.name == "browser_login_status" {
                        decode_tool_args::<BrowserLoginStatusRequest>(call)?
                            .domain
                            .unwrap_or_default()
                    } else {
                        decode_tool_args::<BrowserLoginStatusRequest>(call)?
                            .domain
                            .unwrap_or_default()
                    }
                }
                "browser_login_complete_manual" => {
                    decode_tool_args::<BrowserLoginCompleteRequest>(call)?.domain
                }
                "browser_login_fill_credentials" => {
                    decode_tool_args::<BrowserLoginFillCredentialsRequest>(call)?.domain
                }
                _ => return Ok(None),
            };
            if domain.trim().is_empty() {
                Ok(None)
            } else {
                Ok(Some((
                    resolve_browser_login_domain(&domain)?,
                    aria_core::WebActionFamily::Login,
                )))
            }
        }
        _ => Ok(None),
    }
}

fn resolve_browser_session_id_or_current(
    sessions_dir: &Path,
    session_id: aria_core::Uuid,
    agent_id: &str,
    browser_session_id: Option<&str>,
) -> Result<String, OrchestratorError> {
    if let Some(browser_session_id) = browser_session_id {
        let browser_session_id = required_trimmed(browser_session_id, "browser_session_id")?;
        current_browser_session_for_agent(sessions_dir, session_id, agent_id, &browser_session_id)?;
        return Ok(browser_session_id);
    }

    let mut sessions = RuntimeStore::for_sessions_dir(sessions_dir)
        .list_browser_sessions(Some(session_id), Some(agent_id))
        .map_err(OrchestratorError::ToolError)?;
    sessions.sort_by_key(|record| std::cmp::Reverse(record.updated_at_us));
    sessions
        .into_iter()
        .next()
        .map(|record| record.browser_session_id)
        .ok_or_else(|| OrchestratorError::ToolError("no active browser session found".into()))
}

fn resolve_domain_access_decision(
    sessions_dir: Option<&Path>,
    profile: &AgentCapabilityProfile,
    domain: &str,
    action_family: aria_core::WebActionFamily,
    session_id: Option<aria_core::Uuid>,
) -> Result<Option<aria_core::DomainDecisionKind>, OrchestratorError> {
    let Some(sessions_dir) = sessions_dir else {
        return Ok(None);
    };
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    let mut decisions = store
        .list_domain_access_decisions(Some(domain), Some(&profile.agent_id))
        .map_err(OrchestratorError::ToolError)?;
    decisions.sort_by_key(|decision| std::cmp::Reverse(decision.created_at_us));

    for decision in decisions {
        if decision.action_family != action_family {
            continue;
        }
        if let Some(expires_at_us) = decision.expires_at_us {
            if expires_at_us <= now_us {
                continue;
            }
        }
        if let Some(bound_session_id) = decision.session_id {
            if Some(bound_session_id) != session_id {
                continue;
            }
        }
        match decision.decision {
            aria_core::DomainDecisionKind::AllowOnce | aria_core::DomainDecisionKind::DenyOnce => {
                store
                    .delete_domain_access_decision(&decision.decision_id)
                    .map_err(OrchestratorError::ToolError)?;
                return Ok(Some(decision.decision));
            }
            aria_core::DomainDecisionKind::AllowForSession => {
                if decision.session_id.is_some() && decision.session_id == session_id {
                    return Ok(Some(decision.decision));
                }
            }
            aria_core::DomainDecisionKind::AllowAlways
            | aria_core::DomainDecisionKind::DenyAlways => return Ok(Some(decision.decision)),
        }
    }

    Ok(None)
}

fn validate_web_request(
    capability_profile: Option<&AgentCapabilityProfile>,
    call: &ToolCall,
    sessions_dir: Option<&Path>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    let Some((domain, action_family)) = extract_web_target(call)? else {
        return Ok(());
    };
    let allow_private = profile_allows_private_network_targets(capability_profile);
    if let Some((url, _)) = match call.name.as_str() {
        "fetch_url" | "web_fetch" | "web_extract" => Some((decode_tool_args::<UrlRequest>(call)?.url, true)),
        "browser_open" => Some((decode_tool_args::<BrowserOpenRequest>(call)?.url, true)),
        "browser_snapshot" | "browser_screenshot" | "browser_extract" => {
            Some((decode_tool_args::<BrowserArtifactRequest>(call)?.url, true))
        }
        "browser_download" => Some((decode_tool_args::<BrowserDownloadRequest>(call)?.url, true)),
        "crawl_page" | "crawl_site" => Some((decode_tool_args::<CrawlRequest>(call)?.url, true)),
        _ => None,
    } {
        validate_web_url_target_syntactic(&url, allow_private)?;
    }
    let Some(profile) = capability_profile else {
        return Ok(());
    };

    if profile
        .web_domain_blocklist
        .iter()
        .any(|rule| domain_matches_rule(&domain, rule))
    {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::DomainPolicy,
            domain.clone(),
            format!(
                "web access to domain '{}' is blocked for agent '{}'",
                domain, profile.agent_id
            ),
        );
        return Err(OrchestratorError::ToolError(format!(
            "web access to domain '{}' is blocked for agent '{}'",
            domain, profile.agent_id
        )));
    }

    if profile
        .web_domain_allowlist
        .iter()
        .any(|rule| domain_matches_rule(&domain, rule))
    {
        return Ok(());
    }

    if let Some(decision) =
        resolve_domain_access_decision(sessions_dir, profile, &domain, action_family, session_id)?
    {
        match decision {
            aria_core::DomainDecisionKind::AllowOnce
            | aria_core::DomainDecisionKind::AllowForSession
            | aria_core::DomainDecisionKind::AllowAlways => return Ok(()),
            aria_core::DomainDecisionKind::DenyOnce | aria_core::DomainDecisionKind::DenyAlways => {
                append_scope_denial_record(
                    sessions_dir,
                    &profile.agent_id,
                    session_id,
                    ScopeDenialKind::DomainPolicy,
                    domain.clone(),
                    format!(
                        "web access to domain '{}' is denied by stored policy for agent '{}'",
                        domain, profile.agent_id
                    ),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "web access to domain '{}' is denied by stored policy for agent '{}'",
                    domain, profile.agent_id
                )));
            }
        }
    }

    if matches!(
        profile.web_approval_policy,
        Some(aria_core::WebApprovalPolicy::PromptOnUnknownDomain)
            | Some(aria_core::WebApprovalPolicy::RequireApprovalAlways)
    ) {
        return Err(aria_intelligence::approval_required_error(&call.name));
    }

    Ok(())
}

fn build_policy_eval_context(
    principal: &str,
    channel: GatewayChannel,
    capability_profile: Option<&AgentCapabilityProfile>,
    whitelist: &[String],
    forbid: &[String],
) -> aria_policy::EvalContext {
    let mut whitelist = whitelist.to_vec();
    for prefix in [
        "domain/",
        "browser_profile/",
        "browser_action/",
        "crawl_scope/",
    ] {
        if !whitelist.iter().any(|existing| existing == prefix) {
            whitelist.push(prefix.to_string());
        }
    }
    aria_policy::EvalContext {
        channel: format!("{:?}", channel),
        blast_radius: capability_blast_radius(capability_profile),
        prompt_origin: principal.to_string(),
        whitelist,
        forbid: forbid.to_vec(),
    }
}

fn evaluate_cedar_decision(
    cedar: &aria_policy::CedarEvaluator,
    principal: &str,
    action: &str,
    resource: &str,
    resource_path: &str,
    ctx: &aria_policy::EvalContext,
    tool_name: &str,
) -> Result<(), OrchestratorError> {
    let decision = cedar
        .evaluate_with_context_and_path_tristate(
            principal,
            action,
            resource,
            resource_path,
            ctx,
            SENSITIVE_TOOL_ACTIONS,
        )
        .map_err(|e| OrchestratorError::ToolError(format!("policy evaluation failed: {}", e)))?;
    match decision {
        aria_policy::Decision::Allow => Ok(()),
        aria_policy::Decision::Deny => Err(OrchestratorError::ToolError(format!(
            "policy denied action '{}' on resource '{}'",
            action, resource
        ))),
        aria_policy::Decision::AskUser => Err(aria_intelligence::approval_required_error(tool_name)),
    }
}

fn validate_cedar_web_platform_request(
    cedar: &aria_policy::CedarEvaluator,
    principal: &str,
    channel: GatewayChannel,
    capability_profile: Option<&AgentCapabilityProfile>,
    call: &ToolCall,
    whitelist: &[String],
    forbid: &[String],
) -> Result<(), OrchestratorError> {
    let ctx = build_policy_eval_context(principal, channel, capability_profile, whitelist, forbid);
    if let Some((domain, family)) = extract_web_target(call)? {
        let action = match family {
            aria_core::WebActionFamily::Fetch => "web_domain_fetch",
            aria_core::WebActionFamily::Crawl => "web_domain_crawl",
            aria_core::WebActionFamily::Screenshot => "web_domain_screenshot",
            aria_core::WebActionFamily::InteractiveRead => "web_domain_interactive_read",
            aria_core::WebActionFamily::InteractiveWrite => "web_domain_interactive_write",
            aria_core::WebActionFamily::Login => "web_domain_login",
            aria_core::WebActionFamily::Download => "web_domain_download",
        };
        evaluate_cedar_decision(
            cedar,
            principal,
            action,
            &format!("web_domain_{}", domain.replace(['.', '-'], "_")),
            &format!("domain/{}", domain),
            &ctx,
            &call.name,
        )?;
    }
    if let Some(profile_id) = extract_browser_profile_target(call)? {
        evaluate_cedar_decision(
            cedar,
            principal,
            "browser_profile_access",
            &format!("browser_profile_{}", profile_id.replace(['.', '-'], "_")),
            &format!("browser_profile/{}", profile_id),
            &ctx,
            &call.name,
        )?;
    }
    if let Some(request) = extract_browser_action_request(call)? {
        evaluate_cedar_decision(
            cedar,
            principal,
            "browser_action_access",
            &format!("browser_action_{:?}", request.action).to_ascii_lowercase(),
            &format!("browser_action/{:?}", request.action).to_ascii_lowercase(),
            &ctx,
            &call.name,
        )?;
    }
    if matches!(call.name.as_str(), "crawl_page" | "crawl_site") {
        let request: CrawlRequest = decode_tool_args(call)?;
        let scope = if call.name == "crawl_page" {
            aria_core::CrawlScope::SinglePage
        } else {
            request.scope.unwrap_or(aria_core::CrawlScope::SameOrigin)
        };
        evaluate_cedar_decision(
            cedar,
            principal,
            "crawl_scope_access",
            &format!("crawl_scope_{:?}", scope).to_ascii_lowercase(),
            &format!("crawl_scope/{:?}", scope).to_ascii_lowercase(),
            &ctx,
            &call.name,
        )?;
    }
    Ok(())
}

fn tool_returns_web_content(call: &ToolCall) -> bool {
    matches!(
        call.name.as_str(),
        "fetch_url" | "web_fetch" | "web_extract" | "browser_extract"
    )
        || call.name.starts_with("browser_")
        || call.name.starts_with("crawl_")
        || call.name.starts_with("watch_")
}


fn scan_web_tool_result(
    firewall: &aria_safety::DfaFirewall,
    call: &ToolCall,
    result: &ToolExecutionResult,
    sessions_dir: Option<&Path>,
    profile: Option<&AgentCapabilityProfile>,
    session_id: Option<aria_core::Uuid>,
) -> Result<(), OrchestratorError> {
    if !tool_returns_web_content(call) {
        return Ok(());
    }

    let summary_scan = firewall.scan_egress(result.render_for_prompt());
    let payload_text = serde_json::to_string(&result.as_provider_payload())
        .unwrap_or_else(|_| String::from("\"<unserializable web payload>\""));
    let payload_scan = firewall.scan_egress(&payload_text);

    let mut alerts = Vec::new();
    if let aria_safety::ScanResult::Alert(found) = summary_scan {
        alerts.extend(found);
    }
    if let aria_safety::ScanResult::Alert(found) = payload_scan {
        for item in found {
            if !alerts.contains(&item) {
                alerts.push(item);
            }
        }
    }
    if alerts.is_empty() {
        return Ok(());
    }

    if let Some(profile) = profile {
        append_scope_denial_record(
            sessions_dir,
            &profile.agent_id,
            session_id,
            ScopeDenialKind::ContentFirewall,
            call.name.clone(),
            format!(
                "web tool output blocked by firewall for '{}'; matched patterns: {}",
                call.name,
                alerts.join(", ")
            ),
        );
    }
    Err(OrchestratorError::ToolError(format!(
        "web tool output blocked by firewall for '{}'",
        call.name
    )))
}

#[cfg(feature = "mcp-runtime")]
fn ensure_mcp_server_exists(
    store: &RuntimeStore,
    server_id: &str,
) -> Result<(), OrchestratorError> {
    let server = store
        .list_mcp_servers()
        .map_err(OrchestratorError::ToolError)?
        .into_iter()
        .find(|server| server.server_id == server_id);
    let Some(server) = server else {
        return Err(OrchestratorError::ToolError(format!(
            "unknown MCP server '{}'",
            server_id
        )));
    };
    if !server.enabled {
        return Err(OrchestratorError::ToolError(format!(
            "disabled MCP server '{}'",
            server_id
        )));
    }
    Ok(())
}

#[cfg(feature = "mcp-runtime")]
fn mcp_client_pool(
) -> &'static std::sync::Mutex<
    std::collections::HashMap<
        String,
        std::sync::Arc<tokio::sync::Mutex<McpClient<TransportSelector>>>,
    >,
> {
    static POOL: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<
                String,
                std::sync::Arc<tokio::sync::Mutex<McpClient<TransportSelector>>>,
            >,
        >,
    > = std::sync::OnceLock::new();
    POOL.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(feature = "mcp-runtime")]
fn load_mcp_registry_from_store(store: &RuntimeStore) -> Result<McpRegistry, String> {
    let mut registry = McpRegistry::new();
    for server in store.list_mcp_servers()? {
        let server_id = server.server_id.clone();
        registry.register_server(server);
        for tool in store.list_mcp_imported_tools(&server_id)? {
            registry
                .import_tool(tool)
                .map_err(|e| format!("load MCP tool import failed: {}", e))?;
        }
        for prompt in store.list_mcp_imported_prompts(&server_id)? {
            registry
                .import_prompt(prompt)
                .map_err(|e| format!("load MCP prompt import failed: {}", e))?;
        }
        for resource in store.list_mcp_imported_resources(&server_id)? {
            registry
                .import_resource(resource)
                .map_err(|e| format!("load MCP resource import failed: {}", e))?;
        }
    }
    Ok(registry)
}

#[cfg(feature = "mcp-runtime")]
fn build_mcp_client(store: &RuntimeStore) -> Result<McpClient<TransportSelector>, String> {
    Ok(McpClient::new(
        load_mcp_registry_from_store(store)?,
        TransportSelector::default(),
    ))
}

#[cfg(feature = "mcp-runtime")]
fn pooled_mcp_client(
    store: &RuntimeStore,
) -> Result<std::sync::Arc<tokio::sync::Mutex<McpClient<TransportSelector>>>, String> {
    let key = store.cache_key();
    let mut pool = mcp_client_pool()
        .lock()
        .map_err(|_| "mcp client pool poisoned".to_string())?;
    if let Some(client) = pool.get(&key) {
        return Ok(client.clone());
    }
    let client = std::sync::Arc::new(tokio::sync::Mutex::new(build_mcp_client(store)?));
    pool.insert(key, client.clone());
    Ok(client)
}

#[cfg(feature = "mcp-runtime")]
fn parse_mcp_primitive_kind(value: &str) -> Result<McpPrimitiveKind, OrchestratorError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "tool" => Ok(McpPrimitiveKind::Tool),
        "prompt" => Ok(McpPrimitiveKind::Prompt),
        "resource" => Ok(McpPrimitiveKind::Resource),
        other => Err(OrchestratorError::ToolError(format!(
            "unknown primitive_kind '{}'",
            other
        ))),
    }
}

#[cfg(feature = "mcp-runtime")]
fn ensure_mcp_binding_target_exists(
    store: &RuntimeStore,
    server_id: &str,
    primitive_kind: McpPrimitiveKind,
    target_name: &str,
) -> Result<(), OrchestratorError> {
    let exists = match primitive_kind {
        McpPrimitiveKind::Tool => store
            .list_mcp_imported_tools(server_id)
            .map_err(OrchestratorError::ToolError)?
            .into_iter()
            .any(|entry| entry.tool_name == target_name),
        McpPrimitiveKind::Prompt => store
            .list_mcp_imported_prompts(server_id)
            .map_err(OrchestratorError::ToolError)?
            .into_iter()
            .any(|entry| entry.prompt_name == target_name),
        McpPrimitiveKind::Resource => store
            .list_mcp_imported_resources(server_id)
            .map_err(OrchestratorError::ToolError)?
            .into_iter()
            .any(|entry| entry.resource_uri == target_name),
    };
    if !exists {
        return Err(OrchestratorError::ToolError(format!(
            "unknown MCP {:?} target '{}::{}'",
            primitive_kind, server_id, target_name
        )));
    }
    Ok(())
}

#[cfg(feature = "mcp-runtime")]
fn mcp_binding_exists(
    store: &RuntimeStore,
    agent_id: &str,
    server_id: &str,
    primitive_kind: McpPrimitiveKind,
    target_name: &str,
) -> Result<bool, String> {
    Ok(store
        .list_mcp_bindings_for_agent(agent_id)?
        .into_iter()
        .any(|binding| {
            binding.server_id == server_id
                && binding.primitive_kind == primitive_kind
                && binding.target_name == target_name
        }))
}

#[cfg(feature = "mcp-runtime")]
fn refresh_mcp_import_cache(
    store: &RuntimeStore,
    server_id: &str,
    refreshed_at_us: u64,
) -> Result<(), String> {
    let server = store
        .list_mcp_servers()?
        .into_iter()
        .find(|server| server.server_id == server_id)
        .ok_or_else(|| format!("unknown MCP server '{}'", server_id))?;
    let record = McpImportCacheRecord {
        server_id: server.server_id.clone(),
        transport: server.transport,
        tool_count: store.list_mcp_imported_tools(server_id)?.len() as u32,
        prompt_count: store.list_mcp_imported_prompts(server_id)?.len() as u32,
        resource_count: store.list_mcp_imported_resources(server_id)?.len() as u32,
        refreshed_at_us,
    };
    store.upsert_mcp_import_cache_record(&record)
}

#[cfg(feature = "mcp-runtime")]
fn native_mcp_profile(
    agent_id: String,
    server_id: &str,
    tool_name: Option<&str>,
    prompt_name: Option<&str>,
    resource_uri: Option<&str>,
) -> AgentCapabilityProfile {
    AgentCapabilityProfile {
        agent_id,
        class: aria_core::AgentClass::Generalist,
        tool_allowlist: vec![],
        skill_allowlist: vec![],
        mcp_server_allowlist: vec![server_id.to_string()],
        mcp_tool_allowlist: tool_name.into_iter().map(|v| v.to_string()).collect(),
        mcp_prompt_allowlist: prompt_name.into_iter().map(|v| v.to_string()).collect(),
        mcp_resource_allowlist: resource_uri.into_iter().map(|v| v.to_string()).collect(),
        filesystem_scopes: vec![],
        retrieval_scopes: vec![],
        delegation_scope: None,
        web_domain_allowlist: vec![],
        web_domain_blocklist: vec![],
        browser_profile_allowlist: vec![],
        browser_action_scope: None,
        browser_session_scope: None,
        crawl_scope: None,
        web_approval_policy: None,
        web_transport_allowlist: vec![],
        requires_elevation: false,
        side_effect_level: aria_core::SideEffectLevel::ReadOnly,
        trust_profile: None,
    }
}

fn parse_skill_activation_policy(value: &str) -> Result<SkillActivationPolicy, OrchestratorError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "manual" => Ok(SkillActivationPolicy::Manual),
        "auto_suggest" | "autosuggest" => Ok(SkillActivationPolicy::AutoSuggest),
        "auto_load_low_risk" | "autoloadlowrisk" | "auto_load" => {
            Ok(SkillActivationPolicy::AutoLoadLowRisk)
        }
        "approval_required" | "approvalrequired" => Ok(SkillActivationPolicy::ApprovalRequired),
        other => Err(OrchestratorError::ToolError(format!(
            "unknown activation_policy '{}'",
            other
        ))),
    }
}

fn parse_semver_triplet(value: &str) -> Option<(u64, u64, u64)> {
    let cleaned = value.trim().trim_start_matches('v');
    let mut parts = cleaned.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.parse::<u64>().ok()?;
    Some((major, minor, patch))
}

fn version_satisfies_requirement(installed: &str, requirement: &str) -> bool {
    let Some((maj, min, pat)) = parse_semver_triplet(installed) else {
        return false;
    };
    let req = requirement.trim();
    if let Some(stripped) = req.strip_prefix("^") {
        if let Some((rmaj, rmin, rpat)) = parse_semver_triplet(stripped) {
            return maj == rmaj && (min > rmin || (min == rmin && pat >= rpat));
        }
        return false;
    }
    if let Some(stripped) = req.strip_prefix(">=") {
        if let Some((rmaj, rmin, rpat)) = parse_semver_triplet(stripped) {
            return (maj, min, pat) >= (rmaj, rmin, rpat);
        }
        return false;
    }
    if let Some((rmaj, rmin, rpat)) = parse_semver_triplet(req) {
        return (maj, min, pat) == (rmaj, rmin, rpat);
    }
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkillManifestSignature {
    algorithm: String,
    skill_id: String,
    version: String,
    payload_sha256_hex: String,
    public_key_hex: String,
    signature_hex: String,
}

fn parse_signing_key_hex(value: &str) -> Result<SigningKey, OrchestratorError> {
    let key_bytes = hex::decode(value.trim())
        .map_err(|e| OrchestratorError::ToolError(format!("invalid signing_key_hex: {}", e)))?;
    let key_bytes: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| OrchestratorError::ToolError("invalid signing_key_hex length".into()))?;
    Ok(SigningKey::from_bytes(&key_bytes))
}

fn sign_skill_manifest_bytes(
    manifest: &SkillPackageManifest,
    manifest_bytes: &[u8],
    signing_key: &SigningKey,
) -> SkillManifestSignature {
    let payload_sha256_hex = hex::encode(Sha256::digest(manifest_bytes));
    let signature = signing_key.sign(manifest_bytes);
    SkillManifestSignature {
        algorithm: "ed25519-sha256".into(),
        skill_id: manifest.skill_id.clone(),
        version: manifest.version.clone(),
        payload_sha256_hex,
        public_key_hex: hex::encode(signing_key.verifying_key().to_bytes()),
        signature_hex: hex::encode(signature.to_bytes()),
    }
}

fn verify_signed_skill_manifest(
    manifest_bytes: &[u8],
    signature: &SkillManifestSignature,
    expected_public_key_hex: Option<&str>,
) -> Result<(), OrchestratorError> {
    if signature.algorithm.trim() != "ed25519-sha256" {
        return Err(OrchestratorError::ToolError(format!(
            "unsupported signature algorithm '{}'",
            signature.algorithm
        )));
    }
    let actual_hash = hex::encode(Sha256::digest(manifest_bytes));
    if actual_hash != signature.payload_sha256_hex.to_ascii_lowercase() {
        return Err(OrchestratorError::ToolError(
            "signed manifest hash does not match skill.toml".into(),
        ));
    }
    if let Some(expected) = expected_public_key_hex {
        if expected.trim().to_ascii_lowercase() != signature.public_key_hex.to_ascii_lowercase() {
            return Err(OrchestratorError::ToolError(
                "signed manifest public key does not match expected_public_key_hex".into(),
            ));
        }
    }
    let public_key_bytes = hex::decode(signature.public_key_hex.trim()).map_err(|e| {
        OrchestratorError::ToolError(format!("invalid signature public_key_hex: {}", e))
    })?;
    let public_key_bytes: [u8; 32] = public_key_bytes.try_into().map_err(|_| {
        OrchestratorError::ToolError("invalid signature public_key_hex length".into())
    })?;
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).map_err(|e| {
        OrchestratorError::ToolError(format!("invalid signature public key bytes: {}", e))
    })?;
    let signature_bytes = hex::decode(signature.signature_hex.trim())
        .map_err(|e| OrchestratorError::ToolError(format!("invalid signature_hex: {}", e)))?;
    let sig = Signature::from_slice(&signature_bytes)
        .map_err(|e| OrchestratorError::ToolError(format!("invalid signature bytes: {}", e)))?;
    verifying_key
        .verify(manifest_bytes, &sig)
        .map_err(|_| OrchestratorError::ToolError("signed manifest verification failed".into()))
}

const SENSITIVE_TOOL_ACTIONS: &[&str] = &[
    "run_shell",
    "write_file",
    "set_domain_access_decision",
    "grant_access",
    "manage_prompts",
    "manage_cron",
    "browser_download",
    "browser_session_persist_state",
    "browser_session_restore_state",
    "browser_login_begin_manual",
    "browser_login_complete_manual",
    "browser_login_fill_credentials",
];

pub(crate) fn runtime_exposes_base_tool(tool_name: &str) -> bool {
    !matches!(tool_name, "search_web" | "summarise_doc" | "query_rag")
}

fn idempotency_lookup(key: &str) -> Option<ToolExecutionResult> {
    app_runtime().idempotency_results.get(key)
}

fn idempotency_store_result(key: String, value: ToolExecutionResult) {
    app_runtime().idempotency_results.insert(key, value);
}

fn request_media_type_label(content: &MessageContent) -> Option<&'static str> {
    match content {
        MessageContent::Image { .. } => Some("image"),
        MessageContent::Audio { .. } => Some("audio"),
        MessageContent::Video { .. } => Some("video"),
        MessageContent::Document { .. } => Some("document"),
        MessageContent::Location { .. } => Some("location"),
        MessageContent::Text(_) => None,
    }
}

struct SessionToolCacheStore {
    max_entries: usize,
    entries: dashmap::DashMap<([u8; 16], String), Arc<tokio::sync::Mutex<DynamicToolCache>>>,
    lru: std::sync::Mutex<VecDeque<([u8; 16], String)>>,
}

impl SessionToolCacheStore {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            entries: dashmap::DashMap::new(),
            lru: std::sync::Mutex::new(VecDeque::new()),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    fn get(
        &self,
        key: &([u8; 16], String),
    ) -> Option<Arc<tokio::sync::Mutex<DynamicToolCache>>> {
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    fn get_or_insert_with<F>(
        &self,
        key: ([u8; 16], String),
        factory: F,
    ) -> Arc<tokio::sync::Mutex<DynamicToolCache>>
    where
        F: FnOnce() -> DynamicToolCache,
    {
        let mut lru = self
            .lru
            .lock()
            .expect("session tool cache lru lock poisoned");

        if !self.entries.contains_key(&key) {
            while self.entries.len() >= self.max_entries {
                let Some(oldest) = lru.pop_front() else {
                    break;
                };
                if self.entries.remove(&oldest).is_some() {
                    break;
                }
            }
            self.entries
                .insert(key.clone(), Arc::new(tokio::sync::Mutex::new(factory())));
        }
        Self::touch_locked(&mut lru, &key);
        drop(lru);
        self.entries
            .get(&key)
            .map(|entry| entry.value().clone())
            .expect("session tool cache entry inserted")
    }

    fn touch_locked(lru: &mut VecDeque<([u8; 16], String)>, key: &([u8; 16], String)) {
        lru.retain(|candidate| candidate != key);
        lru.push_back(key.clone());
    }
}

fn request_needs_planning(
    request_text: &str,
    scheduling_intent: Option<&SchedulingIntent>,
) -> bool {
    if scheduling_intent.is_some() {
        return true;
    }
    let lower = request_text.to_ascii_lowercase();
    ["plan", "steps", "strategy", "approach", "design"]
        .iter()
        .any(|needle| lower.contains(needle))
}

fn build_scenario_prompt_context(
    req: &AgentRequest,
    request_text: &str,
    trust_profile: Option<aria_core::TrustProfile>,
    scheduling_intent: Option<&SchedulingIntent>,
    available_tools: &[CachedTool],
) -> String {
    let mut blocks = Vec::new();

    if request_needs_planning(request_text, scheduling_intent) {
        let tools_summary = if available_tools.is_empty() {
            "No tools available".to_string()
        } else {
            available_tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        blocks.push(PromptManager::build_planning_prompt(
            request_text,
            "Ask the user to clarify ambiguous side effects instead of assuming.",
            &tools_summary,
        ));
    }

    if let Some(media_type) = request_media_type_label(&req.content) {
        blocks.push(PromptManager::build_media_prompt(media_type, request_text));
    }

    if matches!(
        trust_profile,
        Some(aria_core::TrustProfile::RoboticsControl)
    ) {
        blocks.push(PromptManager::build_robotics_prompt(
            request_text,
            "state=unknown; require explicit robot state snapshot before actuation",
            "max_abs_velocity=0.2; degraded_local_mode blocks motion; never emit direct actuator commands from the LLM",
        ));
    }

    if blocks.is_empty() {
        String::new()
    } else {
        format!("\n{}", blocks.join("\n\n"))
    }
}

fn active_prompt_mode_labels(
    req: &AgentRequest,
    request_text: &str,
    trust_profile: Option<aria_core::TrustProfile>,
    scheduling_intent: Option<&SchedulingIntent>,
) -> Vec<&'static str> {
    let mut modes = Vec::new();

    if scheduling_intent.is_some() {
        modes.push("scheduling");
    }
    if request_needs_planning(request_text, scheduling_intent) {
        modes.push("planning");
    }
    if request_media_type_label(&req.content).is_some() {
        modes.push("media");
    }
    if matches!(
        trust_profile,
        Some(aria_core::TrustProfile::RoboticsControl)
    ) {
        modes.push("robotics");
    }
    if modes.is_empty() {
        modes.push("execution");
    }
    modes
}

fn learning_prompt_mode_label(
    req: &AgentRequest,
    request_text: &str,
    trust_profile: Option<aria_core::TrustProfile>,
    scheduling_intent: Option<&SchedulingIntent>,
) -> String {
    active_prompt_mode_labels(req, request_text, trust_profile, scheduling_intent).join("+")
}

fn infer_rag_corpora_labels(rag_context: &str) -> Vec<String> {
    let mut corpora = Vec::new();
    for (needle, label) in [
        ("Session Context:", "session"),
        ("Workspace Context:", "workspace"),
        ("Policy/Runtime Context:", "policy_runtime"),
        ("External Context:", "external"),
        ("Social Context:", "social"),
        ("PageIndex Context:", "page_index"),
    ] {
        if rag_context.contains(needle) {
            corpora.push(label.to_string());
        }
    }
    corpora
}

fn truncate_trace_text(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    let mut out = String::new();
    for ch in trimmed.chars().take(limit) {
        out.push(ch);
    }
    out
}

fn estimate_token_count(text: &str) -> usize {
    text.split_whitespace().count()
}

fn truncate_to_token_budget(text: &str, token_budget: usize) -> String {
    if token_budget == 0 || text.trim().is_empty() {
        return String::new();
    }
    let mut out = Vec::new();
    let mut count = 0usize;
    for token in text.split_whitespace() {
        if count >= token_budget {
            break;
        }
        out.push(token);
        count += 1;
    }
    let mut joined = out.join(" ");
    if count < estimate_token_count(text) {
        joined.push_str(" [truncated]");
    }
    joined
}

#[derive(Debug, Clone, Copy)]
struct PromptBudget {
    history_tokens: usize,
    rag_tokens: usize,
    control_tokens: usize,
    tool_count: usize,
}

impl Default for PromptBudget {
    fn default() -> Self {
        Self {
            history_tokens: 900,
            rag_tokens: 1200,
            control_tokens: 500,
            tool_count: 8,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RetrievalBuildMetrics {
    session_hits: u32,
    workspace_hits: u32,
    policy_hits: u32,
    external_hits: u32,
    social_hits: u32,
    page_context_hits: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ControlDocumentConflict {
    kind: aria_core::ControlDocumentKind,
    paths: Vec<String>,
    diagnostic: String,
}

fn trace_outcome_for_result(result: &aria_intelligence::OrchestratorResult) -> TraceOutcome {
    match result {
        aria_intelligence::OrchestratorResult::Completed(_) => TraceOutcome::Succeeded,
        aria_intelligence::OrchestratorResult::AgentElevationRequired { .. }
        | aria_intelligence::OrchestratorResult::ToolApprovalRequired { .. } => {
            TraceOutcome::ApprovalRequired
        }
    }
}

fn trace_response_summary(result: &aria_intelligence::OrchestratorResult) -> String {
    match result {
        aria_intelligence::OrchestratorResult::Completed(text) => truncate_trace_text(text, 240),
        aria_intelligence::OrchestratorResult::AgentElevationRequired { message, .. } => {
            truncate_trace_text(message, 240)
        }
        aria_intelligence::OrchestratorResult::ToolApprovalRequired { call, .. } => {
            format!("approval required for tool {}", call.name)
        }
    }
}

fn should_audit_request_policy(policy: Option<&aria_core::ToolRuntimePolicy>) -> bool {
    match policy {
        Some(policy) => {
            !matches!(policy.tool_choice, aria_core::ToolChoicePolicy::Auto)
                || !policy.allow_parallel_tool_calls
        }
        None => false,
    }
}

fn append_request_policy_audit(
    sessions_dir: &Path,
    req: &AgentRequest,
    agent_id: Option<&str>,
) {
    let Some(policy) = req.tool_runtime_policy.as_ref() else {
        return;
    };
    if !should_audit_request_policy(Some(policy)) {
        return;
    }
    let record = RequestPolicyAuditRecord {
        audit_id: format!("reqpol-{}", uuid::Uuid::new_v4()),
        request_id: uuid::Uuid::from_bytes(req.request_id).to_string(),
        session_id: uuid::Uuid::from_bytes(req.session_id).to_string(),
        user_id: req.user_id.clone(),
        agent_id: agent_id.map(str::to_string),
        channel: format!("{:?}", req.channel),
        tool_runtime_policy: policy.clone(),
        created_at_us: req.timestamp_us,
    };
    let _ = RuntimeStore::for_sessions_dir(&sessions_dir).append_request_policy_audit(&record);
}

fn current_repair_fallback_allowlist() -> Vec<String> {
    app_runtime().repair_fallback_allowlist.clone()
}

fn repair_fallback_allowed(
    allowlist: &[String],
    profile: Option<&aria_core::ModelCapabilityProfile>,
) -> bool {
    let Some(profile) = profile else {
        return false;
    };
    let slash_ref = profile.model_ref.as_slash_ref();
    allowlist.iter().any(|entry| {
        entry == &slash_ref
            || entry == &profile.model_ref.model_id
            || entry == &format!("{}/{}", profile.model_ref.provider_id, profile.model_ref.model_id)
    })
}

#[derive(Debug, Clone)]
struct RepairFallbackAuditSink {
    sessions_dir: PathBuf,
    request_id: String,
    session_id: String,
    user_id: String,
    agent_id: String,
    provider_id: Option<String>,
    model_id: Option<String>,
    created_at_us: u64,
}

impl OrchestratorEventSink for RepairFallbackAuditSink {
    fn on_event(&self, event: &OrchestratorEvent) {
        match event {
            OrchestratorEvent::RepairFallbackUsed { tool_name, .. } => {
                let record = RepairFallbackAuditRecord {
                    audit_id: format!("repairfb-{}", uuid::Uuid::new_v4()),
                    request_id: self.request_id.clone(),
                    session_id: self.session_id.clone(),
                    user_id: self.user_id.clone(),
                    agent_id: self.agent_id.clone(),
                    provider_id: self.provider_id.clone(),
                    model_id: self.model_id.clone(),
                    tool_name: tool_name.clone(),
                    created_at_us: self.created_at_us,
                };
                let _ = RuntimeStore::for_sessions_dir(&self.sessions_dir)
                    .append_repair_fallback_audit(&record);
            }
            OrchestratorEvent::StreamingDecision {
                phase,
                mode,
                model_ref,
            } => {
                let record = StreamingDecisionAuditRecord {
                    audit_id: format!("streamdec-{}", uuid::Uuid::new_v4()),
                    request_id: self.request_id.clone(),
                    session_id: self.session_id.clone(),
                    user_id: self.user_id.clone(),
                    agent_id: self.agent_id.clone(),
                    phase: (*phase).to_string(),
                    mode: (*mode).to_string(),
                    model_ref: model_ref.clone(),
                    created_at_us: self.created_at_us,
                };
                let _ = RuntimeStore::for_sessions_dir(&self.sessions_dir)
                    .append_streaming_decision_audit(&record);
            }
        }
    }
}

fn should_sample_learning_record(request_id: [u8; 16], sampling_percent: u8) -> bool {
    if sampling_percent >= 100 {
        return true;
    }
    if sampling_percent == 0 {
        return false;
    }
    (u16::from(request_id[0]) % 100) < u16::from(sampling_percent)
}

fn redact_learning_token(token: &str) -> String {
    let trimmed = token.trim();
    if trimmed.contains('@') && trimmed.contains('.') {
        return "[redacted-email]".to_string();
    }
    if trimmed.starts_with("sk-")
        || trimmed.starts_with("ghp_")
        || trimmed.starts_with("xoxb-")
        || (trimmed.len() >= 24
            && trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'))
    {
        return "[redacted-secret]".to_string();
    }
    trimmed.to_string()
}

fn sanitize_learning_text(config: &LearningConfig, text: &str) -> String {
    if !config.redact_sensitive {
        return text.to_string();
    }
    text.split_whitespace()
        .map(redact_learning_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn persist_learning_trace(
    learning: &LearningConfig,
    sessions_dir: &Path,
    req: &AgentRequest,
    agent_id: &str,
    prompt_mode: &str,
    request_text: &str,
    tool_names: &[String],
    result: &aria_intelligence::OrchestratorResult,
    rag_context: &str,
    recorded_at_us: u64,
) {
    if !learning.enabled
        || !should_sample_learning_record(req.request_id, learning.sampling_percent)
    {
        return;
    }
    let trace = ExecutionTrace {
        request_id: uuid::Uuid::from_bytes(req.request_id).to_string(),
        session_id: uuid::Uuid::from_bytes(req.session_id).to_string(),
        user_id: req.user_id.clone(),
        agent_id: agent_id.to_string(),
        channel: req.channel,
        prompt_mode: prompt_mode.to_string(),
        task_fingerprint: TaskFingerprint::from_parts(
            agent_id,
            prompt_mode,
            request_text,
            &tool_names,
        ),
        user_input_summary: sanitize_learning_text(
            learning,
            &truncate_trace_text(request_text, 240),
        ),
        tool_names: tool_names.to_vec(),
        retrieved_corpora: infer_rag_corpora_labels(rag_context),
        outcome: trace_outcome_for_result(result),
        latency_ms: 0,
        response_summary: sanitize_learning_text(learning, &trace_response_summary(result)),
        tool_runtime_policy: req.tool_runtime_policy.clone(),
        recorded_at_us,
    };
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let _ = store.record_execution_trace(&trace);
    let _ = store.prune_learning_records(
        learning.max_trace_rows,
        learning.max_reward_rows,
        learning.max_derivative_rows,
    );
    let _ = store.synthesize_candidate_artifacts(recorded_at_us);
    let _ = store.compile_prompt_optimization_candidates(recorded_at_us, 2);
    let _ = store.compile_macro_candidates(recorded_at_us, 2);
    let _ = store.compile_wasm_candidates(recorded_at_us, 5);
    let _ = store.synthesize_selector_models(recorded_at_us);
}

fn build_learning_rollout_prompt_context(candidates: &[CandidateArtifactRecord]) -> String {
    if candidates.is_empty() {
        return String::new();
    }

    let mut blocks = Vec::new();
    for candidate in candidates {
        let payload = serde_json::from_str::<serde_json::Value>(&candidate.payload_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let example_inputs = payload
            .get("example_inputs")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str())
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
            .unwrap_or_default();
        let tools = payload
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        let block = match candidate.kind {
            CandidateArtifactKind::Prompt => format!(
                "Promoted prompt strategy: {}. Example requests: {}. Use this as guidance only; do not bypass policy or approvals.",
                candidate.summary, example_inputs
            ),
            CandidateArtifactKind::Macro => format!(
                "Promoted workflow macro: {}. Prefer tool sequence [{}] when it fits the request. This is guidance only; do not bypass policy or approvals.",
                candidate.summary, tools
            ),
            CandidateArtifactKind::Wasm => format!(
                "Promoted Wasm candidate metadata present for {}. Do not activate directly from prompt guidance.",
                candidate.title
            ),
        };
        blocks.push(block);
    }

    format!(
        "\n<learning_rollouts>\n{}\n</learning_rollouts>\n",
        blocks.join("\n")
    )
}

fn apply_learning_macro_rollouts(
    cache: &mut DynamicToolCache,
    tool_registry: &ToolManifestStore,
    candidates: &[CandidateArtifactRecord],
) {
    for candidate in candidates {
        if !matches!(candidate.kind, CandidateArtifactKind::Macro) {
            continue;
        }
        let payload = match serde_json::from_str::<serde_json::Value>(&candidate.payload_json) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        let Some(tools) = payload.get("tools").and_then(|v| v.as_array()) else {
            continue;
        };
        for tool_name in tools.iter().filter_map(|item| item.as_str()) {
            if let Some(tool) = tool_registry.get_by_name(tool_name) {
                let _ = cache.insert(tool);
            }
        }
    }
}

fn apply_learning_selector_models(
    cache: &mut DynamicToolCache,
    tool_registry: &ToolManifestStore,
    models: &[SelectorModelRecord],
) {
    for model in models {
        if !matches!(model.kind, SelectorModelKind::ToolRanker) {
            continue;
        }
        let payload = match serde_json::from_str::<serde_json::Value>(&model.payload_json) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        let Some(tools) = payload.get("tools").and_then(|v| v.as_array()) else {
            continue;
        };
        for tool_name in tools
            .iter()
            .filter_map(|item| item.get("name").and_then(|v| v.as_str()))
        {
            if let Some(tool) = tool_registry.get_by_name(tool_name) {
                let _ = cache.insert(tool);
            }
        }
    }
}

fn record_learning_reward(
    learning: &LearningConfig,
    sessions_dir: &Path,
    request_id: [u8; 16],
    session_id: [u8; 16],
    kind: RewardKind,
    notes: Option<String>,
    recorded_at_us: u64,
) {
    if !learning.enabled || !should_sample_learning_record(request_id, learning.sampling_percent) {
        return;
    }
    let request_id = uuid::Uuid::from_bytes(request_id).to_string();
    let reward = RewardEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        request_id: request_id.clone(),
        session_id: uuid::Uuid::from_bytes(session_id).to_string(),
        kind,
        value: match kind {
            RewardKind::Accepted => 1,
            RewardKind::Rejected => -1,
            RewardKind::Edited => -1,
            RewardKind::Retried => -1,
            RewardKind::OverrideApplied => 1,
        },
        notes: notes.map(|notes| sanitize_learning_text(learning, &notes)),
        recorded_at_us,
    };
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let _ = store.record_reward_event(&reward);
    let _ = store.prune_learning_records(
        learning.max_trace_rows,
        learning.max_reward_rows,
        learning.max_derivative_rows,
    );
    let _ = store.synthesize_candidate_artifacts(recorded_at_us);
    let _ = store.compile_prompt_optimization_candidates(recorded_at_us, 2);
    let _ = store.compile_macro_candidates(recorded_at_us, 2);
    let _ = store.compile_wasm_candidates(recorded_at_us, 5);
    let _ = store.synthesize_selector_models(recorded_at_us);
}

#[derive(Clone)]
struct RecordingToolExecutor<T> {
    inner: T,
    executed_tools: Arc<std::sync::Mutex<Vec<String>>>,
}

impl<T> RecordingToolExecutor<T> {
    fn new(inner: T, executed_tools: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        Self {
            inner,
            executed_tools,
        }
    }
}

#[async_trait::async_trait]
impl<T: ToolExecutor> ToolExecutor for RecordingToolExecutor<T> {
    async fn execute(&self, call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        let result = self.inner.execute(call).await;
        if result.is_ok() {
            let mut guard = self
                .executed_tools
                .lock()
                .expect("recording tool executor lock poisoned");
            guard.push(call.name.clone());
        }
        result
    }
}

fn maybe_record_retry_reward(
    learning: &LearningConfig,
    sessions_dir: &Path,
    req: &AgentRequest,
    agent_id: &str,
    prompt_mode: &str,
    request_text: &str,
    recorded_at_us: u64,
) {
    let fingerprint = TaskFingerprint::from_parts(agent_id, prompt_mode, request_text, &Vec::new());
    let session_id = uuid::Uuid::from_bytes(req.session_id).to_string();
    let request_id = uuid::Uuid::from_bytes(req.request_id).to_string();
    let prior = RuntimeStore::for_sessions_dir(&sessions_dir)
        .list_execution_traces_by_session_and_fingerprint(&session_id, &fingerprint.key)
        .unwrap_or_default();
    if prior.iter().any(|trace| trace.request_id != request_id) {
        record_learning_reward(
            learning,
            sessions_dir,
            req.request_id,
            req.session_id,
            RewardKind::Retried,
            Some(format!("repeated task fingerprint {}", fingerprint.key)),
            recorded_at_us,
        );
    }
}


#[async_trait::async_trait]
impl<T: ToolExecutor> ToolExecutor for PolicyCheckedExecutor<T> {
    async fn execute(&self, call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        if let Some(profile) = &self.capability_profile {
            if profile.requires_elevation {
                append_scope_denial_record(
                    self.sessions_dir.as_deref(),
                    &profile.agent_id,
                    self.session_id,
                    ScopeDenialKind::ElevationRequired,
                    call.name.clone(),
                    format!(
                        "tool '{}' not permitted for agent '{}' without elevation",
                        call.name, profile.agent_id
                    ),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "tool '{}' not permitted for agent '{}' without elevation",
                    call.name, profile.agent_id
                )));
            }
            if !profile.tool_allowlist.is_empty()
                && !profile.tool_allowlist.iter().any(|tool| tool == &call.name)
            {
                append_scope_denial_record(
                    self.sessions_dir.as_deref(),
                    &profile.agent_id,
                    self.session_id,
                    ScopeDenialKind::ToolAllowlist,
                    call.name.clone(),
                    format!(
                        "tool '{}' not permitted for agent '{}'",
                        call.name, profile.agent_id
                    ),
                );
                return Err(OrchestratorError::ToolError(format!(
                    "tool '{}' not permitted for agent '{}'",
                    call.name, profile.agent_id
                )));
            }
        }

        validate_spawn_agent_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_filesystem_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_execution_profile(
            self.capability_profile.as_ref(),
            self.channel,
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_network_egress_request(
            self.capability_profile.as_ref(),
            self.channel,
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_skill_activation_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
        )?;
        validate_mcp_tool_request(
            self.capability_profile.as_ref(),
            &self.principal,
            call,
            self.sessions_dir.as_deref(),
        )?;
        validate_mcp_prompt_request(
            self.capability_profile.as_ref(),
            &self.principal,
            call,
            self.sessions_dir.as_deref(),
        )?;
        validate_mcp_resource_request(
            self.capability_profile.as_ref(),
            &self.principal,
            call,
            self.sessions_dir.as_deref(),
        )?;
        validate_browser_profile_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_browser_action_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_web_request(
            self.capability_profile.as_ref(),
            call,
            self.sessions_dir.as_deref(),
            self.session_id,
        )?;
        validate_cedar_web_platform_request(
            &self.cedar,
            &self.principal,
            self.channel,
            self.capability_profile.as_ref(),
            call,
            &self.whitelist,
            &self.forbid,
        )?;

        let ast_call = Self::to_ast_call(call);
        debug!(
            tool = %call.name,
            principal = %self.principal,
            ast_call = %ast_call,
            "PolicyCheckedExecutor: evaluating"
        );
        let parsed = aria_policy::parse_ast_action(&ast_call)
            .map_err(|e| OrchestratorError::ToolError(format!("policy AST parse failed: {}", e)))?;
        let ctx = build_policy_eval_context(
            &self.principal,
            self.channel,
            self.capability_profile.as_ref(),
            &self.whitelist,
            &self.forbid,
        );

        let decision = self
            .cedar
            .evaluate_with_context_tristate(
                &self.principal,
                &parsed.action,
                &parsed.resource,
                &ctx,
                SENSITIVE_TOOL_ACTIONS,
            )
            .map_err(|e| {
                OrchestratorError::ToolError(format!("policy evaluation failed: {}", e))
            })?;
        if decision == aria_policy::Decision::Deny {
            debug!(
                tool = %call.name,
                action = %parsed.action,
                resource = %parsed.resource,
                "PolicyCheckedExecutor: DENIED"
            );
            return Err(OrchestratorError::ToolError(format!(
                "tool '{}' denied by policy for resource '{}'",
                parsed.action, parsed.resource
            )));
        }
        if decision == aria_policy::Decision::AskUser {
            return Err(aria_intelligence::approval_required_error(&call.name));
        }
        debug!(tool = %call.name, "PolicyCheckedExecutor: ALLOWED, delegating to executor");
        let result = self.inner.execute(call).await?;
        if let Some(firewall) = self.firewall.as_ref() {
            scan_web_tool_result(
                firewall,
                call,
                &result,
                self.sessions_dir.as_deref(),
                self.capability_profile.as_ref(),
                self.session_id,
            )?;
        }
        Ok(result)
    }
}

#[cfg(test)]
fn build_capability_profile(
    agent_id: &str,
    tool_allowlist: &[&str],
    requires_elevation: bool,
) -> AgentCapabilityProfile {
    AgentCapabilityProfile {
        agent_id: agent_id.to_string(),
        class: aria_core::AgentClass::Restricted,
        tool_allowlist: tool_allowlist
            .iter()
            .map(|tool| (*tool).to_string())
            .collect(),
        skill_allowlist: vec![],
        mcp_server_allowlist: vec![],
        mcp_tool_allowlist: vec![],
        mcp_prompt_allowlist: vec![],
        mcp_resource_allowlist: vec![],
        filesystem_scopes: vec![],
        retrieval_scopes: vec![],
        delegation_scope: None,
        web_domain_allowlist: vec![],
        web_domain_blocklist: vec![],
        browser_profile_allowlist: vec![],
        browser_action_scope: None,
        browser_session_scope: None,
        crawl_scope: None,
        web_approval_policy: None,
        web_transport_allowlist: vec![],
        requires_elevation,
        side_effect_level: if requires_elevation {
            aria_core::SideEffectLevel::Privileged
        } else {
            aria_core::SideEffectLevel::ReadOnly
        },
        trust_profile: None,
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestOkExecutor;

#[cfg(test)]
#[async_trait::async_trait]
impl ToolExecutor for TestOkExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        Ok(tool_text_result("ok"))
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestResultExecutor {
    result: ToolExecutionResult,
}

#[cfg(test)]
#[async_trait::async_trait]
impl ToolExecutor for TestResultExecutor {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolExecutionResult, OrchestratorError> {
        Ok(self.result.clone())
    }
}

#[cfg(test)]
async fn start_test_http_server(body: &'static str, content_type: &'static str) -> String {
    use axum::{routing::get, Router};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let app = Router::new().route(
        "/",
        get(move || async move {
            (
                [(axum::http::header::CONTENT_TYPE, content_type)],
                body.to_string(),
            )
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[cfg(test)]
async fn start_routed_test_http_server(
    routes: Vec<(&'static str, &'static str, &'static str)>,
) -> String {
    use axum::{extract::Path, routing::get, Router};
    let route_map: std::collections::HashMap<String, (&'static str, &'static str)> = routes
        .into_iter()
        .map(|(path, body, content_type)| (path.to_string(), (body, content_type)))
        .collect();
    let route_map = Arc::new(route_map);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind routed test server");
    let addr = listener.local_addr().expect("local addr");
    let route_map_root = route_map.clone();
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let route_map = route_map_root.clone();
                async move {
                    let (body, content_type) = route_map
                        .get("/")
                        .copied()
                        .unwrap_or(("<html><body>missing</body></html>", "text/html; charset=utf-8"));
                    (
                        [(axum::http::header::CONTENT_TYPE, content_type)],
                        body.to_string(),
                    )
                }
            }),
        )
        .route(
            "/*path",
            get(move |Path(path): Path<String>| {
                let route_map = route_map.clone();
                async move {
                    let key = format!("/{}", path);
                    let (body, content_type) = route_map
                        .get(&key)
                        .copied()
                        .unwrap_or(("<html><body>missing</body></html>", "text/html; charset=utf-8"));
                    (
                        [(axum::http::header::CONTENT_TYPE, content_type)],
                        body.to_string(),
                    )
                }
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[cfg(test)]
async fn start_retrying_test_http_server(
    failure_status: axum::http::StatusCode,
    failures_before_success: usize,
    success_body: &'static str,
    content_type: &'static str,
) -> String {
    use axum::{routing::get, Router};
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_for_route = attempts.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind retrying test server");
    let addr = listener.local_addr().expect("local addr");
    let app = Router::new().route(
        "/",
        get(move || {
            let attempts = attempts_for_route.clone();
            async move {
                let seen = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if seen < failures_before_success {
                    (
                        failure_status,
                        [(axum::http::header::RETRY_AFTER, "0")],
                        "retry later".to_string(),
                    )
                } else {
                    (
                        axum::http::StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, content_type)],
                        success_body.to_string(),
                    )
                }
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[cfg(test)]
fn browser_env_test_guard() -> std::sync::MutexGuard<'static, ()> {
    BROWSER_ENV_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
fn sha256_file_hex_for_test(path: &Path) -> String {
    format!(
        "{:x}",
        Sha256::digest(std::fs::read(path).expect("read test bridge"))
    )
}

#[cfg(test)]
fn set_test_browser_bridge_env(
    bridge_path: &Path,
) -> (
    Option<std::ffi::OsString>,
    Option<std::ffi::OsString>,
    Option<std::ffi::OsString>,
) {
    let original_bridge = std::env::var_os("ARIA_BROWSER_AUTOMATION_BIN");
    let original_allowlist = std::env::var_os("ARIA_BROWSER_AUTOMATION_SHA256_ALLOWLIST");
    let original_containment = std::env::var_os("ARIA_BROWSER_AUTOMATION_OS_CONTAINMENT");
    let wrapper_path = bridge_path
        .parent()
        .expect("bridge parent")
        .join(format!(
            "{}-wrapper.sh",
            bridge_path.file_stem().and_then(|stem| stem.to_str()).unwrap_or("bridge")
        ));
    let quoted_original = bridge_path.to_string_lossy().replace('\'', "'\"'\"'");
    std::fs::write(
        &wrapper_path,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--bridge-meta\" ]; then\nprintf '%s' '{{\"protocol_version\":1,\"bridge_version\":\"test\",\"supported_modes\":[\"argv_json\",\"stdin_json\"],\"supported_commands\":[\"browser_action\",\"persist_state\",\"restore_state\",\"fill_credentials\"]}}'\nexit 0\nfi\nexec '{}' \"$@\"\n",
            quoted_original
        ),
    )
    .expect("write bridge wrapper");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&wrapper_path)
            .expect("wrapper metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&wrapper_path, perms).expect("chmod wrapper");
    }
    let checksum = sha256_file_hex_for_test(&wrapper_path);
    unsafe {
        std::env::set_var("ARIA_BROWSER_AUTOMATION_BIN", &wrapper_path);
        std::env::set_var("ARIA_BROWSER_AUTOMATION_SHA256_ALLOWLIST", checksum);
        std::env::remove_var("ARIA_BROWSER_AUTOMATION_OS_CONTAINMENT");
    }
    (original_bridge, original_allowlist, original_containment)
}

#[cfg(test)]
fn restore_test_browser_bridge_env(
    original_bridge: Option<std::ffi::OsString>,
    original_allowlist: Option<std::ffi::OsString>,
    original_containment: Option<std::ffi::OsString>,
) {
    if let Some(value) = original_bridge {
        unsafe { std::env::set_var("ARIA_BROWSER_AUTOMATION_BIN", value) };
    } else {
        unsafe { std::env::remove_var("ARIA_BROWSER_AUTOMATION_BIN") };
    }
    if let Some(value) = original_allowlist {
        unsafe { std::env::set_var("ARIA_BROWSER_AUTOMATION_SHA256_ALLOWLIST", value) };
    } else {
        unsafe { std::env::remove_var("ARIA_BROWSER_AUTOMATION_SHA256_ALLOWLIST") };
    }
    if let Some(value) = original_containment {
        unsafe { std::env::set_var("ARIA_BROWSER_AUTOMATION_OS_CONTAINMENT", value) };
    } else {
        unsafe { std::env::remove_var("ARIA_BROWSER_AUTOMATION_OS_CONTAINMENT") };
    }
}

#[cfg(test)]
fn set_private_web_targets_env(enabled: bool) -> Option<std::ffi::OsString> {
    let original = std::env::var_os("ARIA_ALLOW_PRIVATE_WEB_TARGETS");
    if enabled {
        unsafe { std::env::set_var("ARIA_ALLOW_PRIVATE_WEB_TARGETS", "1") };
    } else {
        unsafe { std::env::remove_var("ARIA_ALLOW_PRIVATE_WEB_TARGETS") };
    }
    original
}

#[cfg(test)]
fn restore_private_web_targets_env(original: Option<std::ffi::OsString>) {
    if let Some(value) = original {
        unsafe { std::env::set_var("ARIA_ALLOW_PRIVATE_WEB_TARGETS", value) };
    } else {
        unsafe { std::env::remove_var("ARIA_ALLOW_PRIVATE_WEB_TARGETS") };
    }
}

#[cfg(test)]
fn set_web_storage_policy_env(
    values: &[(&str, &str)],
) -> std::collections::HashMap<String, Option<std::ffi::OsString>> {
    let mut originals = std::collections::HashMap::new();
    for (key, value) in values {
        originals.insert((*key).to_string(), std::env::var_os(key));
        unsafe { std::env::set_var(key, value) };
    }
    originals
}

#[cfg(test)]
fn restore_web_storage_policy_env(
    originals: std::collections::HashMap<String, Option<std::ffi::OsString>>,
) {
    for (key, value) in originals {
        if let Some(value) = value {
            unsafe { std::env::set_var(&key, value) };
        } else {
            unsafe { std::env::remove_var(&key) };
        }
    }
}

#[cfg(test)]
fn build_delegating_profile(
    agent_id: &str,
    tool_allowlist: &[&str],
    allowed_agents: &[&str],
    max_fanout: u16,
    max_runtime_seconds: u32,
) -> AgentCapabilityProfile {
    let mut profile = build_capability_profile(agent_id, tool_allowlist, false);
    profile.delegation_scope = Some(aria_core::DelegationScope {
        can_spawn_children: true,
        allowed_agents: allowed_agents
            .iter()
            .map(|agent| (*agent).to_string())
            .collect(),
        max_fanout,
        max_runtime_seconds,
    });
    profile
}

#[cfg(test)]
fn build_filesystem_profile(
    agent_id: &str,
    tool_allowlist: &[&str],
    root_path: &Path,
    allow_read: bool,
    allow_write: bool,
    allow_execute: bool,
) -> AgentCapabilityProfile {
    let mut profile = build_capability_profile(agent_id, tool_allowlist, false);
    profile.filesystem_scopes = vec![aria_core::FilesystemScope {
        root_path: root_path.to_string_lossy().to_string(),
        allow_read,
        allow_write,
        allow_execute,
    }];
    profile
}

fn capability_profile_from_agent_config(
    agent: &aria_intelligence::AgentConfig,
) -> Option<AgentCapabilityProfile> {
    if agent.tool_allowlist.is_empty()
        && agent.skill_allowlist.is_empty()
        && agent.mcp_server_allowlist.is_empty()
        && agent.mcp_tool_allowlist.is_empty()
        && agent.mcp_prompt_allowlist.is_empty()
        && agent.mcp_resource_allowlist.is_empty()
        && agent.filesystem_scopes.is_empty()
        && agent.retrieval_scopes.is_empty()
        && agent.delegation_scope.is_none()
        && agent.web_domain_allowlist.is_empty()
        && agent.web_domain_blocklist.is_empty()
        && agent.browser_profile_allowlist.is_empty()
        && agent.browser_action_scope.is_none()
        && agent.browser_session_scope.is_none()
        && agent.crawl_scope.is_none()
        && agent.web_approval_policy.is_none()
        && agent.web_transport_allowlist.is_empty()
        && !agent.requires_elevation
    {
        return None;
    }

    Some(AgentCapabilityProfile {
        agent_id: agent.id.clone(),
        class: agent.class,
        tool_allowlist: agent.tool_allowlist.clone(),
        skill_allowlist: agent.skill_allowlist.clone(),
        mcp_server_allowlist: agent.mcp_server_allowlist.clone(),
        mcp_tool_allowlist: agent.mcp_tool_allowlist.clone(),
        mcp_prompt_allowlist: agent.mcp_prompt_allowlist.clone(),
        mcp_resource_allowlist: agent.mcp_resource_allowlist.clone(),
        filesystem_scopes: agent.filesystem_scopes.clone(),
        retrieval_scopes: agent.retrieval_scopes.clone(),
        delegation_scope: agent.delegation_scope.clone(),
        web_domain_allowlist: agent.web_domain_allowlist.clone(),
        web_domain_blocklist: agent.web_domain_blocklist.clone(),
        browser_profile_allowlist: agent.browser_profile_allowlist.clone(),
        browser_action_scope: agent.browser_action_scope,
        browser_session_scope: agent.browser_session_scope,
        crawl_scope: agent.crawl_scope,
        web_approval_policy: agent.web_approval_policy,
        web_transport_allowlist: agent.web_transport_allowlist.clone(),
        requires_elevation: agent.requires_elevation,
        side_effect_level: agent.side_effect_level,
        trust_profile: agent.trust_profile.clone(),
    })
}

fn resolve_scheduled_agent_id(
    explicit_agent_id: Option<&str>,
    invoking_agent_id: Option<&str>,
    action_label: &str,
) -> Result<String, OrchestratorError> {
    if let Some(agent_id) = explicit_agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(agent_id.to_string());
    }
    if let Some(agent_id) = invoking_agent_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(agent_id.to_string());
    }
    Err(OrchestratorError::ToolError(format!(
        "Clarification required: specify which agent should handle {} using `agent_id`.",
        action_label
    )))
}

fn tool_text_result(text: impl Into<String>) -> ToolExecutionResult {
    ToolExecutionResult::text(text)
}

fn capability_blast_radius(profile: Option<&AgentCapabilityProfile>) -> u32 {
    match profile.map(|p| p.side_effect_level) {
        Some(aria_core::SideEffectLevel::ReadOnly) => 0,
        Some(aria_core::SideEffectLevel::ExternalFetch) => 1,
        Some(aria_core::SideEffectLevel::StatefulWrite) => 2,
        Some(aria_core::SideEffectLevel::Privileged) => 3,
        None => 1,
    }
}

fn capability_allows_external_network(profile: Option<&AgentCapabilityProfile>) -> bool {
    match profile {
        Some(p) => match p.trust_profile {
            Some(
                aria_core::TrustProfile::TrustedLocal
                | aria_core::TrustProfile::TrustedWorkspace
                | aria_core::TrustProfile::RoboticsControl,
            ) => false,
            _ => !matches!(p.side_effect_level, aria_core::SideEffectLevel::ReadOnly),
        },
        None => true,
    }
}

fn capability_allows_vault_egress(profile: Option<&AgentCapabilityProfile>) -> bool {
    match profile {
        Some(p) => {
            if matches!(
                p.trust_profile,
                Some(
                    aria_core::TrustProfile::UntrustedWeb
                        | aria_core::TrustProfile::UntrustedSocial
                        | aria_core::TrustProfile::TrustedLocal
                        | aria_core::TrustProfile::TrustedWorkspace
                        | aria_core::TrustProfile::RoboticsControl
                )
            ) {
                return false;
            }
            matches!(
                p.side_effect_level,
                aria_core::SideEffectLevel::StatefulWrite | aria_core::SideEffectLevel::Privileged
            )
        }
        None => true,
    }
}

fn build_dynamic_page_index(agent_store: &AgentConfigStore) -> PageIndexTree {
    let mut tree = PageIndexTree::new(32);
    let mut idx = 100u32;

    if agent_store.is_empty() {
        let _ = tree.insert(PageNode {
            node_id: "agent.developer".into(),
            title: "developer Agent".into(),
            summary: "Developer agent fallback".into(),
            start_index: 0,
            end_index: 1,
            children: vec![],
        });
        return tree;
    }

    for cfg in agent_store.all() {
        let node = PageNode {
            node_id: format!("agent.{}", cfg.id),
            title: format!("{} Agent", cfg.id),
            summary: cfg.description.clone(),
            start_index: idx,
            end_index: idx + 1,
            children: vec![],
        };
        let _ = tree.insert(node);
        idx += 1;
    }
    tree
}

fn local_embed(text: &str, dim: usize) -> Vec<f32> {
    let mut vec = vec![0.0_f32; dim];
    if dim == 0 {
        return vec;
    }
    for token in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.trim().is_empty())
    {
        let normalized = token.to_ascii_lowercase();
        let mut hash = 0_u64;
        for b in normalized.as_bytes() {
            hash = hash.wrapping_mul(16777619).wrapping_add(u64::from(*b));
        }
        let idx = (hash as usize) % dim;
        vec[idx] += 1.0;
    }
    let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for v in &mut vec {
            *v /= norm;
        }
    }
    vec
}

use std::future::Future;
use std::pin::Pin;

pub type AsyncHookFn = Box<
    dyn Fn(
            &AgentRequest,
            Arc<VectorStore>,
            Arc<PageIndexTree>,
        ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>>
        + Send
        + Sync,
>;

pub struct HookRegistry {
    pub message_pre: Vec<AsyncHookFn>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self {
            message_pre: Vec::new(),
        }
    }

    pub fn register_message_pre(&mut self, hook: AsyncHookFn) {
        self.message_pre.push(hook);
    }

    pub async fn execute_message_pre(
        &self,
        req: &AgentRequest,
        vector_store: &Arc<VectorStore>,
        page_index: &Arc<PageIndexTree>,
    ) -> String {
        let mut contexts = Vec::new();
        for hook in &self.message_pre {
            if let Ok(ctx) = hook(req, vector_store.clone(), page_index.clone()).await {
                if !ctx.is_empty() {
                    contexts.push(ctx);
                }
            }
        }
        contexts.join("\n\n")
    }
}

fn scheduled_session_id(job_id: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for b in job_id.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    out[0..8].copy_from_slice(&hash.to_le_bytes());
    out[8..16].copy_from_slice(&(!hash).to_le_bytes());
    out
}

fn agent_run_session_id(run_id: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for b in run_id.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    out[0..8].copy_from_slice(&hash.to_le_bytes());
    out[8..16].copy_from_slice(&(!hash).to_le_bytes());
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchedulingMode {
    Notify,
    Defer,
    Both,
}

impl SchedulingMode {
    fn as_tool_mode(self) -> &'static str {
        match self {
            SchedulingMode::Notify => "notify",
            SchedulingMode::Defer => "defer",
            SchedulingMode::Both => "both",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchedulingIntent {
    mode: SchedulingMode,
    normalized_schedule: Option<ToolSchedule>,
    deferred_task: Option<String>,
    rationale: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ToolSchedule {
    Every {
        seconds: u64,
    },
    At {
        at: String,
    },
    Daily {
        hour: u32,
        minute: u32,
        #[serde(default)]
        timezone: Option<String>,
    },
    Weekly {
        weekday: String,
        hour: u32,
        minute: u32,
        #[serde(default = "default_interval_weeks")]
        interval_weeks: u32,
        #[serde(default)]
        timezone: Option<String>,
    },
    Cron {
        expr: String,
        #[serde(default)]
        timezone: Option<String>,
    },
}

fn default_interval_weeks() -> u32 {
    1
}

impl ToolSchedule {
    fn to_schedule_parts(
        &self,
        default_timezone: chrono_tz::Tz,
    ) -> Result<(String, ScheduleSpec), String> {
        match self {
            ToolSchedule::Every { seconds } => {
                if *seconds == 0 {
                    return Err("schedule.every.seconds must be greater than 0".into());
                }
                let schedule_str = format!("every:{}s", seconds);
                Ok((schedule_str.clone(), ScheduleSpec::EverySeconds(*seconds)))
            }
            ToolSchedule::At { at } => {
                let dt = chrono::DateTime::parse_from_rfc3339(at)
                    .map_err(|_| "schedule.at.at must be RFC3339 with timezone offset".to_string())?
                    .with_timezone(&chrono::Utc);
                let schedule_str = format!("at:{}", dt.to_rfc3339());
                Ok((schedule_str.clone(), ScheduleSpec::Once(dt)))
            }
            ToolSchedule::Daily {
                hour,
                minute,
                timezone,
            } => {
                if *hour > 23 || *minute > 59 {
                    return Err("schedule.daily requires hour 0-23 and minute 0-59".into());
                }
                let timezone = timezone_from_option(timezone.as_deref(), default_timezone)?;
                let schedule_str = format!("daily@{:02}:{:02}#{}", hour, minute, timezone);
                Ok((
                    schedule_str.clone(),
                    ScheduleSpec::DailyAt {
                        hour: *hour,
                        minute: *minute,
                        timezone,
                    },
                ))
            }
            ToolSchedule::Weekly {
                weekday,
                hour,
                minute,
                interval_weeks,
                timezone,
            } => {
                if *hour > 23 || *minute > 59 {
                    return Err("schedule.weekly requires hour 0-23 and minute 0-59".into());
                }
                if *interval_weeks == 0 || *interval_weeks > 2 {
                    return Err("schedule.weekly.interval_weeks must be 1 or 2".into());
                }
                let weekday_parsed = parse_weekday_token(weekday)
                    .ok_or_else(|| "schedule.weekly.weekday must be mon..sun".to_string())?;
                let timezone = timezone_from_option(timezone.as_deref(), default_timezone)?;
                let prefix = if *interval_weeks == 2 {
                    "biweekly"
                } else {
                    "weekly"
                };
                let schedule_str = format!(
                    "{}:{}@{:02}:{:02}#{}",
                    prefix,
                    weekday_to_short_name(weekday_parsed),
                    hour,
                    minute,
                    timezone
                );
                Ok((
                    schedule_str.clone(),
                    ScheduleSpec::WeeklyAt {
                        interval_weeks: *interval_weeks,
                        weekday: weekday_parsed,
                        hour: *hour,
                        minute: *minute,
                        timezone,
                    },
                ))
            }
            ToolSchedule::Cron { expr, timezone } => {
                let timezone = timezone_from_option(timezone.as_deref(), default_timezone)?;
                let schedule_str = format!("cron[{}]:{}", timezone, expr.trim());
                let spec = ScheduleSpec::parse(&schedule_str)
                    .ok_or_else(|| "schedule.cron.expr is invalid".to_string())?;
                Ok((schedule_str, spec))
            }
        }
    }

    fn from_normalized_hint(schedule: &str, default_timezone: chrono_tz::Tz) -> Option<Self> {
        match ScheduleSpec::parse(schedule)? {
            ScheduleSpec::EverySeconds(seconds) => Some(Self::Every { seconds }),
            ScheduleSpec::Once(dt) => Some(Self::At {
                at: dt.to_rfc3339(),
            }),
            ScheduleSpec::DailyAt {
                hour,
                minute,
                timezone,
            } => Some(Self::Daily {
                hour,
                minute,
                timezone: Some(timezone.to_string()),
            }),
            ScheduleSpec::WeeklyAt {
                interval_weeks,
                weekday,
                hour,
                minute,
                timezone,
            } => Some(Self::Weekly {
                weekday: weekday_to_short_name(weekday).to_string(),
                hour,
                minute,
                interval_weeks,
                timezone: Some(timezone.to_string()),
            }),
            ScheduleSpec::Cron(_, expr, timezone) => Some(Self::Cron {
                expr,
                timezone: Some(timezone.to_string()),
            }),
        }
        .or_else(|| {
            let normalized = normalize_schedule_input(
                schedule,
                chrono::Utc::now().with_timezone(&default_timezone),
            );
            if normalized != schedule {
                Self::from_normalized_hint(&normalized, default_timezone)
            } else {
                None
            }
        })
    }

    fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{\"kind\":\"invalid\"}".into())
    }
}

fn timezone_from_option(
    timezone: Option<&str>,
    default_timezone: chrono_tz::Tz,
) -> Result<chrono_tz::Tz, String> {
    match timezone {
        Some(tz_name) => tz_name
            .trim()
            .parse::<chrono_tz::Tz>()
            .map_err(|_| format!("Invalid schedule timezone '{}'", tz_name)),
        None => Ok(default_timezone),
    }
}

fn weekday_to_short_name(weekday: chrono::Weekday) -> &'static str {
    match weekday {
        chrono::Weekday::Mon => "mon",
        chrono::Weekday::Tue => "tue",
        chrono::Weekday::Wed => "wed",
        chrono::Weekday::Thu => "thu",
        chrono::Weekday::Fri => "fri",
        chrono::Weekday::Sat => "sat",
        chrono::Weekday::Sun => "sun",
    }
}

fn parse_weekday_token(token: &str) -> Option<chrono::Weekday> {
    match token.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Some(chrono::Weekday::Mon),
        "tue" | "tues" | "tuesday" => Some(chrono::Weekday::Tue),
        "wed" | "wednesday" => Some(chrono::Weekday::Wed),
        "thu" | "thurs" | "thursday" => Some(chrono::Weekday::Thu),
        "fri" | "friday" => Some(chrono::Weekday::Fri),
        "sat" | "saturday" => Some(chrono::Weekday::Sat),
        "sun" | "sunday" => Some(chrono::Weekday::Sun),
        _ => None,
    }
}

fn parse_time_of_day_expr(expr: &str) -> Option<(u32, u32)> {
    let mut s = expr.trim().to_ascii_lowercase();
    if s.is_empty() || s.contains('*') || s.contains('/') {
        return None;
    }
    s = s.replace(' ', "");
    let (meridian, base) = if let Some(v) = s.strip_suffix("am") {
        ("am", v)
    } else if let Some(v) = s.strip_suffix("pm") {
        ("pm", v)
    } else {
        ("", s.as_str())
    };

    let (hour_raw, minute_raw) = if let Some((h, m)) = base.split_once(':') {
        (h, m)
    } else {
        (base, "0")
    };
    let mut hour = hour_raw.parse::<u32>().ok()?;
    let minute = minute_raw.parse::<u32>().ok()?;
    if minute > 59 {
        return None;
    }

    if !meridian.is_empty() {
        if hour == 0 || hour > 12 {
            return None;
        }
        hour %= 12;
        if meridian == "pm" {
            hour += 12;
        }
    } else if hour > 23 {
        return None;
    }
    Some((hour, minute))
}

fn normalize_schedule_input(raw: &str, now_local: chrono::DateTime<chrono_tz::Tz>) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "1m".to_string();
    }
    // If an absolute datetime is provided without `at:`, interpret it in request-local timezone.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return format!("at:{}", dt.to_rfc3339());
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M"))
    {
        use chrono::TimeZone;
        let tz = now_local.timezone();
        if let Some(dt) = tz
            .from_local_datetime(&ndt)
            .single()
            .or_else(|| tz.from_local_datetime(&ndt).earliest())
            .or_else(|| tz.from_local_datetime(&ndt).latest())
        {
            return format!("at:{}", dt.to_rfc3339());
        }
    }
    if let Some(at_text) = trimmed.strip_prefix("at:") {
        let at_text = at_text.trim();
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(at_text) {
            return format!("at:{}", dt.to_rfc3339());
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(at_text, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(at_text, "%Y-%m-%d %H:%M"))
        {
            use chrono::TimeZone;
            let tz = now_local.timezone();
            if let Some(dt) = tz
                .from_local_datetime(&ndt)
                .single()
                .or_else(|| tz.from_local_datetime(&ndt).earliest())
                .or_else(|| tz.from_local_datetime(&ndt).latest())
            {
                return format!("at:{}", dt.to_rfc3339());
            }
        }
    }
    if aria_intelligence::ScheduleSpec::parse(trimmed).is_some() {
        return trimmed.to_string();
    }
    if let Some((hour, minute)) = parse_time_of_day_expr(trimmed) {
        let mut target = now_local.date_naive().and_time(
            chrono::NaiveTime::from_hms_opt(hour, minute, 0).unwrap_or(chrono::NaiveTime::MIN),
        );
        if target <= now_local.naive_local() {
            target += chrono::Duration::days(1);
        }
        use chrono::TimeZone;
        let tz = now_local.timezone();
        if let Some(dt) = tz
            .from_local_datetime(&target)
            .single()
            .or_else(|| tz.from_local_datetime(&target).earliest())
            .or_else(|| tz.from_local_datetime(&target).latest())
        {
            return format!("at:{}", dt.to_rfc3339());
        }
    }
    trimmed.to_string()
}

fn sanitize_text_token(raw: &str) -> String {
    raw.trim_matches(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                ',' | '.' | '!' | '?' | ';' | '"' | '\'' | '(' | ')' | '[' | ']'
            )
    })
    .to_ascii_lowercase()
}

fn normalize_duration_pair(value: &str, unit: Option<&str>) -> Option<String> {
    let token = sanitize_text_token(value);
    if token.is_empty() {
        return None;
    }
    if let Some(num) = token
        .strip_suffix("seconds")
        .or_else(|| token.strip_suffix("second"))
    {
        return Some(format!("{}s", num));
    }
    if let Some(num) = token
        .strip_suffix("secs")
        .or_else(|| token.strip_suffix("sec"))
    {
        return Some(format!("{}s", num));
    }
    if let Some(num) = token.strip_suffix('s') {
        if num.chars().all(|c| c.is_ascii_digit()) {
            return Some(format!("{}s", num));
        }
    }
    if let Some(num) = token
        .strip_suffix("minutes")
        .or_else(|| token.strip_suffix("minute"))
    {
        return Some(format!("{}m", num));
    }
    if let Some(num) = token
        .strip_suffix("mins")
        .or_else(|| token.strip_suffix("min"))
    {
        return Some(format!("{}m", num));
    }
    if let Some(num) = token.strip_suffix('m') {
        if num.chars().all(|c| c.is_ascii_digit()) {
            return Some(format!("{}m", num));
        }
    }
    if let Some(num) = token
        .strip_suffix("hours")
        .or_else(|| token.strip_suffix("hour"))
    {
        return Some(format!("{}h", num));
    }
    if let Some(num) = token
        .strip_suffix("hrs")
        .or_else(|| token.strip_suffix("hr"))
    {
        return Some(format!("{}h", num));
    }
    if let Some(num) = token.strip_suffix('h') {
        if num.chars().all(|c| c.is_ascii_digit()) {
            return Some(format!("{}h", num));
        }
    }
    if token.chars().all(|c| c.is_ascii_digit()) {
        let u = unit.map(sanitize_text_token).unwrap_or_default();
        let suffix = match u.as_str() {
            "second" | "seconds" | "sec" | "secs" | "s" => "s",
            "minute" | "minutes" | "min" | "mins" | "m" => "m",
            "hour" | "hours" | "hr" | "hrs" | "h" => "h",
            _ => return None,
        };
        return Some(format!("{}{}", token, suffix));
    }
    None
}

fn extract_schedule_hint(text: &str, now_local: chrono::DateTime<chrono_tz::Tz>) -> Option<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let words_lower: Vec<String> = words.iter().map(|w| sanitize_text_token(w)).collect();

    for i in 0..words_lower.len() {
        let token = words_lower[i].as_str();
        if token == "in" || token == "after" {
            if let Some(next) = words.get(i + 1) {
                let unit = words.get(i + 2).copied();
                if let Some(duration) = normalize_duration_pair(next, unit) {
                    return Some(duration);
                }
            }
        }
        if let Some(duration) = normalize_duration_pair(words[i], words.get(i + 1).copied()) {
            if text.to_ascii_lowercase().contains(" in ")
                || text.to_ascii_lowercase().contains(" after ")
            {
                return Some(duration);
            }
        }
    }

    let lower = text.to_ascii_lowercase();
    if lower.contains("every day") || lower.contains("daily") || lower.contains("everyday") {
        if let Some(idx) = words_lower.iter().position(|w| w == "at") {
            let candidate = if let Some(next) = words.get(idx + 1) {
                if let Some(next2) = words.get(idx + 2) {
                    let joined = format!("{} {}", next, next2);
                    if parse_time_of_day_expr(&joined).is_some() {
                        joined
                    } else {
                        (*next).to_string()
                    }
                } else {
                    (*next).to_string()
                }
            } else {
                String::new()
            };
            if let Some((hour, minute)) = parse_time_of_day_expr(&candidate) {
                return Some(format!("daily@{:02}:{:02}", hour, minute));
            }
        }
    }

    const WEEKDAYS: [(&str, &str); 7] = [
        ("monday", "mon"),
        ("tuesday", "tue"),
        ("wednesday", "wed"),
        ("thursday", "thu"),
        ("friday", "fri"),
        ("saturday", "sat"),
        ("sunday", "sun"),
    ];
    for (full, short) in WEEKDAYS {
        if lower.contains(full) || lower.contains(short) {
            let biweekly = lower.contains("every two weeks")
                || lower.contains("every 2 weeks")
                || lower.contains("alternate ")
                || lower.contains("every other ");
            if let Some(idx) = words_lower.iter().position(|w| w == "at") {
                let candidate = if let Some(next) = words.get(idx + 1) {
                    if let Some(next2) = words.get(idx + 2) {
                        let joined = format!("{} {}", next, next2);
                        if parse_time_of_day_expr(&joined).is_some() {
                            joined
                        } else {
                            (*next).to_string()
                        }
                    } else {
                        (*next).to_string()
                    }
                } else {
                    String::new()
                };
                if let Some((hour, minute)) = parse_time_of_day_expr(&candidate) {
                    let prefix = if biweekly { "biweekly" } else { "weekly" };
                    return Some(format!("{}:{}@{:02}:{:02}", prefix, short, hour, minute));
                }
            }
        }
    }

    if let Some(idx) = words_lower.iter().position(|w| w == "at") {
        let candidate = if let Some(next) = words.get(idx + 1) {
            if let Some(next2) = words.get(idx + 2) {
                let joined = format!("{} {}", next, next2);
                if parse_time_of_day_expr(&joined).is_some() {
                    joined
                } else {
                    (*next).to_string()
                }
            } else {
                (*next).to_string()
            }
        } else {
            String::new()
        };
        if !candidate.is_empty() {
            let normalized = normalize_schedule_input(&candidate, now_local);
            if normalized != candidate
                || aria_intelligence::ScheduleSpec::parse(&normalized).is_some()
            {
                return Some(normalized);
            }
        }
    }

    None
}

fn strip_schedule_phrase(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    for needle in [" in ", " after ", " at ", " every day", " daily", " every "] {
        if let Some(idx) = lower.find(needle) {
            return text[..idx]
                .trim()
                .trim_end_matches(|c: char| c == ',' || c == '.')
                .to_string();
        }
    }
    text.trim()
        .trim_end_matches(|c: char| c == ',' || c == '.')
        .to_string()
}

fn infer_deferred_task(text: &str) -> Option<String> {
    let trimmed = strip_schedule_phrase(text);
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("remind me to ") {
        let offset = trimmed.len() - rest.len();
        return Some(trimmed[offset..].trim().to_string());
    }
    if let Some(rest) = lower.strip_prefix("remind me ") {
        let offset = trimmed.len() - rest.len();
        return Some(trimmed[offset..].trim().to_string());
    }
    Some(trimmed)
}

fn classify_scheduling_intent(
    text: &str,
    now_local: chrono::DateTime<chrono_tz::Tz>,
) -> Option<SchedulingIntent> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let schedule_hint = extract_schedule_hint(trimmed, now_local);
    let has_schedule = schedule_hint.is_some();
    let has_reminder_words = lower.contains("remind")
        || lower.contains("reminder")
        || lower.contains("notify me")
        || lower.contains("notification");
    let has_immediate_words = lower.contains(" now ")
        || lower.starts_with("now ")
        || lower.contains(" immediately")
        || lower.contains(" right now");

    if !has_schedule && !has_reminder_words {
        return None;
    }

    let mode = if has_schedule && has_immediate_words {
        SchedulingMode::Both
    } else if has_reminder_words {
        SchedulingMode::Notify
    } else {
        SchedulingMode::Defer
    };
    let rationale = match mode {
        SchedulingMode::Notify => "explicit reminder language",
        SchedulingMode::Defer => "delayed work request without reminder phrasing",
        SchedulingMode::Both => "request contains immediate and delayed cues",
    };

    Some(SchedulingIntent {
        mode,
        normalized_schedule: schedule_hint
            .map(|s| normalize_schedule_input(&s, now_local))
            .and_then(|s| ToolSchedule::from_normalized_hint(&s, now_local.timezone())),
        deferred_task: infer_deferred_task(trimmed),
        rationale,
    })
}

fn scheduling_intent_context(intent: &SchedulingIntent, user_timezone: chrono_tz::Tz) -> String {
    PromptManager::build_scheduling_context(
        intent.mode.as_tool_mode(),
        intent.rationale,
        intent
            .normalized_schedule
            .as_ref()
            .map(ToolSchedule::to_json_string)
            .as_deref(),
        intent.deferred_task.as_deref(),
        user_timezone.name(),
        &chrono::Utc::now()
            .with_timezone(&user_timezone)
            .format("%Y-%m-%d %H:%M:%S %:z")
            .to_string(),
    )
}

fn parse_tz_or_utc(tz_name: &str) -> chrono_tz::Tz {
    tz_name
        .trim()
        .parse::<chrono_tz::Tz>()
        .unwrap_or(chrono_tz::UTC)
}

fn resolve_request_timezone(config: &Config, user_id: &str) -> chrono_tz::Tz {
    if let Some(tz_name) = config.localization.user_timezones.get(user_id) {
        return parse_tz_or_utc(tz_name);
    }
    parse_tz_or_utc(&config.localization.default_timezone)
}

fn resolve_request_timezone_with_overrides(
    config: &Config,
    user_id: &str,
    overrides: Option<&dashmap::DashMap<String, String>>,
) -> chrono_tz::Tz {
    if let Some(map) = overrides {
        if let Some(tz_name) = map.get(user_id) {
            return parse_tz_or_utc(tz_name.value());
        }
    }
    resolve_request_timezone(config, user_id)
}

fn persist_user_timezone_override(
    runtime_config_path: &std::path::Path,
    config: &Config,
    user_id: &str,
    tz_name: Option<&str>,
) -> Result<(), String> {
    struct RuntimeConfigLockGuard {
        path: std::path::PathBuf,
    }
    impl Drop for RuntimeConfigLockGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
    fn acquire_runtime_config_lock(
        runtime_config_path: &std::path::Path,
    ) -> Result<RuntimeConfigLockGuard, String> {
        use std::io::Write;
        let lock_path = runtime_config_path.with_extension("runtime.lock");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    let _ = writeln!(
                        file,
                        "pid={} ts_us={}",
                        std::process::id(),
                        chrono::Utc::now().timestamp_micros()
                    );
                    let _ = file.sync_all();
                    return Ok(RuntimeConfigLockGuard { path: lock_path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Ok(meta) = std::fs::metadata(&lock_path) {
                        if let Ok(modified) = meta.modified() {
                            if modified
                                .elapsed()
                                .unwrap_or(std::time::Duration::from_secs(0))
                                > std::time::Duration::from_secs(30)
                            {
                                let _ = std::fs::remove_file(&lock_path);
                            }
                        }
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(format!(
                            "timed out acquiring runtime config lock '{}'",
                            lock_path.display()
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                Err(err) => {
                    return Err(format!(
                        "failed to create runtime config lock '{}': {}",
                        lock_path.display(),
                        err
                    ));
                }
            }
        }
    }

    let _lock = acquire_runtime_config_lock(runtime_config_path)?;

    let mut root = if runtime_config_path.exists() {
        let content = std::fs::read_to_string(runtime_config_path)
            .map_err(|e| format!("failed to read runtime config: {}", e))?;
        serde_json::from_str::<serde_json::Value>(&content)
            .map_err(|e| format!("failed to parse runtime config: {}", e))?
    } else {
        serde_json::to_value(config).map_err(|e| format!("failed to seed runtime config: {}", e))?
    };

    let root_obj = root
        .as_object_mut()
        .ok_or_else(|| "runtime config root is not an object".to_string())?;
    let localization = root_obj
        .entry("localization")
        .or_insert_with(|| serde_json::json!({}));
    if !localization.is_object() {
        *localization = serde_json::json!({});
    }
    let localization_obj = localization
        .as_object_mut()
        .ok_or_else(|| "runtime localization is not an object".to_string())?;
    localization_obj
        .entry("default_timezone")
        .or_insert_with(|| serde_json::json!(config.localization.default_timezone.clone()));
    let user_timezones = localization_obj
        .entry("user_timezones")
        .or_insert_with(|| serde_json::json!({}));
    if !user_timezones.is_object() {
        *user_timezones = serde_json::json!({});
    }
    let tz_obj = user_timezones
        .as_object_mut()
        .ok_or_else(|| "runtime localization.user_timezones is not an object".to_string())?;
    match tz_name {
        Some(tz) => {
            tz_obj.insert(
                user_id.to_string(),
                serde_json::Value::String(tz.to_string()),
            );
        }
        None => {
            tz_obj.remove(user_id);
        }
    }

    if let Some(parent) = runtime_config_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create runtime config dir: {}", e))?;
    }
    let tmp = runtime_config_path.with_extension("runtime.json.tmp");
    let json = serde_json::to_string_pretty(&root)
        .map_err(|e| format!("failed to serialize runtime config: {}", e))?;
    {
        use std::io::Write;
        let mut tmp_file = std::fs::File::create(&tmp)
            .map_err(|e| format!("failed to write temp runtime config: {}", e))?;
        tmp_file
            .write_all(json.as_bytes())
            .map_err(|e| format!("failed to write temp runtime config: {}", e))?;
        tmp_file
            .sync_all()
            .map_err(|e| format!("failed to flush temp runtime config: {}", e))?;
    }
    std::fs::rename(&tmp, runtime_config_path)
        .map_err(|e| format!("failed to replace runtime config: {}", e))?;
    if let Some(parent) = runtime_config_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn looks_like_tool_payload(text: &str) -> bool {
    if text.contains("<tool_call>") && text.contains("</tool_call>") {
        return true;
    }
    let trimmed = text.trim();
    let body = if let Some(rest) = trimmed.strip_prefix("```json") {
        let rest = rest.trim_start();
        let end = rest.rfind("```").unwrap_or(rest.len());
        &rest[..end]
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest.trim_start();
        let end = rest.rfind("```").unwrap_or(rest.len());
        &rest[..end]
    } else {
        trimmed
    };
    let start = match body.find('{') {
        Some(i) => i,
        None => return false,
    };
    let mut candidate = &body[start..];
    if let Some(idx) = candidate.rfind('}') {
        candidate = &candidate[..=idx];
    }
    let parsed = serde_json::from_str::<serde_json::Value>(candidate).ok();
    let Some(v) = parsed else {
        return false;
    };
    let Some(obj) = v.as_object() else {
        return false;
    };
    let has_tool_key = ["tool", "name", "function", "fn", "action"]
        .iter()
        .any(|k| obj.get(*k).and_then(|x| x.as_str()).is_some());
    let has_args_key = ["args", "arguments", "parameters", "input", "params"]
        .iter()
        .any(|k| obj.contains_key(*k));
    has_tool_key && has_args_key
}

fn user_facing_tool_recovery_message(
    _request_text: &str,
    tool_name: Option<&str>,
    error: Option<&str>,
) -> String {
    let tool_name = tool_name.unwrap_or("tool");

    match error {
        Some(err) if !err.trim().is_empty() => format!(
            "I couldn't execute the generated {} call cleanly: {}",
            tool_name, err
        ),
        _ => format!(
            "I couldn't execute the generated {} call cleanly. Please retry with a more specific request.",
            tool_name
        ),
    }
}

/// Universal response dispatcher that routes messages back to the originating channel.
async fn send_universal_response(req: &AgentRequest, text: &str, config: &ResolvedAppConfig) {
    let correlation_id = Some(uuid::Uuid::from_bytes(req.request_id).to_string());
    let store = RuntimeStore::for_sessions_dir(Path::new(&config.ssmu.sessions_dir));
    let mut targets = vec![req.channel];
    for rule in &config.gateway.fanout {
        if !rule.enabled {
            continue;
        }
        let Some(source) = parse_gateway_channel_label(&rule.source) else {
            continue;
        };
        let Some(destination) = parse_gateway_channel_label(&rule.destination) else {
            continue;
        };
        if source == req.channel && !targets.contains(&destination) {
            targets.push(destination);
        }
    }
    for target_channel in targets {
        let recipient_id = match target_channel {
            aria_core::GatewayChannel::Telegram => {
                i64::from_le_bytes(req.session_id[0..8].try_into().unwrap_or([0u8; 8])).to_string()
            }
            _ => req.user_id.clone(),
        };
        let mut envelope = envelope_from_text_response_with_correlation(
            req.session_id,
            target_channel,
            recipient_id.clone(),
            text,
            correlation_id.clone(),
        );
        envelope.envelope_id = deterministic_outbound_envelope_id(
            req.request_id,
            target_channel,
            &recipient_id,
            text,
        );
        if store
            .is_outbound_delivery_sent(envelope.envelope_id)
            .unwrap_or(false)
        {
            debug!(
                channel = ?target_channel,
                envelope_id = %uuid::Uuid::from_bytes(envelope.envelope_id),
                "Skipping duplicate outbound delivery for already-sent envelope"
            );
            continue;
        }
        let status = match dispatch_outbound(&envelope, config).await {
            Ok(()) => {
                crate::channel_health::record_channel_health_event(
                    target_channel,
                    crate::channel_health::ChannelHealthEventKind::OutboundSent,
                );
                let _ = store.record_outbound_delivery(&envelope, "sent", None);
                "sent"
            }
            Err(err) => {
                crate::channel_health::record_channel_health_event(
                    target_channel,
                    crate::channel_health::ChannelHealthEventKind::OutboundFailed,
                );
                let _ = store.record_outbound_delivery(&envelope, "failed", Some(&err));
                if config.cluster.is_cluster() {
                    let now_us = chrono::Utc::now().timestamp_micros() as u64;
                    let _ = store.enqueue_durable_message(&crate::runtime_store::DurableQueueMessage {
                        message_id: format!("outbox-{}", uuid::Uuid::from_bytes(envelope.envelope_id)),
                        queue: crate::runtime_store::DurableQueueKind::Outbox,
                        tenant_id: config.cluster.tenant_id.clone(),
                        workspace_scope: config.cluster.workspace_scope.clone(),
                        dedupe_key: Some(uuid::Uuid::from_bytes(envelope.envelope_id).to_string()),
                        payload_json: serde_json::to_string(&envelope).unwrap_or_default(),
                        attempt_count: 0,
                        last_error: Some(err.clone()),
                        status: crate::runtime_store::DurableQueueStatus::Pending,
                        visible_at_us: now_us,
                        claimed_by: None,
                        claimed_until_us: None,
                        created_at_us: now_us,
                        updated_at_us: now_us,
                    });
                }
                "failed"
            }
        };
        debug!(channel = ?target_channel, delivery_status = status, "Universal response dispatched");
    }
}

async fn retry_failed_outbound_deliveries_once(
    config: &ResolvedAppConfig,
    limit: usize,
) -> Result<usize, String> {
    if !config.features.outbox_delivery {
        return Ok(0);
    }
    let store = RuntimeStore::for_sessions_dir(Path::new(&config.ssmu.sessions_dir));
    if config.cluster.is_cluster() {
        let mut recovered = 0usize;
        let worker_id = format!("outbox:{}:{}", config.node.id, runtime_instance_id());
        for _ in 0..limit {
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            let Some(message) = store.claim_durable_message(
                crate::runtime_store::DurableQueueKind::Outbox,
                &config.cluster.tenant_id,
                &config.cluster.workspace_scope,
                &worker_id,
                now_us,
                now_us + 30_000_000,
            )? else {
                break;
            };
            let envelope: aria_core::OutboundEnvelope =
                serde_json::from_str(&message.payload_json)
                .map_err(|e| format!("parse durable outbox payload failed: {}", e))?;
            match crate::outbound::dispatch_outbound_with_retry(&envelope, config, 3).await {
                Ok(()) => {
                    crate::channel_health::record_channel_health_event(
                        envelope.channel,
                        crate::channel_health::ChannelHealthEventKind::OutboundSent,
                    );
                    let _ = store.record_outbound_delivery(&envelope, "sent", None);
                    let _ = store.ack_durable_message(&message.message_id, now_us);
                    recovered = recovered.saturating_add(1);
                }
                Err(err) => {
                    crate::channel_health::record_channel_health_event(
                        envelope.channel,
                        crate::channel_health::ChannelHealthEventKind::OutboundFailed,
                    );
                    let _ = store.record_outbound_delivery(&envelope, "failed", Some(&err));
                    let _ = store.fail_durable_message(
                        &message.message_id,
                        &err,
                        now_us,
                        now_us + 5_000_000,
                        3,
                    );
                }
            }
        }
        return Ok(recovered);
    }
    let failed = store.list_outbound_deliveries_by_status("failed", limit)?;
    let mut recovered = 0usize;
    for envelope in failed {
        if store
            .is_outbound_delivery_sent(envelope.envelope_id)
            .unwrap_or(false)
        {
            continue;
        }
        match crate::outbound::dispatch_outbound_with_retry(&envelope, config, 3).await {
            Ok(()) => {
                crate::channel_health::record_channel_health_event(
                    envelope.channel,
                    crate::channel_health::ChannelHealthEventKind::OutboundSent,
                );
                let _ = store.record_outbound_delivery(&envelope, "sent", None);
                recovered = recovered.saturating_add(1);
            }
            Err(err) => {
                crate::channel_health::record_channel_health_event(
                    envelope.channel,
                    crate::channel_health::ChannelHealthEventKind::OutboundFailed,
                );
                let _ = store.record_outbound_delivery(&envelope, "failed", Some(&err));
            }
        }
    }
    Ok(recovered)
}

fn request_text_from_content(content: &MessageContent) -> String {
    match content {
        MessageContent::Text(t) => t.clone(),
        MessageContent::Image { url, caption } => caption
            .clone()
            .unwrap_or_else(|| format!("User sent an image: {}", url)),
        MessageContent::Audio { url, transcript } => transcript
            .clone()
            .unwrap_or_else(|| format!("User sent an audio message: {}", url)),
        MessageContent::Video {
            url,
            caption,
            transcript,
        } => transcript
            .clone()
            .or_else(|| caption.clone())
            .unwrap_or_else(|| format!("User sent a video: {}", url)),
        MessageContent::Document {
            url,
            caption,
            mime_type,
        } => caption.clone().unwrap_or_else(|| {
            format!(
                "User sent a document ({}) : {}",
                mime_type.as_deref().unwrap_or("unknown"),
                url
            )
        }),
        MessageContent::Location { lat, lng } => {
            format!("User shared location lat={}, lng={}", lat, lng)
        }
    }
}

fn build_sub_agent_result_context(
    store: &RuntimeStore,
    session_uuid: uuid::Uuid,
) -> Result<String, String> {
    let runs = store.list_agent_runs_for_session(session_uuid)?;
    let mut terminal: Vec<AgentRunRecord> = runs
        .into_iter()
        .filter(|run| {
            matches!(
                run.status,
                AgentRunStatus::Completed
                    | AgentRunStatus::Failed
                    | AgentRunStatus::Cancelled
                    | AgentRunStatus::TimedOut
            ) && run.parent_run_id.is_some()
        })
        .collect();
    terminal.sort_by_key(|run| run.finished_at_us.unwrap_or(run.created_at_us));
    terminal.reverse();
    if terminal.is_empty() {
        return Ok(String::new());
    }
    let mut lines = vec!["Sub-agent Updates:".to_string()];
    for run in terminal.into_iter().take(5) {
        let summary = run
            .result
            .and_then(|result| result.response_summary.or(result.error))
            .unwrap_or_else(|| "(no summary)".to_string());
        lines.push(format!(
            "- {} [{}] agent={} summary={}",
            run.run_id,
            format!("{:?}", run.status).to_ascii_lowercase(),
            run.agent_id,
            summary
        ));
    }
    Ok(lines.join("\n"))
}

fn classify_rag_corpus(metadata: &aria_ssmu::vector::ChunkMetadata) -> RagCorpus {
    use aria_ssmu::vector::ChunkKind;

    if matches!(metadata.kind, ChunkKind::SessionSummary) {
        return RagCorpus::Session;
    }

    let source = metadata.source_id.to_ascii_lowercase();
    let tags = metadata
        .tags
        .iter()
        .map(|tag| tag.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let has_tag = |needle: &str| tags.iter().any(|tag| tag == needle);

    if has_tag("social")
        || source.contains("twitter")
        || source.contains("x.com")
        || source.contains("social")
    {
        return RagCorpus::Social;
    }
    if has_tag("external")
        || has_tag("web")
        || has_tag("news")
        || source.starts_with("http://")
        || source.starts_with("https://")
    {
        return RagCorpus::External;
    }
    if has_tag("policy")
        || has_tag("runtime")
        || source.starts_with("policy")
        || source.starts_with("security")
        || source.starts_with("agent.")
        || source.starts_with("tool.")
    {
        return RagCorpus::PolicyRuntime;
    }
    if has_tag("workspace")
        || has_tag("source")
        || has_tag("files")
        || source.starts_with("workspace")
    {
        return RagCorpus::Workspace;
    }

    RagCorpus::Workspace
}

fn corpus_allowed_for_trust_profile(
    corpus: RagCorpus,
    trust_profile: Option<aria_core::TrustProfile>,
) -> bool {
    match trust_profile {
        Some(
            aria_core::TrustProfile::TrustedLocal
            | aria_core::TrustProfile::TrustedWorkspace
            | aria_core::TrustProfile::RoboticsControl,
        ) => !matches!(corpus, RagCorpus::External | RagCorpus::Social),
        Some(aria_core::TrustProfile::UntrustedWeb) => !matches!(corpus, RagCorpus::Social),
        Some(aria_core::TrustProfile::UntrustedSocial) => true,
        _ => !matches!(corpus, RagCorpus::Social),
    }
}

fn retrieval_scope_for_corpus(corpus: RagCorpus) -> aria_core::RetrievalScope {
    match corpus {
        RagCorpus::Session => aria_core::RetrievalScope::SessionMemory,
        RagCorpus::Workspace => aria_core::RetrievalScope::Workspace,
        RagCorpus::PolicyRuntime => aria_core::RetrievalScope::PolicyRuntime,
        RagCorpus::External => aria_core::RetrievalScope::External,
        RagCorpus::Social => aria_core::RetrievalScope::Social,
    }
}

fn corpus_allowed_for_retrieval_profile(
    corpus: RagCorpus,
    capability_profile: Option<&AgentCapabilityProfile>,
) -> bool {
    let Some(profile) = capability_profile else {
        return true;
    };
    if profile.retrieval_scopes.is_empty() {
        return true;
    }
    let required = retrieval_scope_for_corpus(corpus);
    profile.retrieval_scopes.contains(&required)
}

fn control_document_kind_for_name(name: &str) -> Option<aria_core::ControlDocumentKind> {
    match name.to_ascii_lowercase().as_str() {
        "instructions.md" => Some(aria_core::ControlDocumentKind::Instructions),
        "skills.md" => Some(aria_core::ControlDocumentKind::Skills),
        "tools.md" => Some(aria_core::ControlDocumentKind::Tools),
        "memory.md" => Some(aria_core::ControlDocumentKind::Memory),
        _ => None,
    }
}

fn control_document_precedence(kind: aria_core::ControlDocumentKind) -> u8 {
    match kind {
        aria_core::ControlDocumentKind::Instructions => 0,
        aria_core::ControlDocumentKind::Skills => 1,
        aria_core::ControlDocumentKind::Tools => 2,
        aria_core::ControlDocumentKind::Memory => 3,
    }
}

fn discover_control_documents(
    workspace_root: &Path,
    updated_at_us: u64,
) -> Result<Vec<aria_core::ControlDocumentEntry>, String> {
    let mut entries = Vec::new();
    if !workspace_root.exists() || !workspace_root.is_dir() {
        return Ok(entries);
    }

    let mut stack = vec![workspace_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let read_dir = std::fs::read_dir(&dir)
            .map_err(|e| format!("read control-doc dir {} failed: {}", dir.display(), e))?;
        for entry in read_dir {
            let entry = entry.map_err(|e| format!("read control-doc entry failed: {}", e))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|e| format!("read file type failed for {}: {}", path.display(), e))?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(kind) = control_document_kind_for_name(file_name) else {
                continue;
            };
            let body = std::fs::read_to_string(&path)
                .map_err(|e| format!("read control document {} failed: {}", path.display(), e))?;
            let mut hasher = Sha256::new();
            hasher.update(body.as_bytes());
            let sha256_hex = hex::encode(hasher.finalize());
            let relative_path = path
                .strip_prefix(workspace_root)
                .map_err(|e| format!("strip workspace prefix failed: {}", e))?
                .to_string_lossy()
                .to_string();
            entries.push(aria_core::ControlDocumentEntry {
                document_id: format!(
                    "{}::{}",
                    workspace_root.display(),
                    relative_path.replace('\\', "/")
                ),
                workspace_root: workspace_root.to_string_lossy().to_string(),
                relative_path,
                kind,
                sha256_hex,
                body,
                updated_at_us,
            });
        }
    }

    entries.sort_by_key(|entry| {
        (
            control_document_precedence(entry.kind),
            entry.relative_path.clone(),
        )
    });
    Ok(entries)
}

fn index_control_documents_for_workspace(
    store: &RuntimeStore,
    workspace_root: &Path,
    updated_at_us: u64,
) -> Result<Vec<aria_core::ControlDocumentEntry>, String> {
    let entries = discover_control_documents(workspace_root, updated_at_us)?;
    for entry in &entries {
        store.upsert_control_document(entry, updated_at_us)?;
    }
    Ok(entries)
}

fn build_control_document_context(
    store: &RuntimeStore,
    workspace_roots: &[String],
    capability_profile: Option<&AgentCapabilityProfile>,
) -> Result<String, String> {
    if let Some(profile) = capability_profile {
        if !profile.retrieval_scopes.is_empty()
            && !profile
                .retrieval_scopes
                .contains(&aria_core::RetrievalScope::ControlDocument)
        {
            return Ok(String::new());
        }
    }

    let mut entries = Vec::new();
    for root in workspace_roots {
        entries.extend(store.list_control_documents(root)?);
    }
    entries.sort_by_key(|entry| {
        (
            control_document_precedence(entry.kind),
            entry.relative_path.clone(),
        )
    });
    if entries.is_empty() {
        return Ok(String::new());
    }

    let rendered = entries
        .into_iter()
        .map(|entry| {
            format!(
                "[{:?}] {}:\n{}",
                entry.kind, entry.relative_path, entry.body
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    Ok(format!("Control Documents:\n{}", rendered))
}

fn detect_control_document_conflicts(
    store: &RuntimeStore,
    workspace_roots: &[String],
) -> Result<Vec<ControlDocumentConflict>, String> {
    let mut entries = Vec::new();
    for root in workspace_roots {
        entries.extend(store.list_control_documents(root)?);
    }
    let mut by_kind: BTreeMap<
        String,
        (
            aria_core::ControlDocumentKind,
            Vec<aria_core::ControlDocumentEntry>,
        ),
    > = BTreeMap::new();
    for entry in entries {
        let key = format!("{:?}", entry.kind);
        by_kind
            .entry(key)
            .or_insert_with(|| (entry.kind, Vec::new()))
            .1
            .push(entry);
    }
    let mut conflicts = Vec::new();
    let mut grouped = by_kind.into_values().collect::<Vec<_>>();
    grouped.sort_by_key(|(kind, _)| control_document_precedence(*kind));
    for (kind, docs) in grouped {
        if docs.len() <= 1 {
            continue;
        }
        let paths = docs
            .iter()
            .map(|entry| entry.relative_path.clone())
            .collect::<Vec<_>>();
        conflicts.push(ControlDocumentConflict {
            kind,
            paths: paths.clone(),
            diagnostic: format!(
                "{:?} appears in multiple files with precedence determined by path order: {}",
                kind,
                paths.join(", ")
            ),
        });
    }
    Ok(conflicts)
}

fn build_split_rag_context(
    request_text: &str,
    query_embedding: &[f32],
    vector_store: &VectorStore,
    page_index: &PageIndexTree,
    keyword_index: &KeywordIndex,
    capability_profile: Option<&AgentCapabilityProfile>,
    trust_profile: Option<aria_core::TrustProfile>,
) -> (String, RetrievalBuildMetrics) {
    let hybrid = HybridMemoryEngine::new(vector_store, page_index, QueryPlannerConfig::default())
        .with_keyword_index(keyword_index)
        .retrieve_hybrid(request_text, query_embedding, 5, 3, 0.005);
    let mut metrics = RetrievalBuildMetrics::default();

    let mut session_chunks = Vec::new();
    let mut workspace_chunks = Vec::new();
    let mut policy_chunks = Vec::new();
    let mut external_chunks = Vec::new();
    let mut social_chunks = Vec::new();

    for result in vector_store.hybrid_search(
        query_embedding,
        Some(keyword_index),
        request_text,
        5,
        0.005,
        60.0,
    ) {
        let rendered = format!(
            "- {:.3} {}: {}",
            result.rrf_score, result.id, result.content
        );
        let corpus = classify_rag_corpus(&result.metadata);
        if !corpus_allowed_for_retrieval_profile(corpus, capability_profile) {
            continue;
        }
        match corpus {
            RagCorpus::Session => {
                metrics.session_hits += 1;
                session_chunks.push(rendered)
            }
            RagCorpus::Workspace => {
                metrics.workspace_hits += 1;
                workspace_chunks.push(rendered)
            }
            RagCorpus::PolicyRuntime => {
                metrics.policy_hits += 1;
                policy_chunks.push(rendered)
            }
            RagCorpus::External => {
                if corpus_allowed_for_trust_profile(RagCorpus::External, trust_profile) {
                    metrics.external_hits += 1;
                    external_chunks.push(rendered);
                }
            }
            RagCorpus::Social => {
                if corpus_allowed_for_trust_profile(RagCorpus::Social, trust_profile) {
                    metrics.social_hits += 1;
                    social_chunks.push(rendered);
                }
            }
        }
    }

    let page_context = hybrid
        .page_context
        .into_iter()
        .map(|n| format!("- {}: {}", n.title, n.summary))
        .collect::<Vec<_>>()
        .join("\n");
    metrics.page_context_hits = if page_context.is_empty() { 0 } else { page_context.lines().count() as u32 };

    let mut sections = vec![format!("Plan: {:?}", hybrid.plan)];
    if !session_chunks.is_empty() {
        sections.push(format!("Session Context:\n{}", session_chunks.join("\n")));
    }
    if !workspace_chunks.is_empty() {
        sections.push(format!(
            "Workspace Context:\n{}",
            workspace_chunks.join("\n")
        ));
    }
    if !policy_chunks.is_empty() {
        sections.push(format!(
            "Policy/Runtime Context:\n{}",
            policy_chunks.join("\n")
        ));
    }
    if !external_chunks.is_empty() {
        sections.push(format!("External Context:\n{}", external_chunks.join("\n")));
    }
    if !social_chunks.is_empty() {
        sections.push(format!("Social Context:\n{}", social_chunks.join("\n")));
    }
    if !page_context.is_empty() {
        sections.push(format!("PageIndex Context:\n{}", page_context));
    }

    (sections.join("\n\n"), metrics)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentResolution {
    Resolved(String),
    NeedsClarification(String),
}

fn build_agent_clarification(candidates: &[String]) -> String {
    PromptManager::build_clarification_message(candidates)
}

fn apply_compaction_result(
    session_memory: &aria_ssmu::SessionMemory,
    session_uuid: uuid::Uuid,
    summary_res: &str,
    remove_count: usize,
    timestamp_us: u64,
) {
    if let Some(json_start) = summary_res.find('{') {
        if let Some(json_end) = summary_res.rfind('}') {
            let json_str = &summary_res[json_start..=json_end];
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(constraints) =
                    parsed.get("durable_constraints").and_then(|v| v.as_array())
                {
                    for c in constraints {
                        if let Some(c_str) = c.as_str() {
                            let _ = session_memory
                                .add_durable_constraint(session_uuid, c_str.to_string());
                        }
                    }
                }
                if let Some(summary_text) = parsed.get("summary").and_then(|v| v.as_str()) {
                    let summary_msg = aria_ssmu::Message {
                        role: "system".into(),
                        content: format!("[Previous Conversation Summary]: {}", summary_text),
                        timestamp_us,
                    };
                    let _ =
                        session_memory.replace_old_history(session_uuid, remove_count, summary_msg);
                }
            }
        }
    }
}

fn should_trigger_compaction(
    state: Option<&aria_core::CompactionState>,
    history_len: usize,
    total_tokens: usize,
    now_us: u64,
) -> bool {
    if history_len <= 3 {
        return false;
    }
    if total_tokens < 2000 && history_len < 24 {
        return false;
    }
    let min_interval_us = 5 * 60 * 1_000_000;
    if let Some(state) = state {
        if matches!(state.status, aria_core::CompactionStatus::Running) {
            return false;
        }
        if let Some(last_completed) = state.last_completed_at_us {
            if now_us.saturating_sub(last_completed) < min_interval_us {
                return false;
            }
        }
        if let Some(last_started) = state.last_started_at_us {
            if now_us.saturating_sub(last_started) < min_interval_us
                && !matches!(state.status, aria_core::CompactionStatus::Failed)
            {
                return false;
            }
        }
    }
    true
}

#[derive(Debug, Clone)]
struct CompactionLeaseClaim {
    resource_key: String,
    holder_id: String,
    fencing_token: u64,
}

#[derive(Debug, Clone)]
struct ResourceLeaseClaim {
    sessions_dir: PathBuf,
    resource_key: String,
    holder_id: String,
    fencing_token: u64,
}

impl Drop for ResourceLeaseClaim {
    fn drop(&mut self) {
        let store = RuntimeStore::for_sessions_dir(&self.sessions_dir);
        let _ = store.release_resource_lease(
            &self.resource_key,
            &self.holder_id,
            self.fencing_token,
        );
    }
}

async fn acquire_resource_lease_with_retry(
    sessions_dir: &Path,
    resource_key: &str,
    holder_id: &str,
    lease_ttl_seconds: u64,
    retry_attempts: u32,
    retry_delay_ms: u64,
    busy_error: &str,
) -> Result<ResourceLeaseClaim, OrchestratorError> {
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    let attempts = retry_attempts.max(1);
    for attempt in 0..attempts {
        let now_us = chrono::Utc::now().timestamp_micros() as u64;
        let lease_until_us = now_us + lease_ttl_seconds.max(1) * 1_000_000;
        match store.try_acquire_resource_lease(
            resource_key,
            "exclusive",
            holder_id,
            now_us,
            lease_until_us,
        ) {
            Ok(Some(fencing_token)) => {
                return Ok(ResourceLeaseClaim {
                    sessions_dir: sessions_dir.to_path_buf(),
                    resource_key: resource_key.to_string(),
                    holder_id: holder_id.to_string(),
                    fencing_token,
                });
            }
            Ok(None) if attempt + 1 < attempts => {
                tokio::time::sleep(Duration::from_millis(retry_delay_ms.max(1))).await;
            }
            Ok(None) => {
                return Err(OrchestratorError::ToolError(busy_error.to_string()));
            }
            Err(err) => {
                return Err(OrchestratorError::ToolError(format!(
                    "resource lease failed for {}: {}",
                    resource_key, err
                )));
            }
        }
    }
    Err(OrchestratorError::ToolError(busy_error.to_string()))
}

async fn acquire_shared_quota_claim(
    sessions_dir: &Path,
    scope: &str,
    limit: usize,
    holder_id: &str,
    lease_ttl_seconds: u64,
) -> Result<ResourceLeaseClaim, OrchestratorError> {
    let bounded_limit = limit.max(1);
    for slot in 0..bounded_limit {
        let resource_key = format!("quota:{}:slot:{}", scope, slot);
        if let Ok(claim) = acquire_resource_lease_with_retry(
            sessions_dir,
            &resource_key,
            holder_id,
            lease_ttl_seconds,
            1,
            1,
            &format!("shared quota busy for {}", scope),
        )
        .await
        {
            return Ok(claim);
        }
    }
    Err(OrchestratorError::BackendOverloaded(format!(
        "system busy: shared quota exhausted for {}",
        scope
    )))
}

fn try_mark_compaction_inflight(
    session_uuid: uuid::Uuid,
    sessions_dir: &std::path::Path,
) -> Option<CompactionLeaseClaim> {
    if let Ok(mut guard) = app_runtime().in_flight_compactions.lock() {
        if !guard.insert(session_uuid.to_string()) {
            return None;
        }
    } else {
        return None;
    }
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    let lease_until_us = now_us + 15 * 60 * 1_000_000;
    let resource_key = format!("compaction:{}", session_uuid);
    let holder_id = format!("compaction-worker:{}", crate::runtime_instance_id());
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    match store.try_acquire_resource_lease(
        &resource_key,
        "exclusive",
        &holder_id,
        now_us,
        lease_until_us,
    ) {
        Ok(Some(fencing_token)) => Some(CompactionLeaseClaim {
            resource_key,
            holder_id,
            fencing_token,
        }),
        _ => {
            if let Ok(mut guard) = app_runtime().in_flight_compactions.lock() {
                guard.remove(&session_uuid.to_string());
            }
            None
        }
    }
}

fn clear_compaction_inflight(
    session_uuid: uuid::Uuid,
    sessions_dir: &std::path::Path,
    claim: &CompactionLeaseClaim,
) {
    if let Ok(mut guard) = app_runtime().in_flight_compactions.lock() {
        guard.remove(&session_uuid.to_string());
    }
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    let _ = store.release_resource_lease(
        &claim.resource_key,
        &claim.holder_id,
        claim.fencing_token,
    );
}

fn compaction_state_record(
    session_uuid: uuid::Uuid,
    status: aria_core::CompactionStatus,
    last_started_at_us: Option<u64>,
    last_completed_at_us: Option<u64>,
    summary_hash: Option<String>,
    summary_version: u32,
    last_error: Option<String>,
) -> aria_core::CompactionState {
    aria_core::CompactionState {
        session_id: *session_uuid.as_bytes(),
        status,
        last_started_at_us,
        last_completed_at_us,
        metadata: aria_core::CompactionMetadata {
            summary_hash,
            summary_version,
            last_error,
        },
    }
}

fn shared_channel_quota_limit(global_limit: usize) -> usize {
    global_limit.max(1).min(4)
}

fn shared_agent_class_quota_limit(global_limit: usize, class: aria_core::AgentClass) -> usize {
    let global_limit = global_limit.max(1);
    match class {
        aria_core::AgentClass::Restricted | aria_core::AgentClass::Notifier => 1,
        aria_core::AgentClass::Specialist => global_limit.min(2),
        aria_core::AgentClass::RoboticsPlanner => global_limit.min(2),
        aria_core::AgentClass::Generalist => global_limit.min(3),
    }
}

fn shared_user_quota_limit(global_limit: usize) -> usize {
    global_limit.max(1).min(2)
}

fn select_prompt_tool_window<E: EmbeddingModel>(
    request_text: &str,
    active_tools: &[CachedTool],
    tool_registry: &ToolManifestStore,
    embedder: &E,
    capability_profile: Option<&aria_core::ModelCapabilityProfile>,
    budget: PromptBudget,
) -> Vec<CachedTool> {
    let compatible_active = active_tools
        .iter()
        .filter(|tool| aria_intelligence::tool_is_compatible_with_model(tool, capability_profile))
        .cloned()
        .collect::<Vec<_>>();
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    for tool in compatible_active.iter().rev().take(budget.tool_count / 2 + 1) {
        if seen.insert(tool.name.clone()) {
            selected.push(tool.clone());
        }
    }
    if let Ok(ranked) = tool_registry.search_with_explanations(
        request_text,
        embedder,
        budget.tool_count,
        capability_profile,
    ) {
        for entry in ranked {
            if !entry.visibility.available {
                continue;
            }
            if selected.len() >= budget.tool_count {
                break;
            }
            if seen.insert(entry.tool.name.clone()) {
                selected.push(entry.tool);
            }
        }
    }
    selected.truncate(budget.tool_count);
    selected
}

fn spawn_history_compaction(
    llm_pool: Arc<LlmBackendPool>,
    session_memory: aria_ssmu::SessionMemory,
    session_uuid: uuid::Uuid,
    sessions_dir: PathBuf,
    old_ctx: String,
    remove_count: usize,
    timestamp_us: u64,
) {
    let Some(compaction_claim) = try_mark_compaction_inflight(session_uuid, &sessions_dir) else {
        return;
    };
    let started_at_us = chrono::Utc::now().timestamp_micros() as u64;
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let previous_version = store
        .read_compaction_state(session_uuid)
        .map(|state| state.metadata.summary_version)
        .unwrap_or(0);
    let _ = store.upsert_compaction_state(
        &compaction_state_record(
            session_uuid,
            aria_core::CompactionStatus::Running,
            Some(started_at_us),
            None,
            None,
            previous_version,
            None,
        ),
        started_at_us,
    );
    tokio::spawn(async move {
        let prompt = PromptManager::build_summarization_prompt(&old_ctx);
        let store = RuntimeStore::for_sessions_dir(&sessions_dir);
        let finished_at_us = chrono::Utc::now().timestamp_micros() as u64;

        match llm_pool.query_with_fallback(&prompt, &[]).await {
            Ok(LLMResponse::TextAnswer(summary_res)) => {
                apply_compaction_result(
                    &session_memory,
                    session_uuid,
                    &summary_res,
                    remove_count,
                    timestamp_us,
                );
                let mut hasher = Sha256::new();
                hasher.update(summary_res.as_bytes());
                let summary_hash = hex::encode(hasher.finalize());
                let _ = store.upsert_compaction_state(
                    &compaction_state_record(
                        session_uuid,
                        aria_core::CompactionStatus::Succeeded,
                        Some(started_at_us),
                        Some(finished_at_us),
                        Some(summary_hash),
                        previous_version.saturating_add(1),
                        None,
                    ),
                    finished_at_us,
                );
            }
            Ok(other) => {
                let _ = store.upsert_compaction_state(
                    &compaction_state_record(
                        session_uuid,
                        aria_core::CompactionStatus::Failed,
                        Some(started_at_us),
                        Some(finished_at_us),
                        None,
                        previous_version,
                        Some(format!("unexpected compaction response: {:?}", other)),
                    ),
                    finished_at_us,
                );
            }
            Err(err) => {
                let _ = store.upsert_compaction_state(
                    &compaction_state_record(
                        session_uuid,
                        aria_core::CompactionStatus::Failed,
                        Some(started_at_us),
                        Some(finished_at_us),
                        None,
                        previous_version,
                        Some(err.to_string()),
                    ),
                    finished_at_us,
                );
            }
        }
        clear_compaction_inflight(session_uuid, &sessions_dir, &compaction_claim);
    });
}

async fn process_next_queued_agent_run<F, Fut>(
    sessions_dir: &Path,
    run_handler: F,
) -> Result<Option<AgentRunRecord>, String>
where
    F: FnOnce(AgentRunRecord) -> Fut,
    Fut: Future<Output = Result<String, String>>,
{
    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let started_at_us = chrono::Utc::now().timestamp_micros() as u64;
    let Some(mut run) = store.claim_next_queued_agent_run(started_at_us)? else {
        return Ok(None);
    };

    store.append_agent_run_event(&AgentRunEvent {
        event_id: format!("evt-{}", uuid::Uuid::new_v4()),
        run_id: run.run_id.clone(),
        kind: AgentRunEventKind::Started,
        summary: format!("started child agent '{}'", run.agent_id),
        created_at_us: started_at_us,
    })?;

    let timeout_seconds = run.max_runtime_seconds.unwrap_or(600);
    let timed_out_immediately = timeout_seconds == 0;
    let outcome = if timed_out_immediately {
        None
    } else {
        Some(
            tokio::time::timeout(
                Duration::from_secs(timeout_seconds as u64),
                run_handler(run.clone()),
            )
            .await,
        )
    };
    let finished_at_us = chrono::Utc::now().timestamp_micros() as u64;
    if let Ok(current) = store.read_agent_run(&run.run_id) {
        if current.status == AgentRunStatus::Cancelled {
            return Ok(Some(current));
        }
    }

    let (status, result, summary, mailbox_body) = match outcome {
        Some(Ok(Ok(response_summary))) => (
            AgentRunStatus::Completed,
            Some(aria_core::AgentRunResult {
                response_summary: Some(response_summary.clone()),
                error: None,
                completed_at_us: Some(finished_at_us),
            }),
            format!("child agent '{}' completed", run.agent_id),
            format!(
                "Sub-agent '{}' completed: {}",
                run.agent_id, response_summary
            ),
        ),
        Some(Ok(Err(error))) => (
            AgentRunStatus::Failed,
            Some(aria_core::AgentRunResult {
                response_summary: None,
                error: Some(error.clone()),
                completed_at_us: Some(finished_at_us),
            }),
            format!("child agent '{}' failed", run.agent_id),
            format!("Sub-agent '{}' failed: {}", run.agent_id, error),
        ),
        Some(Err(_)) | None => {
            let error = format!(
                "child agent '{}' exceeded runtime limit of {}s",
                run.agent_id, timeout_seconds
            );
            (
                AgentRunStatus::TimedOut,
                Some(aria_core::AgentRunResult {
                    response_summary: None,
                    error: Some(error.clone()),
                    completed_at_us: Some(finished_at_us),
                }),
                format!("child agent '{}' timed out", run.agent_id),
                format!("Sub-agent '{}' timed out: {}", run.agent_id, error),
            )
        }
    };

    run.status = status;
    run.finished_at_us = Some(finished_at_us);
    run.result = result;
    store.upsert_agent_run(&run, finished_at_us)?;
    store.append_agent_run_event(&AgentRunEvent {
        event_id: format!("evt-{}", uuid::Uuid::new_v4()),
        run_id: run.run_id.clone(),
        kind: match status {
            AgentRunStatus::Completed => AgentRunEventKind::Completed,
            AgentRunStatus::Failed => AgentRunEventKind::Failed,
            AgentRunStatus::Cancelled => AgentRunEventKind::Cancelled,
            AgentRunStatus::TimedOut => AgentRunEventKind::TimedOut,
            AgentRunStatus::Queued | AgentRunStatus::Running => AgentRunEventKind::Started,
        },
        summary,
        created_at_us: finished_at_us,
    })?;

    if run.inbox_on_completion {
        store.append_agent_mailbox_message(&AgentMailboxMessage {
            message_id: format!("msg-{}", uuid::Uuid::new_v4()),
            run_id: run.run_id.clone(),
            session_id: run.session_id,
            from_agent_id: Some(run.agent_id.clone()),
            to_agent_id: run.requested_by_agent.clone(),
            body: mailbox_body,
            created_at_us: finished_at_us,
            delivered: false,
        })?;
        store.append_agent_run_event(&AgentRunEvent {
            event_id: format!("evt-{}", uuid::Uuid::new_v4()),
            run_id: run.run_id.clone(),
            kind: AgentRunEventKind::InboxNotification,
            summary: "queued inbox notification for parent run".into(),
            created_at_us: finished_at_us,
        })?;
    }

    Ok(Some(run))
}

fn resolve_agent_for_request<E: EmbeddingModel>(
    req: &AgentRequest,
    router_index: &RouterIndex,
    embedder: &E,
    agent_store: &AgentConfigStore,
    session_memory: &aria_ssmu::SessionMemory,
) -> Result<AgentResolution, OrchestratorError> {
    let (override_agent, _) = get_effective_session_overrides(
        session_memory,
        req.session_id,
        req.channel,
        &req.user_id,
    )
        .map_err(OrchestratorError::ToolError)?;
    if let Some(agent) = normalize_override_value(override_agent) {
        return Ok(AgentResolution::Resolved(agent));
    }

    let request_text = request_text_from_content(&req.content);
    if let Some(agent) = heuristic_agent_override_for_request(&request_text, agent_store) {
        return Ok(AgentResolution::Resolved(agent));
    }
    match router_index.route_text(&request_text, embedder) {
        Ok(aria_intelligence::RouterDecision::Confident { agent_id, .. }) => {
            Ok(AgentResolution::Resolved(agent_id))
        }
        Ok(aria_intelligence::RouterDecision::NeedsLlmFallback { candidates }) => {
            if let Some(agent) = default_fallback_agent(agent_store) {
                Ok(AgentResolution::Resolved(agent))
            } else {
                Ok(AgentResolution::NeedsClarification(
                    build_agent_clarification(
                        &candidates.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
                    ),
                ))
            }
        }
        Err(aria_intelligence::RouterError::NoAgents)
        | Err(aria_intelligence::RouterError::NoRoutingCandidate) => {
            let candidates = agent_store
                .all()
                .map(|cfg| cfg.id.clone())
                .collect::<Vec<_>>();
            if candidates.len() == 1 {
                Ok(AgentResolution::Resolved(candidates[0].clone()))
            } else if let Some(agent) = default_fallback_agent(agent_store) {
                Ok(AgentResolution::Resolved(agent))
            } else {
                Ok(AgentResolution::NeedsClarification(
                    build_agent_clarification(&candidates),
                ))
            }
        }
        Err(e) => Err(OrchestratorError::ToolError(format!(
            "agent routing failed: {}",
            e
        ))),
    }
}

pub(crate) fn normalize_override_value(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(crate) fn stable_channel_user_session_uuid(
    channel: GatewayChannel,
    user_id: &str,
) -> uuid::Uuid {
    uuid::Uuid::from_bytes(aria_core::derive_scoped_session_id(
        uuid::Uuid::nil().into_bytes(),
        channel,
        user_id,
        aria_core::SessionScopePolicy::ChannelPeer,
    ))
}

pub(crate) fn get_effective_session_overrides(
    session_memory: &aria_ssmu::SessionMemory,
    session_id: [u8; 16],
    channel: GatewayChannel,
    user_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    let session_uuid = uuid::Uuid::from_bytes(session_id);
    let direct = session_memory.get_overrides(&session_uuid)?;
    if normalize_override_value(direct.0.clone()).is_some()
        || normalize_override_value(direct.1.clone()).is_some()
    {
        return Ok(direct);
    }

    let stable_uuid = stable_channel_user_session_uuid(channel, user_id);
    if stable_uuid == session_uuid {
        return Ok(direct);
    }

    session_memory.get_overrides(&stable_uuid)
}

pub(crate) fn persist_session_overrides(
    session_memory: &aria_ssmu::SessionMemory,
    session_id: [u8; 16],
    channel: GatewayChannel,
    user_id: &str,
    agent_override: Option<String>,
    model_override: Option<String>,
) -> Result<(), String> {
    let session_uuid = uuid::Uuid::from_bytes(session_id);
    session_memory.update_overrides(
        session_uuid,
        agent_override.clone(),
        model_override.clone(),
    )?;

    let stable_uuid = stable_channel_user_session_uuid(channel, user_id);
    if stable_uuid != session_uuid {
        session_memory.update_overrides(stable_uuid, agent_override, model_override)?;
    }

    Ok(())
}

fn default_fallback_agent(agent_store: &AgentConfigStore) -> Option<String> {
    if agent_store.get("omni").is_some() {
        Some("omni".to_string())
    } else {
        None
    }
}

fn heuristic_agent_override_for_request(
    request_text: &str,
    agent_store: &AgentConfigStore,
) -> Option<String> {
    let lower = request_text.to_ascii_lowercase();
    let browser_request = [
        "browser profile",
        "managed browser",
        "browser session",
        "browser screenshot",
        "browser extract",
        "browser snapshot",
        "browser open",
        "take a screenshot",
        "extract its text",
        "extract text from",
        "browser_login",
        "browser_profile_",
        "browser_session_",
        "chromium",
        "chrome profile",
    ]
    .iter()
    .any(|needle| lower.contains(needle));

    if browser_request && agent_store.get("omni").is_some() {
        return Some("omni".to_string());
    }

    let crawl_request = [
        "crawl ",
        "crawl_page",
        "crawl_site",
        "watch page",
        "watch site",
        "monitor page",
        "monitor site",
        "change detection",
        "website memory",
    ]
    .iter()
    .any(|needle| lower.contains(needle));

    if crawl_request && agent_store.get("researcher").is_some() {
        return Some("researcher".to_string());
    }

    let scheduling_request = request_is_scheduling_like(&lower);

    if scheduling_request && agent_store.get("productivity").is_some() {
        return Some("productivity".to_string());
    }

    None
}

fn contextual_runtime_tool_names_for_request(
    agent_id: &str,
    request_text: &str,
) -> Vec<&'static str> {
    let lower = request_text.to_ascii_lowercase();
    let mut tools = Vec::new();

    let browser_profile_request = [
        "browser profile",
        "managed browser profile",
        "create a browser profile",
        "browser session",
        "browser login",
        "chromium profile",
        "chrome profile",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if browser_profile_request && matches!(agent_id, "omni" | "developer") {
        tools.extend([
            "browser_profile_create",
            "browser_profile_list",
            "browser_profile_use",
            "browser_session_start",
        ]);
    }

    let browser_read_request = [
        "browser screenshot",
        "take a screenshot",
        "browser extract",
        "extract its text",
        "extract text from",
        "browser snapshot",
        "browser open",
        "open https://",
        "open http://",
        "browser_act",
        "click selector",
        "type into",
        "enter text",
        "select option",
        "scroll ",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if browser_read_request && matches!(agent_id, "omni" | "developer") {
        tools.extend([
            "browser_open",
            "browser_snapshot",
            "browser_screenshot",
            "browser_extract",
            "browser_act",
        ]);
    }

    let crawl_request = [
        "crawl ",
        "crawl_page",
        "crawl_site",
        "watch page",
        "watch site",
        "monitor page",
        "monitor site",
        "change detection",
        "website memory",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if crawl_request && matches!(agent_id, "researcher" | "omni" | "developer") {
        tools.extend([
            "crawl_page",
            "crawl_site",
            "watch_page",
            "watch_site",
            "list_watch_jobs",
        ]);
    }

    let scheduling_request = request_is_scheduling_like(&lower);
    if scheduling_request && matches!(agent_id, "productivity" | "omni" | "developer") {
        tools.extend(["set_reminder", "schedule_message", "manage_cron"]);
    }

    let mcp_request = [
        "mcp server",
        "register mcp",
        "import mcp",
        "bind mcp",
        "mcp import",
        "invoke mcp",
        "mcp tool",
        "mcp prompt",
        "mcp resource",
        "render mcp",
        "read mcp resource",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if mcp_request && matches!(agent_id, "omni" | "developer") {
        tools.extend([
            "register_mcp_server",
            "import_mcp_tool",
            "import_mcp_prompt",
            "import_mcp_resource",
            "bind_mcp_import",
            "invoke_mcp_tool",
            "render_mcp_prompt",
            "read_mcp_resource",
        ]);
    }

    tools
}

fn request_is_reminder_like(request_text: &str) -> bool {
    let lower = request_text.to_ascii_lowercase();
    lower.contains("remind") || lower.contains("reminder") || lower.contains("notify me")
}

fn request_is_browser_read_like(request_text: &str) -> bool {
    if request_is_browser_action_like(request_text) {
        return false;
    }
    let lower = request_text.to_ascii_lowercase();
    [
        "browser screenshot",
        "take a screenshot",
        "browser extract",
        "extract its text",
        "extract the text from",
        "extract text from",
        "browser snapshot",
        "browser open",
        "open https://",
        "open http://",
    ]
    .iter()
        .any(|needle| lower.contains(needle))
}

fn request_is_browser_action_like(request_text: &str) -> bool {
    let lower = request_text.to_ascii_lowercase();
    [
        "browser_act",
        "click selector",
        "click ",
        "type into",
        "enter text",
        "scroll ",
        "select option",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn request_is_mcp_operation_like(request_text: &str) -> bool {
    let lower = request_text.to_ascii_lowercase();
    [
        "mcp server",
        "register mcp",
        "import mcp",
        "bind mcp",
        "mcp import",
        "invoke mcp",
        "mcp tool",
        "mcp prompt",
        "mcp resource",
        "render mcp",
        "read mcp resource",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn browser_read_retry_policy(request_text: &str) -> aria_core::ToolRuntimePolicy {
    let lower = request_text.to_ascii_lowercase();
    let screenshot = lower.contains("screenshot");
    let extract = lower.contains("extract") || lower.contains("text from");
    let snapshot = lower.contains("snapshot");
    let open = lower.contains("browser open")
        || lower.contains("open https://")
        || lower.contains("open http://");

    let tool_choice = if screenshot && !(extract || snapshot || open) {
        aria_core::ToolChoicePolicy::Specific("browser_screenshot".into())
    } else if extract && !(screenshot || snapshot || open) {
        aria_core::ToolChoicePolicy::Specific("browser_extract".into())
    } else if snapshot && !(screenshot || extract || open) {
        aria_core::ToolChoicePolicy::Specific("browser_snapshot".into())
    } else if open && !(screenshot || extract || snapshot) {
        aria_core::ToolChoicePolicy::Specific("browser_open".into())
    } else {
        aria_core::ToolChoicePolicy::Required
    };

    aria_core::ToolRuntimePolicy {
        tool_choice,
        allow_parallel_tool_calls: false,
    }
}

fn extract_first_http_url_from_text(request_text: &str) -> Option<String> {
    request_text
        .split_whitespace()
        .map(|token| token.trim_matches(|c: char| "\"'()[]{}<>,.;".contains(c)))
        .find(|token| token.starts_with("http://") || token.starts_with("https://"))
        .map(|token| token.to_string())
}

fn extract_named_value(request_text: &str, field: &str) -> Option<String> {
    let lower = request_text.to_ascii_lowercase();
    let field_lower = field.to_ascii_lowercase();
    for pattern in [
        format!("{field_lower} "),
        format!("{field_lower}="),
        format!("{field_lower}:"),
    ] {
        if let Some(pos) = lower.find(&pattern) {
            let start = pos + pattern.len();
            let tail = &request_text[start..];
            let tail = tail.trim_start();
            if let Some(quoted) = tail.strip_prefix('"') {
                if let Some(end) = quoted.find('"') {
                    return Some(quoted[..end].trim().to_string());
                }
            }
            if let Some(quoted) = tail.strip_prefix('\'') {
                if let Some(end) = quoted.find('\'') {
                    return Some(quoted[..end].trim().to_string());
                }
            }
            let value = tail
                .split([',', '\n'])
                .next()
                .unwrap_or("")
                .trim()
                .trim_end_matches(|c: char| ".;)".contains(c))
                .trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn extract_after_phrase(request_text: &str, phrase: &str) -> Option<String> {
    let lower = request_text.to_ascii_lowercase();
    let phrase_lower = phrase.to_ascii_lowercase();
    let pos = lower.find(&phrase_lower)?;
    let tail = request_text[pos + phrase.len()..].trim_start();
    if let Some(quoted) = tail.strip_prefix('"') {
        if let Some(end) = quoted.find('"') {
            return Some(quoted[..end].trim().to_string());
        }
    }
    let value = tail
        .split([' ', ',', '\n'])
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches(|c: char| ".;)".contains(c))
        .trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn trim_balanced_wrapping_quotes(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0];
        let last = trimmed.as_bytes()[trimmed.len() - 1];
        if (first == b'"' && last == b'"')
            || (first == b'\'' && last == b'\'')
            || (first == b'`' && last == b'`')
        {
            return trimmed[1..trimmed.len() - 1].trim();
        }
    }
    trimmed
}

fn extract_first_path_from_text(request_text: &str) -> Option<String> {
    request_text
        .split_whitespace()
        .find(|segment| segment.starts_with('/') || segment.starts_with("./"))
        .map(|segment| {
            segment
                .trim_start_matches(|c: char| matches!(c, '"' | '\'' | '`' | '('))
                .trim_end_matches(|c: char| matches!(c, '"' | '\'' | '`' | ',' | '.' | ')'))
                .to_string()
        })
        .filter(|segment| segment.len() > 1)
}

fn extract_shell_command_from_text(request_text: &str) -> Option<String> {
    for phrase in ["execute ", "command ", "run_shell "] {
        if let Some(raw) = extract_after_phrase(request_text, phrase) {
            let trimmed = trim_balanced_wrapping_quotes(&raw).trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn extract_reminder_task_text(request_text: &str) -> Option<String> {
    let lower = request_text.to_ascii_lowercase();
    for phrase in ["to say ", "to remind me ", "to "] {
        if let Some(pos) = lower.find(phrase) {
            let tail = request_text[pos + phrase.len()..].trim();
            let trimmed = trim_balanced_wrapping_quotes(tail).trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    infer_deferred_task(request_text).filter(|value| !value.is_empty())
}

fn infer_browser_profile_id_from_request(request_text: &str) -> Option<String> {
    extract_after_phrase(request_text, "profile_id ")
        .or_else(|| extract_named_value(request_text, "profile_id"))
        .or_else(|| extract_after_phrase(request_text, "profile "))
        .or_else(|| extract_after_phrase(request_text, "id "))
        .map(|value| trim_balanced_wrapping_quotes(&value).to_string())
        .filter(|value| !value.is_empty())
}

fn infer_browser_session_id_from_request(request_text: &str) -> Option<String> {
    extract_after_phrase(request_text, "browser_session_id ")
        .or_else(|| extract_named_value(request_text, "browser_session_id"))
        .or_else(|| extract_after_phrase(request_text, "session "))
        .map(|value| trim_balanced_wrapping_quotes(&value).to_string())
        .filter(|value| !value.is_empty())
}

fn infer_domain_from_request(request_text: &str) -> Option<String> {
    extract_named_value(request_text, "domain")
        .or_else(|| extract_after_phrase(request_text, "domain "))
        .or_else(|| {
            let lower = request_text.to_ascii_lowercase();
            lower.find(" for ").and_then(|pos| {
                let tail = request_text[pos + 5..].trim_start();
                let candidate = tail
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches(|c: char| "\"'()[]{}<>,.;".contains(c))
                    .trim_end_matches('/');
                if candidate.contains('.')
                    && !candidate.contains("://")
                    && candidate
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-')
                {
                    Some(candidate.to_string())
                } else {
                    None
                }
            })
        })
        .or_else(|| {
            extract_first_http_url_from_text(request_text).and_then(|url| {
                reqwest::Url::parse(&url)
                    .ok()
                    .and_then(|parsed| parsed.host_str().map(str::to_string))
            })
        })
        .map(|value| trim_balanced_wrapping_quotes(&value).to_string())
        .filter(|value| !value.is_empty())
}

fn heuristic_specific_tool_call(
    request_text: &str,
    tool_name: &str,
    scheduling_intent: Option<&SchedulingIntent>,
) -> Option<ToolCall> {
    match tool_name {
        "write_file" => {
            let path = extract_first_path_from_text(request_text)
                .or_else(|| extract_named_value(request_text, "path"))?;
            let content = extract_after_phrase(request_text, "exact content ")
                .or_else(|| extract_after_phrase(request_text, "content "))
                .map(|value| trim_balanced_wrapping_quotes(&value).to_string())?;
            Some(ToolCall {
                invocation_id: None,
                name: "write_file".into(),
                arguments: serde_json::json!({
                    "path": path,
                    "content": content,
                })
                .to_string(),
            })
        }
        "read_file" => {
            let path = extract_first_path_from_text(request_text)
                .or_else(|| extract_named_value(request_text, "path"))?;
            Some(ToolCall {
                invocation_id: None,
                name: "read_file".into(),
                arguments: serde_json::json!({ "path": path }).to_string(),
            })
        }
        "run_shell" => {
            let command = extract_named_value(request_text, "command")
                .or_else(|| extract_shell_command_from_text(request_text))?;
            Some(ToolCall {
                invocation_id: None,
                name: "run_shell".into(),
                arguments: serde_json::json!({ "command": command }).to_string(),
            })
        }
        "fetch_url" | "web_fetch" | "web_extract" => {
            let url = extract_first_http_url_from_text(request_text)?;
            Some(ToolCall {
                invocation_id: None,
                name: tool_name.to_string(),
                arguments: serde_json::json!({ "url": url }).to_string(),
            })
        }
        "set_reminder" | "schedule_message" => {
            let intent = scheduling_intent?;
            let schedule = intent.normalized_schedule.clone()?;
            let task = extract_reminder_task_text(request_text)?;
            Some(ToolCall {
                invocation_id: None,
                name: tool_name.to_string(),
                arguments: serde_json::json!({
                    "task": task,
                    "schedule": schedule,
                    "mode": intent.mode.as_tool_mode(),
                })
                .to_string(),
            })
        }
        "spawn_agent" => {
            let agent_id = extract_named_value(request_text, "agent_id")
                .or_else(|| extract_after_phrase(request_text, "agent "))
                .map(|value| trim_balanced_wrapping_quotes(&value).to_string())?;
            let prompt = extract_named_value(request_text, "prompt")
                .or_else(|| extract_after_phrase(request_text, "prompt "))
                .or_else(|| extract_after_phrase(request_text, "to "))
                .map(|value| trim_balanced_wrapping_quotes(&value).to_string())?;
            let max_runtime_seconds = extract_named_value(request_text, "max_runtime_seconds")
                .and_then(|value| value.parse::<u32>().ok());
            let mut args = serde_json::Map::new();
            args.insert("agent_id".into(), serde_json::Value::String(agent_id));
            args.insert("prompt".into(), serde_json::Value::String(prompt));
            if let Some(max_runtime_seconds) = max_runtime_seconds {
                args.insert(
                    "max_runtime_seconds".into(),
                    serde_json::Value::Number(max_runtime_seconds.into()),
                );
            }
            Some(ToolCall {
                invocation_id: None,
                name: "spawn_agent".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        "browser_profile_create" => {
            let profile_id = infer_browser_profile_id_from_request(request_text)
                .unwrap_or_else(|| "default-browser".to_string());
            let display_name = extract_named_value(request_text, "display_name")
                .or_else(|| extract_named_value(request_text, "name"))
                .map(|value| trim_balanced_wrapping_quotes(&value).to_string())
                .unwrap_or_else(|| profile_id.clone());
            let lower = request_text.to_ascii_lowercase();
            let set_as_default = lower.contains("default");
            let auth_enabled = lower.contains("auth enabled")
                || lower.contains("authenticated")
                || lower.contains("login enabled");
            let write_enabled = lower.contains("write enabled")
                || lower.contains("write access")
                || lower.contains("interactive write");
            Some(ToolCall {
                invocation_id: None,
                name: "browser_profile_create".into(),
                arguments: serde_json::json!({
                    "profile_id": profile_id,
                    "display_name": display_name,
                    "set_as_default": set_as_default,
                    "auth_enabled": auth_enabled,
                    "write_enabled": write_enabled,
                })
                .to_string(),
            })
        }
        "browser_profile_use" => {
            let profile_id = infer_browser_profile_id_from_request(request_text)?;
            Some(ToolCall {
                invocation_id: None,
                name: "browser_profile_use".into(),
                arguments: serde_json::json!({ "profile_id": profile_id }).to_string(),
            })
        }
        "browser_profile_list" | "browser_session_list" | "list_agent_runs" => Some(ToolCall {
            invocation_id: None,
            name: tool_name.to_string(),
            arguments: "{}".into(),
        }),
        "get_agent_run" | "get_agent_run_events" | "get_agent_mailbox" => {
            let run_id = extract_named_value(request_text, "run_id")
                .or_else(|| extract_after_phrase(request_text, "run "))?;
            Some(ToolCall {
                invocation_id: None,
                name: tool_name.to_string(),
                arguments: serde_json::json!({ "run_id": run_id }).to_string(),
            })
        }
        "browser_open" | "browser_snapshot" | "browser_screenshot" | "browser_extract" => {
            heuristic_browser_read_tool_call(request_text).and_then(|call| {
                if call.name == tool_name {
                    Some(call)
                } else {
                    None
                }
            })
        }
        "browser_act" => heuristic_browser_action_tool_call(request_text),
        "browser_session_start" => {
            let profile_id = infer_browser_profile_id_from_request(request_text);
            let url = extract_first_http_url_from_text(request_text);
            let mut args = serde_json::Map::new();
            if let Some(profile_id) = profile_id {
                args.insert("profile_id".into(), serde_json::Value::String(profile_id));
            }
            if let Some(url) = url {
                args.insert("url".into(), serde_json::Value::String(url));
            }
            Some(ToolCall {
                invocation_id: None,
                name: "browser_session_start".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        "browser_session_status" => {
            let mut args = serde_json::Map::new();
            if let Some(browser_session_id) = infer_browser_session_id_from_request(request_text) {
                args.insert(
                    "browser_session_id".into(),
                    serde_json::Value::String(browser_session_id),
                );
            }
            Some(ToolCall {
                invocation_id: None,
                name: "browser_session_status".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        "browser_login_begin_manual" => {
            let domain = infer_domain_from_request(request_text)?;
            let mut args = serde_json::Map::new();
            if let Some(browser_session_id) = infer_browser_session_id_from_request(request_text) {
                args.insert(
                    "browser_session_id".into(),
                    serde_json::Value::String(browser_session_id),
                );
            }
            args.insert("domain".into(), serde_json::Value::String(domain));
            Some(ToolCall {
                invocation_id: None,
                name: "browser_login_begin_manual".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        "browser_login_complete_manual" => {
            let domain = infer_domain_from_request(request_text)?;
            let mut args = serde_json::Map::new();
            if let Some(browser_session_id) = infer_browser_session_id_from_request(request_text) {
                args.insert(
                    "browser_session_id".into(),
                    serde_json::Value::String(browser_session_id),
                );
            }
            args.insert("domain".into(), serde_json::Value::String(domain));
            Some(ToolCall {
                invocation_id: None,
                name: "browser_login_complete_manual".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        "browser_login_status" => {
            let mut args = serde_json::Map::new();
            if let Some(browser_session_id) = infer_browser_session_id_from_request(request_text) {
                args.insert(
                    "browser_session_id".into(),
                    serde_json::Value::String(browser_session_id),
                );
            }
            if let Some(domain) = infer_domain_from_request(request_text) {
                args.insert("domain".into(), serde_json::Value::String(domain));
            }
            Some(ToolCall {
                invocation_id: None,
                name: "browser_login_status".into(),
                arguments: serde_json::Value::Object(args).to_string(),
            })
        }
        _ => None,
    }
}

fn heuristic_browser_read_tool_call(request_text: &str) -> Option<ToolCall> {
    if !request_is_browser_read_like(request_text) {
        return None;
    }

    let lower = request_text.to_ascii_lowercase();
    let screenshot = lower.contains("screenshot");
    let extract = lower.contains("extract") || lower.contains("text from");
    let snapshot = lower.contains("snapshot");
    let open = lower.contains("browser open")
        || lower.contains("open https://")
        || lower.contains("open http://");

    let selected = match (screenshot, extract, snapshot, open) {
        (true, false, false, false) => "browser_screenshot",
        (false, true, false, false) => "browser_extract",
        (false, false, true, false) => "browser_snapshot",
        (false, false, false, true) => "browser_open",
        _ => return None,
    };

    let mut arguments = serde_json::Map::new();
    if let Some(url) = extract_first_http_url_from_text(request_text) {
        arguments.insert("url".into(), serde_json::Value::String(url));
    }

    Some(ToolCall {
        invocation_id: None,
        name: selected.into(),
        arguments: serde_json::Value::Object(arguments).to_string(),
    })
}

fn heuristic_browser_action_tool_call(request_text: &str) -> Option<ToolCall> {
    if !request_is_browser_action_like(request_text) {
        return None;
    }
    let lower = request_text.to_ascii_lowercase();
    let action = if lower.contains("select option")
        || lower.contains(" action \"select\"")
        || lower.contains(" action 'select'")
        || lower.contains("action: select")
    {
        "select"
    } else if lower.contains("scroll") {
        "scroll"
    } else if lower.contains("type") || lower.contains("enter text") {
        "type"
    } else if lower.contains("click") {
        "click"
    } else {
        return None;
    };
    let mut arguments = serde_json::Map::new();
    arguments.insert("action".into(), serde_json::Value::String(action.into()));
    let selector =
        extract_named_value(request_text, "selector").or_else(|| extract_after_phrase(request_text, "selector "));
    if let Some(selector) = selector {
        arguments.insert("selector".into(), serde_json::Value::String(selector));
    }
    if action == "type" {
        let text =
            extract_named_value(request_text, "text").or_else(|| extract_after_phrase(request_text, "text "));
        if let Some(text) = text {
            arguments.insert("text".into(), serde_json::Value::String(text));
        }
    }
    Some(ToolCall {
        invocation_id: None,
        name: "browser_act".into(),
        arguments: serde_json::Value::Object(arguments).to_string(),
    })
}

fn heuristic_mcp_tool_call(request_text: &str) -> Option<ToolCall> {
    if !request_is_mcp_operation_like(request_text) {
        return None;
    }

    let lower = request_text.to_ascii_lowercase();
    let mut arguments = serde_json::Map::new();

    if lower.contains("register") && lower.contains("mcp server") {
        arguments.insert(
            "server_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "server_id")?),
        );
        arguments.insert(
            "display_name".into(),
            serde_json::Value::String(extract_named_value(request_text, "display_name")?),
        );
        arguments.insert(
            "transport".into(),
            serde_json::Value::String(extract_named_value(request_text, "transport")?),
        );
        arguments.insert(
            "endpoint".into(),
            serde_json::Value::String(extract_named_value(request_text, "endpoint")?),
        );
        let enabled = extract_named_value(request_text, "enabled")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(true);
        arguments.insert("enabled".into(), serde_json::Value::Bool(enabled));
        return Some(ToolCall {
            invocation_id: None,
            name: "register_mcp_server".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("import") && lower.contains("mcp tool") {
        arguments.insert(
            "import_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "import_id")?),
        );
        arguments.insert(
            "server_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "server_id")?),
        );
        arguments.insert(
            "tool_name".into(),
            serde_json::Value::String(extract_named_value(request_text, "tool_name")?),
        );
        arguments.insert(
            "description".into(),
            serde_json::Value::String(extract_named_value(request_text, "description")?),
        );
        arguments.insert(
            "parameters_schema".into(),
            serde_json::Value::String(
                extract_named_value(request_text, "parameters_schema")
                    .unwrap_or_else(|| "{}".to_string()),
            ),
        );
        return Some(ToolCall {
            invocation_id: None,
            name: "import_mcp_tool".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("import") && lower.contains("mcp prompt") {
        arguments.insert(
            "import_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "import_id")?),
        );
        arguments.insert(
            "server_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "server_id")?),
        );
        arguments.insert(
            "prompt_name".into(),
            serde_json::Value::String(extract_named_value(request_text, "prompt_name")?),
        );
        arguments.insert(
            "description".into(),
            serde_json::Value::String(extract_named_value(request_text, "description")?),
        );
        if let Some(schema) = extract_named_value(request_text, "arguments_schema") {
            arguments.insert("arguments_schema".into(), serde_json::Value::String(schema));
        }
        return Some(ToolCall {
            invocation_id: None,
            name: "import_mcp_prompt".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("import") && lower.contains("mcp resource") {
        arguments.insert(
            "import_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "import_id")?),
        );
        arguments.insert(
            "server_id".into(),
            serde_json::Value::String(extract_named_value(request_text, "server_id")?),
        );
        arguments.insert(
            "resource_uri".into(),
            serde_json::Value::String(extract_named_value(request_text, "resource_uri")?),
        );
        arguments.insert(
            "description".into(),
            serde_json::Value::String(extract_named_value(request_text, "description")?),
        );
        if let Some(mime) = extract_named_value(request_text, "mime_type") {
            arguments.insert("mime_type".into(), serde_json::Value::String(mime));
        }
        return Some(ToolCall {
            invocation_id: None,
            name: "import_mcp_resource".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("bind") && lower.contains("mcp") {
        let primitive_kind = if lower.contains("primitive_kind") {
            extract_named_value(request_text, "primitive_kind")?
        } else if lower.contains(" tool") {
            "tool".to_string()
        } else if lower.contains(" prompt") {
            "prompt".to_string()
        } else if lower.contains(" resource") {
            "resource".to_string()
        } else {
            return None;
        };
        let target_name = extract_named_value(request_text, "target_name").or_else(|| {
            match primitive_kind.as_str() {
                "tool" => extract_after_phrase(request_text, "tool "),
                "prompt" => extract_after_phrase(request_text, "prompt "),
                "resource" => extract_after_phrase(request_text, "resource "),
                _ => None,
            }
        })?;
        let server_id = extract_named_value(request_text, "server_id")
            .or_else(|| extract_after_phrase(request_text, "server "))?;
        arguments.insert("server_id".into(), serde_json::Value::String(server_id));
        arguments.insert(
            "primitive_kind".into(),
            serde_json::Value::String(primitive_kind),
        );
        arguments.insert("target_name".into(), serde_json::Value::String(target_name));
        return Some(ToolCall {
            invocation_id: None,
            name: "bind_mcp_import".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("invoke") && lower.contains("mcp tool") {
        let tool_name = extract_named_value(request_text, "tool_name")
            .or_else(|| extract_after_phrase(request_text, "tool "))?;
        let server_id = extract_named_value(request_text, "server_id")
            .or_else(|| extract_after_phrase(request_text, "server "))?;
        arguments.insert("server_id".into(), serde_json::Value::String(server_id));
        arguments.insert("tool_name".into(), serde_json::Value::String(tool_name));
        arguments.insert("input".into(), serde_json::json!({}));
        return Some(ToolCall {
            invocation_id: None,
            name: "invoke_mcp_tool".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("render") && lower.contains("mcp prompt") {
        let prompt_name = extract_named_value(request_text, "prompt_name")
            .or_else(|| extract_after_phrase(request_text, "prompt "))?;
        let server_id = extract_named_value(request_text, "server_id")
            .or_else(|| extract_after_phrase(request_text, "server "))?;
        arguments.insert("server_id".into(), serde_json::Value::String(server_id));
        arguments.insert("prompt_name".into(), serde_json::Value::String(prompt_name));
        arguments.insert("arguments".into(), serde_json::json!({}));
        return Some(ToolCall {
            invocation_id: None,
            name: "render_mcp_prompt".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    if lower.contains("read") && lower.contains("mcp resource") {
        let resource_uri = extract_named_value(request_text, "resource_uri")
            .or_else(|| extract_after_phrase(request_text, "resource "))?;
        let server_id = extract_named_value(request_text, "server_id")
            .or_else(|| extract_after_phrase(request_text, "server "))?;
        arguments.insert("server_id".into(), serde_json::Value::String(server_id));
        arguments.insert("resource_uri".into(), serde_json::Value::String(resource_uri));
        return Some(ToolCall {
            invocation_id: None,
            name: "read_mcp_resource".into(),
            arguments: serde_json::Value::Object(arguments).to_string(),
        });
    }

    None
}

fn request_is_scheduling_like(request_text: &str) -> bool {
    let lower = request_text.to_ascii_lowercase();
    [
        "remind me",
        "set a reminder",
        "schedule a reminder",
        "schedule this",
        "reminder",
        "daily at",
        "weekly at",
        "every day",
        "every week",
        "cron",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn effective_tool_runtime_policy_for_request(
    req: &AgentRequest,
    request_text: &str,
    scheduling_intent: Option<&SchedulingIntent>,
) -> Option<aria_core::ToolRuntimePolicy> {
    if let Some(policy) = req.tool_runtime_policy.clone() {
        return Some(policy);
    }
    if request_is_browser_action_like(request_text) {
        return Some(aria_core::ToolRuntimePolicy {
            tool_choice: aria_core::ToolChoicePolicy::Specific("browser_act".to_string()),
            allow_parallel_tool_calls: false,
        });
    }
    if request_is_browser_read_like(request_text) {
        return Some(browser_read_retry_policy(request_text));
    }
    scheduling_intent.map(|_| aria_core::ToolRuntimePolicy {
        tool_choice: aria_core::ToolChoicePolicy::Required,
        allow_parallel_tool_calls: false,
    })
}

async fn process_request(
    req: &AgentRequest,
    learning: &LearningConfig,
    router_index: &RouterIndex,
    embedder: &impl EmbeddingModel,
    llm_pool: &Arc<LlmBackendPool>,
    cedar: &Arc<aria_policy::CedarEvaluator>,
    agent_store: &AgentConfigStore,
    tool_registry: &ToolManifestStore,
    session_memory: &aria_ssmu::SessionMemory,
    page_index: &Arc<PageIndexTree>,
    vector_store: &Arc<VectorStore>,
    keyword_index: &Arc<KeywordIndex>,
    firewall: &aria_safety::DfaFirewall,
    vault: &Arc<aria_vault::CredentialVault>,
    tx_cron: &tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
    provider_registry: &Arc<tokio::sync::Mutex<ProviderRegistry>>,
    session_tool_caches: &SessionToolCacheStore,
    _hooks: &HookRegistry,
    session_locks: &dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    _embed_semaphore: &Arc<tokio::sync::Semaphore>,
    max_rounds: usize,
    steering_rx: Option<&mut tokio::sync::mpsc::Receiver<aria_intelligence::SteeringCommand>>,
    global_estop: Option<&Arc<std::sync::atomic::AtomicBool>>,
    sessions_dir: &std::path::Path,
    whitelist: Vec<String>,
    forbid: Vec<String>,
    user_timezone: chrono_tz::Tz,
) -> Result<aria_intelligence::OrchestratorResult, OrchestratorError> {
    let _started = std::time::Instant::now();
    let _admission_permit = app_runtime()
        .global_request_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| OrchestratorError::BackendOverloaded("system busy: global admission cap reached".into()))?;
    let request_text = request_text_from_content(&req.content);

    let session_uuid = uuid::Uuid::from_bytes(req.session_id);
    let scheduling_intent = classify_scheduling_intent(
        &request_text,
        chrono::Utc::now().with_timezone(&user_timezone),
    );
    if let Some(intent) = &scheduling_intent {
        debug!(
            mode = %intent.mode.as_tool_mode(),
            schedule = ?intent.normalized_schedule,
            deferred_task = ?intent.deferred_task,
            rationale = %intent.rationale,
            "Scheduling intent classified"
        );
    }
    debug!(
        session_id = %session_uuid,
        user_id = %req.user_id,
        channel = ?req.channel,
        request_text = %request_text,
        "Request received"
    );

    match firewall.scan_ingress(&request_text) {
        aria_safety::ScanResult::Alert(alerts) => {
            warn!(session_id = %session_uuid, alerts = ?alerts, "DfaFirewall blocked ingress payload");
            return Err(OrchestratorError::SecurityViolation(format!(
                "Blocked bad patterns: {:?}",
                alerts
            )));
        }
        aria_safety::ScanResult::Clean => {}
    }

    let session_uuid_str = session_uuid.to_string();
    let session_mutex = session_locks
        .entry(session_uuid_str.clone())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _session_guard = session_mutex.lock().await;

    let (_override_agent, _override_model) = session_memory
        .get_overrides(&session_uuid)
        .unwrap_or((None, None));
    let agent = match resolve_agent_for_request(
        req,
        router_index,
        embedder,
        agent_store,
        session_memory,
    )? {
        AgentResolution::Resolved(agent) => {
            info!(agent = %agent, "Resolved request agent");
            agent
        }
        AgentResolution::NeedsClarification(question) => {
            append_request_policy_audit(sessions_dir, req, None);
            let user_msg = aria_ssmu::Message {
                role: "user".into(),
                content: request_text.clone(),
                timestamp_us: req.timestamp_us,
            };
            let assistant_msg = aria_ssmu::Message {
                role: "assistant".into(),
                content: question.clone(),
                timestamp_us: req.timestamp_us,
            };
            let _ = session_memory.append(session_uuid, user_msg.clone());
            let _ = session_memory.append_audit_event(sessions_dir, &session_uuid, &user_msg);
            let _ = session_memory.append(session_uuid, assistant_msg.clone());
            let _ = session_memory.append_audit_event(sessions_dir, &session_uuid, &assistant_msg);
            let result = aria_intelligence::OrchestratorResult::Completed(question);
            record_learning_reward(
                learning,
                sessions_dir,
                req.request_id,
                req.session_id,
                RewardKind::Edited,
                Some("clarification required before execution".to_string()),
                req.timestamp_us,
            );
            persist_learning_trace(
                learning,
                sessions_dir,
                req,
                "__clarification__",
                "clarification",
                &request_text,
                &[],
                &result,
                "",
                req.timestamp_us,
            );
            return Ok(result);
        }
    };

    let effective_tool_runtime_policy =
        effective_tool_runtime_policy_for_request(req, &request_text, scheduling_intent.as_ref());
    let effective_req = if effective_tool_runtime_policy != req.tool_runtime_policy {
        let mut adjusted = req.clone();
        adjusted.tool_runtime_policy = effective_tool_runtime_policy;
        adjusted
    } else {
        req.clone()
    };

    append_request_policy_audit(sessions_dir, &effective_req, Some(&agent));

    if let Some(agent_cfg) = agent_store.get(&agent) {
        if agent_cfg.requires_elevation
            && !has_active_elevation_grant(
                sessions_dir,
                session_uuid,
                &req.user_id,
                &agent,
                chrono::Utc::now().timestamp_micros() as u64,
            )
        {
            let result = aria_intelligence::OrchestratorResult::AgentElevationRequired {
                agent_id: agent.clone(),
                message: build_agent_elevation_message(&agent),
            };
            let prompt_mode = learning_prompt_mode_label(
                &effective_req,
                &request_text,
                None,
                scheduling_intent.as_ref(),
            );
            persist_learning_trace(
                learning,
                sessions_dir,
                &effective_req,
                &agent,
                &prompt_mode,
                &request_text,
                &[],
                &result,
                "",
                req.timestamp_us,
            );
            record_learning_reward(
                learning,
                sessions_dir,
                effective_req.request_id,
                effective_req.session_id,
                RewardKind::Edited,
                Some(format!("elevation required for agent {}", agent)),
                effective_req.timestamp_us,
            );
            return Ok(result);
        }
    }

    info!(agent = %agent, "Routed to agent");
    let capability_profile = agent_store
        .get(&agent)
        .and_then(capability_profile_from_agent_config);
    let global_limit = crate::runtime_env().global_request_concurrency_limit.max(1);
    let fairness_holder_id = format!(
        "request:{}:{}",
        crate::runtime_instance_id(),
        uuid::Uuid::from_bytes(effective_req.request_id)
    );
    let _channel_quota_claim = acquire_shared_quota_claim(
        sessions_dir,
        &format!("channel:{:?}", effective_req.channel),
        shared_channel_quota_limit(global_limit),
        &fairness_holder_id,
        60,
    )
    .await?;
    let agent_class = agent_store
        .get(&agent)
        .map(|cfg| cfg.class)
        .unwrap_or(aria_core::AgentClass::Generalist);
    let _agent_class_quota_claim = acquire_shared_quota_claim(
        sessions_dir,
        &format!("agent-class:{:?}", agent_class),
        shared_agent_class_quota_limit(global_limit, agent_class),
        &fairness_holder_id,
        60,
    )
    .await?;
    let _user_quota_claim = acquire_shared_quota_claim(
        sessions_dir,
        &format!("user:{}", effective_req.user_id),
        shared_user_quota_limit(global_limit),
        &fairness_holder_id,
        60,
    )
    .await?;

    let build_policy_executor = || {
        PolicyCheckedExecutor::new(
            MultiplexToolExecutor::new(
                vault.clone(),
                agent.clone(),
                *session_uuid.as_bytes(),
                effective_req.user_id.clone(),
                effective_req.channel,
                tx_cron.clone(),
                session_memory.clone(),
                cedar.clone(),
                sessions_dir.to_path_buf(),
                capability_profile.clone(),
                scheduling_intent.clone(),
                user_timezone,
            ),
            cedar.clone(),
            agent.clone(),
            effective_req.channel,
            whitelist.clone(),
            forbid.clone(),
            capability_profile.clone(),
            Some(sessions_dir.to_path_buf()),
            Some(effective_req.session_id),
        )
        .with_firewall(firewall.clone())
    };
    let policy_executor = build_policy_executor();
    let mut override_backend: Option<Arc<dyn LLMBackend>> = None;
    if let Some(combined) = _override_model {
        if let Some((pid, mid)) = combined.split_once(':') {
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            if let Some(profile) = resolve_model_capability_profile(
                provider_registry,
                sessions_dir,
                None,
                pid,
                mid,
                now_us,
            )
            .await
            {
                let reg = provider_registry.lock().await;
                if let Ok(b) = reg.create_backend_with_profile(&profile) {
                    override_backend = Some(Arc::from(b));
                    info!(provider = %pid, model = %mid, "Using session model override");
                }
            } else {
                let reg = provider_registry.lock().await;
                if let Ok(b) = reg.create_backend(pid, mid) {
                    override_backend = Some(Arc::from(b));
                    info!(provider = %pid, model = %mid, "Using session model override");
                }
            }
        }
    }

    let active_model_capability = override_backend
        .as_ref()
        .and_then(|backend| backend.capability_profile())
        .or_else(|| llm_pool.primary_capability_profile());
    let allow_repair_fallback = repair_fallback_allowed(
        &current_repair_fallback_allowlist(),
        active_model_capability.as_ref(),
    );

    let executed_tool_names = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let orchestrator = AgentOrchestrator::new(
        PoolBackedLLM::new(llm_pool.clone(), override_backend),
        RecordingToolExecutor::new(policy_executor, Arc::clone(&executed_tool_names)),
    )
    .with_repair_fallback(allow_repair_fallback)
        .with_event_sink(Arc::new(RepairFallbackAuditSink {
        sessions_dir: sessions_dir.to_path_buf(),
        request_id: uuid::Uuid::from_bytes(effective_req.request_id).to_string(),
        session_id: session_uuid.to_string(),
        user_id: effective_req.user_id.clone(),
        agent_id: agent.clone(),
        provider_id: active_model_capability
            .as_ref()
            .map(|profile| profile.model_ref.provider_id.clone()),
        model_id: active_model_capability
            .as_ref()
            .map(|profile| profile.model_ref.model_id.clone()),
        created_at_us: effective_req.timestamp_us,
    }));
    let (max_rounds, context_cap, session_ceiling, base_tool_names, system_prompt, trust_profile) =
        agent_store
            .get(&agent)
            .map(|cfg| {
                (
                    cfg.max_tool_rounds,
                    cfg.context_cap,
                    cfg.session_tool_ceiling,
                    cfg.base_tool_names.clone(),
                    cfg.system_prompt.clone(),
                    cfg.trust_profile.clone(),
                )
            })
            .unwrap_or((
                max_rounds,
                8,
                15,
                Vec::new(),
                "You are a helpful AI assistant.".to_string(),
                None,
            ));
    let prompt_budget = PromptBudget {
        tool_count: context_cap.clamp(1, 8),
        ..PromptBudget::default()
    };

    let cache_key = (effective_req.session_id, agent.clone());
    let cache_handle = session_tool_caches.get_or_insert_with(cache_key.clone(), || {
        debug!(
            session_id = %session_uuid,
            context_cap,
            session_ceiling,
            "DynamicToolCache: new session cache"
        );
        DynamicToolCache::new(context_cap, session_ceiling)
    });
    let mut cache = cache_handle.lock().await;
    let is_new_cache = cache.total_seen() == 0;
    if is_new_cache {
        debug!(
            agent = %agent,
            base_tools = ?base_tool_names,
            "DynamicToolCache: injecting base tools + search_tool_registry"
        );
        for tool_name in &base_tool_names {
            if !runtime_exposes_base_tool(tool_name) {
                warn!(tool = %tool_name, agent = %agent, "Skipping unavailable base tool");
                continue;
            }
            let tool = if let Some(t) = tool_registry.get_by_name(tool_name) {
                t
            } else {
                CachedTool {
                    name: tool_name.clone(),
                    description: format!("Base tool '{}'", tool_name),
                    parameters_schema: "{}".into(),
                    embedding: Vec::new(),
                    requires_strict_schema: false,
                    streaming_safe: false,
                    parallel_safe: true,
                    modalities: vec![aria_core::ToolModality::Text],
                }
            };
            let _ = cache.insert(tool);
        }
        let _ = cache.insert(CachedTool {
            name: "search_tool_registry".into(),
            description: "Search tool registry and inject best tool.".into(),
            parameters_schema: r#"{"query":"string"}"#.into(),
            embedding: Vec::new(),
            requires_strict_schema: false,
            streaming_safe: false,
            parallel_safe: true,
            modalities: vec![aria_core::ToolModality::Text],
        });
    }
    for tool_name in contextual_runtime_tool_names_for_request(&agent, &request_text) {
        let tool = if let Some(tool) = tool_registry.get_by_name(tool_name) {
            tool
        } else {
            CachedTool {
                name: tool_name.to_string(),
                description: format!("Runtime tool '{}'", tool_name),
                parameters_schema: "{}".into(),
                embedding: Vec::new(),
                requires_strict_schema: false,
                streaming_safe: false,
                parallel_safe: true,
                modalities: vec![aria_core::ToolModality::Text],
            }
        };
        let _ = cache.insert(tool);
    }
    let _ = RuntimeStore::for_sessions_dir(&sessions_dir).upsert_cache_snapshot(
        session_uuid,
        &agent,
        &cache.active_tools(),
        chrono::Utc::now().timestamp_micros() as u64,
    );

    let mut history_ctx = String::new();
    let mut durable_constraints_ctx = String::new();

    if let Ok(constraints) = session_memory.get_durable_constraints(&session_uuid) {
        if !constraints.is_empty() {
            durable_constraints_ctx = format!(
                "\n<durable_constraints>\n{}\n</durable_constraints>\n",
                constraints.join("\n")
            );
        }
    }

    if let Ok(hist) = session_memory.get_history(&session_uuid) {
        let hist_len = hist.len();

        // Token-aware auto-compaction trigger
        let mut total_tokens = 0;
        for m in &hist {
            // Rough approximation: 1 token ≈ 4 chars or 1 token ≈ 1 word
            total_tokens += m.content.split_whitespace().count();
        }

        let start_idx;
        let compaction_state = RuntimeStore::for_sessions_dir(&sessions_dir)
            .read_compaction_state(session_uuid)
            .ok();
        if should_trigger_compaction(
            compaction_state.as_ref(),
            hist_len,
            total_tokens,
            effective_req.timestamp_us,
        ) {
            // We need to compact. Determine how many turns to keep (say, the last 2 turns).
            let keep_turns = 2;
            let remove_count = hist_len.saturating_sub(keep_turns);
            start_idx = remove_count;

            // Extract the old turns for summarization
            let old_turns: Vec<String> = hist
                .iter()
                .take(remove_count)
                .map(|m| format!("{}: {}", m.role, m.content))
                .collect();
            let old_ctx = old_turns.join("\n");

            info!(session_id = %session_uuid, tokens = total_tokens, "Triggering deferred memory compaction & constraint extraction");
            spawn_history_compaction(
                Arc::clone(llm_pool),
                session_memory.clone(),
                session_uuid,
                sessions_dir.to_path_buf(),
                old_ctx,
                remove_count,
                effective_req.timestamp_us,
            );
        } else {
            // Not exceeding limit, keep everything (or apply dynamic windowing)
            // Just drop old messages that exceed a generous fallback window (e.g. 50)
            let max_window = 50;
            start_idx = hist_len.saturating_sub(max_window);
        }

        // Build history text to pass to LLM
        for m in hist.iter().skip(start_idx) {
            history_ctx.push_str(&format!("{}: {}\n", m.role, m.content));
        }
        history_ctx = truncate_to_token_budget(&history_ctx, prompt_budget.history_tokens);

        debug!(
            session_id = %session_uuid,
            total_history_turns = hist_len,
            windowed_turns = hist_len - start_idx,
            tokens = total_tokens,
            "process_request: loaded session history window"
        );
    } else {
        debug!(session_id = %session_uuid, "process_request: no session history (new user)");
    }

    let store = RuntimeStore::for_sessions_dir(&sessions_dir);
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    for root in &whitelist {
        let path = Path::new(root);
        if path.is_dir() {
            let _ = index_control_documents_for_workspace(&store, path, now_us);
        }
    }

    let query_embedding = embedder.embed(&request_text);
    let rag_started = std::time::Instant::now();
    let (rag_context_raw, retrieval_metrics) = build_split_rag_context(
        &request_text,
        &query_embedding,
        vector_store,
        page_index,
        keyword_index,
        capability_profile.as_ref(),
        trust_profile,
    );
    let mut rag_context = truncate_to_token_budget(&rag_context_raw, prompt_budget.rag_tokens);
    let control_document_conflicts =
        detect_control_document_conflicts(&store, &whitelist).unwrap_or_default();
    let control_doc_context_raw =
        build_control_document_context(&store, &whitelist, capability_profile.as_ref())
            .unwrap_or_default();
    let control_doc_context =
        truncate_to_token_budget(&control_doc_context_raw, prompt_budget.control_tokens);
    if !control_doc_context.is_empty() {
            if !rag_context.is_empty() {
                rag_context.push_str("\n\n");
            }
            rag_context.push_str(&control_doc_context);
    }
    if let Ok(sub_agent_context) = build_sub_agent_result_context(&store, session_uuid) {
        if !sub_agent_context.is_empty() {
            if !rag_context.is_empty() {
                rag_context.push_str("\n\n");
            }
            rag_context.push_str(&sub_agent_context);
        }
    }
    debug!(
        rag_context_len = rag_context.len(),
        "process_request: RAG context built"
    );

    let user_msg = aria_ssmu::Message {
        role: "user".into(),
        content: request_text.clone(),
        timestamp_us: effective_req.timestamp_us,
    };
    let _ = session_memory.append(session_uuid, user_msg.clone());
    let _ = session_memory.append_audit_event(sessions_dir, &session_uuid, &user_msg);

    let intent_ctx = scheduling_intent
        .as_ref()
        .map(|intent| scheduling_intent_context(intent, user_timezone))
        .unwrap_or_default();
    let scenario_prompt_ctx = build_scenario_prompt_context(
        &effective_req,
        &request_text,
        trust_profile,
        scheduling_intent.as_ref(),
        &cache.active_tools(),
    );
    let learning_prompt_mode = learning_prompt_mode_label(
        &effective_req,
        &request_text,
        trust_profile,
        scheduling_intent.as_ref(),
    );
    let promoted_learning_candidates = RuntimeStore::for_sessions_dir(&sessions_dir)
        .list_promoted_candidates_for_request(&agent, &learning_prompt_mode, &request_text)
        .unwrap_or_default();
    apply_learning_macro_rollouts(&mut cache, tool_registry, &promoted_learning_candidates);
    let selector_models = RuntimeStore::for_sessions_dir(&sessions_dir)
        .list_selector_models_for_request(&agent, &learning_prompt_mode, &request_text)
        .unwrap_or_default();
    apply_learning_selector_models(&mut cache, tool_registry, &selector_models);
    let prompt_tools = select_prompt_tool_window(
        &request_text,
        &cache.active_tools(),
        tool_registry,
        embedder,
        active_model_capability.as_ref(),
        prompt_budget,
    );
    let learning_rollout_ctx = build_learning_rollout_prompt_context(&promoted_learning_candidates);
    let final_system_prompt = format!(
        "{}{}{}{}{}",
        system_prompt,
        durable_constraints_ctx,
        intent_ctx,
        scenario_prompt_ctx,
        learning_rollout_ctx
    );

    // Item 3 – Secrets Audit: scan RAG context and system prompt for sensitive
    // patterns (API keys, tokens) before forwarding to the LLM.
    // Pattern matches are redacted so they never reach the model context.
    let rag_context = match firewall.scan_egress(&rag_context) {
        aria_safety::ScanResult::Alert(patterns) => {
            tracing::warn!(matched = ?patterns, "Firewall blocked RAG context egress — sensitive pattern detected");
            "[RAG context redacted by firewall]".to_string()
        }
        aria_safety::ScanResult::Clean => rag_context,
    };
    let final_system_prompt = match firewall.scan_egress(&final_system_prompt) {
        aria_safety::ScanResult::Alert(patterns) => {
            tracing::warn!(matched = ?patterns, "Firewall blocked system prompt egress — sensitive pattern detected");
            system_prompt.to_string()
        }
        aria_safety::ScanResult::Clean => final_system_prompt,
    };

    let retrieval_trace = aria_core::RetrievalTraceRecord {
        trace_id: uuid::Uuid::new_v4().to_string(),
        request_id: effective_req.request_id,
        session_id: *session_uuid.as_bytes(),
        agent_id: agent.clone(),
        query_text: truncate_trace_text(&request_text, 240),
        latency_ms: rag_started.elapsed().as_millis() as u64,
        history_tokens: estimate_token_count(&history_ctx) as u32,
        rag_tokens: estimate_token_count(&rag_context) as u32,
        control_tokens: estimate_token_count(&control_doc_context) as u32,
        tool_count: prompt_tools.len() as u32,
        session_hits: retrieval_metrics.session_hits,
        workspace_hits: retrieval_metrics.workspace_hits,
        policy_hits: retrieval_metrics.policy_hits,
        external_hits: retrieval_metrics.external_hits,
        social_hits: retrieval_metrics.social_hits,
        page_context_hits: retrieval_metrics.page_context_hits,
        control_document_conflicts: control_document_conflicts.len() as u32,
        created_at_us: effective_req.timestamp_us,
    };
    let _ = RuntimeStore::for_sessions_dir(&sessions_dir).append_retrieval_trace(&retrieval_trace);

    let direct_specific_tool_result = if let Some(aria_core::ToolRuntimePolicy {
        tool_choice: aria_core::ToolChoicePolicy::Specific(tool_name),
        ..
    }) = effective_req.tool_runtime_policy.as_ref()
    {
        if let Some(heuristic_call) = heuristic_specific_tool_call(
            &request_text,
            tool_name.as_str(),
            scheduling_intent.as_ref(),
        ) {
            match build_policy_executor().execute(&heuristic_call).await {
                Ok(tool_result) => {
                    executed_tool_names
                        .lock()
                        .expect("executed tool names lock poisoned")
                        .push(heuristic_call.name.clone());
                    Some(aria_intelligence::OrchestratorResult::Completed(
                        tool_result.render_for_prompt().to_string(),
                    ))
                }
                Err(OrchestratorError::ToolError(message)) => {
                    if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                        sessions_dir,
                        &effective_req,
                        &heuristic_call,
                        &message,
                    ) {
                        Some(aria_intelligence::OrchestratorResult::Completed(approval_text))
                    } else {
                        Some(aria_intelligence::OrchestratorResult::Completed(format!(
                            "The forced tool '{}' could not be executed from this request: {}",
                            tool_name, message
                        )))
                    }
                }
                Err(err) => Some(aria_intelligence::OrchestratorResult::Completed(format!(
                    "The forced tool '{}' could not be executed from this request: {}",
                    tool_name, err
                ))),
            }
        } else {
            None
        }
    } else {
        None
    };

    let allow_scheduling_retry = steering_rx.is_none();
    let mut orchestrator_result = if let Some(result) = direct_specific_tool_result {
        result
    } else {
        orchestrator
            .run_for_request_with_dynamic_tools(aria_intelligence::DynamicRunContext {
                agent_system_prompt: &final_system_prompt,
                request: &effective_req,
                history_context: &history_ctx,
                rag_context: &rag_context,
                prompt_tools: Some(&prompt_tools),
                cache: &mut cache,
                tool_registry,
                embedder,
                max_tool_rounds: max_rounds,
                model_capability: active_model_capability.as_ref(),
                steering_rx,
                global_estop,
            })
            .await?
    };

    let mut executed_tools_snapshot = executed_tool_names
        .lock()
        .expect("executed tool names lock poisoned")
        .clone();
    if allow_scheduling_retry
        && scheduling_intent.is_some()
        && request_is_reminder_like(&request_text)
        && executed_tools_snapshot.is_empty()
        && matches!(
            orchestrator_result,
            aria_intelligence::OrchestratorResult::Completed(_)
        )
    {
        let mut retry_req = effective_req.clone();
        retry_req.tool_runtime_policy = Some(aria_core::ToolRuntimePolicy {
            tool_choice: aria_core::ToolChoicePolicy::Specific("set_reminder".to_string()),
            allow_parallel_tool_calls: false,
        });
        retry_req.content = aria_core::MessageContent::Text(format!(
            "{}\n\nUse the set_reminder tool to satisfy this scheduling request. Return the tool call if needed.",
            request_text
        ));
        let retry_result = orchestrator
            .run_for_request_with_dynamic_tools(aria_intelligence::DynamicRunContext {
                agent_system_prompt: &final_system_prompt,
                request: &retry_req,
                history_context: &history_ctx,
                rag_context: &rag_context,
                prompt_tools: Some(&prompt_tools),
                cache: &mut cache,
                tool_registry,
                embedder,
                max_tool_rounds: max_rounds,
                model_capability: active_model_capability.as_ref(),
                steering_rx: None,
                global_estop,
            })
            .await?;
        let retry_tools_snapshot = executed_tool_names
            .lock()
            .expect("executed tool names lock poisoned")
            .clone();
        if retry_tools_snapshot.len() == executed_tools_snapshot.len()
            && matches!(
                &retry_result,
                aria_intelligence::OrchestratorResult::Completed(_)
            )
        {
            if let Some(heuristic_call) = heuristic_specific_tool_call(
                &request_text,
                "set_reminder",
                scheduling_intent.as_ref(),
            ) {
                match build_policy_executor().execute(&heuristic_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(heuristic_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                        let updated_snapshot = executed_tool_names
                            .lock()
                            .expect("executed tool names lock poisoned")
                            .clone();
                        executed_tools_snapshot = updated_snapshot;
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &retry_req,
                            &heuristic_call,
                            &message,
                        ) {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        if retry_tools_snapshot.len() > executed_tools_snapshot.len()
            || matches!(
                retry_result,
                aria_intelligence::OrchestratorResult::ToolApprovalRequired { .. }
            )
        {
            orchestrator_result = retry_result;
            executed_tools_snapshot = retry_tools_snapshot;
        }
    }

    if allow_scheduling_retry
        && request_is_browser_read_like(&request_text)
        && executed_tools_snapshot.is_empty()
        && matches!(
            orchestrator_result,
            aria_intelligence::OrchestratorResult::Completed(_)
        )
    {
        let mut retry_req = effective_req.clone();
        retry_req.tool_runtime_policy = Some(browser_read_retry_policy(&request_text));
        retry_req.content = aria_core::MessageContent::Text(format!(
            "{}\n\nUse only the native browser read tools needed for this request: browser_open, browser_snapshot, browser_screenshot, browser_extract. Do not emit <tool_call> tags or explanatory tool plans; execute the tool call through the runtime.",
            request_text
        ));
        let retry_result = orchestrator
            .run_for_request_with_dynamic_tools(aria_intelligence::DynamicRunContext {
                agent_system_prompt: &final_system_prompt,
                request: &retry_req,
                history_context: &history_ctx,
                rag_context: &rag_context,
                prompt_tools: Some(&prompt_tools),
                cache: &mut cache,
                tool_registry,
                embedder,
                max_tool_rounds: max_rounds,
                model_capability: active_model_capability.as_ref(),
                steering_rx: None,
                global_estop,
            })
            .await?;
        let retry_tools_snapshot = executed_tool_names
            .lock()
            .expect("executed tool names lock poisoned")
            .clone();
        let retry_completed_cleanly = matches!(
            &retry_result,
            aria_intelligence::OrchestratorResult::Completed(text)
                if !looks_like_tool_payload(text)
        );
        if retry_tools_snapshot.len() > executed_tools_snapshot.len()
            || matches!(
                retry_result,
                aria_intelligence::OrchestratorResult::ToolApprovalRequired { .. }
            )
            || retry_completed_cleanly
        {
            orchestrator_result = retry_result;
            executed_tools_snapshot = retry_tools_snapshot;
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(_) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() && request_is_browser_action_like(&request_text) {
            if let Some(heuristic_call) = heuristic_browser_action_tool_call(&request_text) {
                match build_policy_executor().execute(&heuristic_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(heuristic_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &effective_req,
                            &heuristic_call,
                            &message,
                        )
                        {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(_) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() && request_is_mcp_operation_like(&request_text) {
            if let Some(heuristic_call) = heuristic_mcp_tool_call(&request_text) {
                match build_policy_executor().execute(&heuristic_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(heuristic_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &effective_req,
                            &heuristic_call,
                            &message,
                        )
                        {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(_) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() {
            if let Some(heuristic_call) = heuristic_browser_read_tool_call(&request_text) {
                match build_policy_executor().execute(&heuristic_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(heuristic_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &effective_req,
                            &heuristic_call,
                            &message,
                        )
                        {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(_) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() {
            if let Some(heuristic_call) = heuristic_browser_action_tool_call(&request_text) {
                match build_policy_executor().execute(&heuristic_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(heuristic_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &effective_req,
                            &heuristic_call,
                            &message,
                        ) {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(_) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() {
            if let Some(aria_core::ToolRuntimePolicy {
                tool_choice: aria_core::ToolChoicePolicy::Specific(tool_name),
                ..
            }) = effective_req.tool_runtime_policy.as_ref()
            {
                if let Some(heuristic_call) =
                    heuristic_specific_tool_call(
                        &request_text,
                        tool_name.as_str(),
                        scheduling_intent.as_ref(),
                    )
                {
                    match build_policy_executor().execute(&heuristic_call).await {
                        Ok(tool_result) => {
                            executed_tools_snapshot.push(heuristic_call.name.clone());
                            orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                                tool_result.render_for_prompt().to_string(),
                            );
                        }
                        Err(OrchestratorError::ToolError(message)) => {
                            if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                                sessions_dir,
                                &effective_req,
                                &heuristic_call,
                                &message,
                            ) {
                                orchestrator_result =
                                    aria_intelligence::OrchestratorResult::Completed(approval_text);
                            } else {
                                orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                                    format!(
                                        "The forced tool '{}' could not be executed from this request: {}",
                                        tool_name, message
                                    ),
                                );
                            }
                        }
                        Err(err) => {
                            orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                                format!(
                                    "The forced tool '{}' could not be executed from this request: {}",
                                    tool_name, err
                                ),
                            );
                        }
                    }
                }
            }
        }
    }

    if let aria_intelligence::OrchestratorResult::Completed(response_text) = &orchestrator_result {
        if executed_tools_snapshot.is_empty() {
            if let Some(repaired_call) =
                aria_intelligence::repair_tool_call_json(response_text, &cache.active_tools())
            {
                match build_policy_executor().execute(&repaired_call).await {
                    Ok(tool_result) => {
                        executed_tools_snapshot.push(repaired_call.name.clone());
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            tool_result.render_for_prompt().to_string(),
                        );
                    }
                    Err(OrchestratorError::ToolError(message)) => {
                        if let Ok((_, approval_text)) = persist_pending_approval_for_tool_error(
                            sessions_dir,
                            &effective_req,
                            &repaired_call,
                            &message,
                        )
                        {
                            orchestrator_result =
                                aria_intelligence::OrchestratorResult::Completed(approval_text);
                        } else {
                            orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                                user_facing_tool_recovery_message(
                                    &request_text,
                                    Some(&repaired_call.name),
                                    Some(&message),
                                ),
                            );
                        }
                    }
                    Err(err) => {
                        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
                            user_facing_tool_recovery_message(
                                &request_text,
                                Some(&repaired_call.name),
                                Some(&err.to_string()),
                            ),
                        );
                    }
                }
            }
        }
    }

    let response_text = match &orchestrator_result {
        aria_intelligence::OrchestratorResult::Completed(text) => text.clone(),
        aria_intelligence::OrchestratorResult::AgentElevationRequired { message, .. } => {
            message.clone()
        }
        aria_intelligence::OrchestratorResult::ToolApprovalRequired { .. } => "".to_string(),
    };
    if matches!(
        orchestrator_result,
        aria_intelligence::OrchestratorResult::Completed(_)
    ) && looks_like_tool_payload(&response_text)
    {
        warn!(
            session_id = %session_uuid,
            preview = %response_text.chars().take(160).collect::<String>(),
            "Suppressing leaked internal tool payload from user-visible response"
        );
        orchestrator_result = aria_intelligence::OrchestratorResult::Completed(
            user_facing_tool_recovery_message(&request_text, None, None),
        );
    }

    match firewall.scan_egress(&response_text) {
        aria_safety::ScanResult::Alert(alerts) => {
            warn!(session_id = %session_uuid, alerts = ?alerts, "DfaFirewall blocked egress payload");
            return Err(OrchestratorError::SecurityViolation(format!(
                "Blocked bad patterns in egress: {:?}",
                alerts
            )));
        }
        aria_safety::ScanResult::Clean => {}
    }

    // Only append to history if the LLM completed its turn
    if let aria_intelligence::OrchestratorResult::Completed(ref response_text) = orchestrator_result
    {
        let assistant_msg = aria_ssmu::Message {
            role: "assistant".into(),
            content: response_text.clone(),
            timestamp_us: effective_req.timestamp_us,
        };
        let _ = session_memory.append(session_uuid, assistant_msg.clone());
        let _ = session_memory.append_audit_event(sessions_dir, &session_uuid, &assistant_msg);
    }

    let _ = RuntimeStore::for_sessions_dir(&sessions_dir).upsert_cache_snapshot(
        session_uuid,
        &agent,
        &cache.active_tools(),
        chrono::Utc::now().timestamp_micros() as u64,
    );

    maybe_record_retry_reward(
        learning,
        sessions_dir,
        &effective_req,
        &agent,
        &learning_prompt_mode,
        &request_text,
        effective_req.timestamp_us,
    );
    persist_learning_trace(
        learning,
        sessions_dir,
        &effective_req,
        &agent,
        &learning_prompt_mode,
        &request_text,
        &executed_tools_snapshot,
        &orchestrator_result,
        &rag_context,
        effective_req.timestamp_us,
    );

    Ok(orchestrator_result)
}

#[cfg(test)]
mod phase5_tests {
    use super::*;
    use aria_core::GatewayChannel;

    #[tokio::test]
    async fn native_tool_executor_rejects_conflicting_write_file_lease() {
        let sessions_dir =
            std::env::temp_dir().join(format!("aria-x-phase5-lease-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&sessions_dir).expect("sessions dir");
        let target_path = sessions_dir.join("shared.txt");
        let store = RuntimeStore::for_sessions_dir(&sessions_dir);
        let now_us = chrono::Utc::now().timestamp_micros() as u64;
        store
            .try_acquire_resource_lease(
                &format!("fs:{}", target_path.display()),
                "exclusive",
                "holder-a",
                now_us,
                now_us + 30_000_000,
            )
            .expect("acquire lease")
            .expect("lease token");

        let (tx, _rx) = tokio::sync::mpsc::channel::<aria_intelligence::CronCommand>(1);
        let exec = NativeToolExecutor {
            tx_cron: tx,
            invoking_agent_id: Some("developer".into()),
            session_id: Some(*uuid::Uuid::new_v4().as_bytes()),
            user_id: Some("u1".into()),
            channel: Some(GatewayChannel::Cli),
            session_memory: None,
            cedar: None,
            sessions_dir: Some(sessions_dir.clone()),
            scheduling_intent: None,
            user_timezone: chrono_tz::UTC,
        };

        let err = exec
            .execute(&ToolCall {
                invocation_id: Some("inv-1".into()),
                name: "write_file".into(),
                arguments: format!(
                    r#"{{"path":"{}","content":"new content"}}"#,
                    target_path.display()
                ),
            })
            .await
            .expect_err("lease contention should fail");

        assert!(format!("{}", err).contains("tool 'write_file' busy"));
        let _ = std::fs::remove_dir_all(&sessions_dir);
    }

    #[tokio::test]
    async fn shared_quota_claim_rejects_second_holder_when_limit_is_one() {
        let sessions_dir =
            std::env::temp_dir().join(format!("aria-x-phase5-quota-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&sessions_dir).expect("sessions dir");

        let first = acquire_shared_quota_claim(&sessions_dir, "channel:cli", 1, "holder-a", 30)
            .await
            .expect("first quota claim");
        let err = acquire_shared_quota_claim(&sessions_dir, "channel:cli", 1, "holder-b", 30)
            .await
            .expect_err("second quota claim should fail");
        assert!(format!("{}", err).contains("shared quota exhausted"));
        drop(first);

        let second = acquire_shared_quota_claim(&sessions_dir, "channel:cli", 1, "holder-c", 30)
            .await
            .expect("quota should recover after release");
        drop(second);
        let _ = std::fs::remove_dir_all(&sessions_dir);
    }
}

#[cfg(test)]
mod phase8_tests {
    use super::*;
    use aria_core::{CapabilitySupport, ModelCapabilityProfile, ModelRef, ToolResultMode, ToolSchemaMode};

    #[test]
    fn truncate_to_token_budget_limits_output() {
        let text = "one two three four five six seven eight";
        let truncated = truncate_to_token_budget(text, 3);
        assert_eq!(truncated, "one two three [truncated]");
        assert_eq!(estimate_token_count(&truncated), 4);
    }

    #[test]
    fn should_trigger_compaction_uses_stateful_cadence() {
        let now_us = 1_000_000_000;
        assert!(should_trigger_compaction(None, 30, 2500, now_us));
        assert!(!should_trigger_compaction(
            Some(&aria_core::CompactionState {
                session_id: *uuid::Uuid::new_v4().as_bytes(),
                status: aria_core::CompactionStatus::Running,
                last_started_at_us: Some(now_us - 1),
                last_completed_at_us: None,
                metadata: aria_core::CompactionMetadata {
                    summary_hash: None,
                    summary_version: 1,
                    last_error: None,
                },
            }),
            30,
            2500,
            now_us,
        ));
        assert!(!should_trigger_compaction(
            Some(&aria_core::CompactionState {
                session_id: *uuid::Uuid::new_v4().as_bytes(),
                status: aria_core::CompactionStatus::Succeeded,
                last_started_at_us: Some(now_us - 60_000_000),
                last_completed_at_us: Some(now_us - 60_000_000),
                metadata: aria_core::CompactionMetadata {
                    summary_hash: Some("hash".into()),
                    summary_version: 1,
                    last_error: None,
                },
            }),
            30,
            2500,
            now_us,
        ));
        assert!(should_trigger_compaction(
            Some(&aria_core::CompactionState {
                session_id: *uuid::Uuid::new_v4().as_bytes(),
                status: aria_core::CompactionStatus::Failed,
                last_started_at_us: Some(now_us - 600_000_000),
                last_completed_at_us: None,
                metadata: aria_core::CompactionMetadata {
                    summary_hash: None,
                    summary_version: 1,
                    last_error: Some("timeout".into()),
                },
            }),
            30,
            2500,
            now_us,
        ));
    }

    #[test]
    fn select_prompt_tool_window_respects_budget_and_capability() {
        let embedder = aria_intelligence::LocalHashEmbedder::new(32);
        let registry = ToolManifestStore::new();
        let active_tools = vec![
            CachedTool {
                name: "text_tool".into(),
                description: "text".into(),
                parameters_schema: "{}".into(),
                embedding: vec![],
                requires_strict_schema: false,
                streaming_safe: true,
                parallel_safe: true,
                modalities: vec![aria_core::ToolModality::Text],
            },
            CachedTool {
                name: "vision_tool".into(),
                description: "vision".into(),
                parameters_schema: "{}".into(),
                embedding: vec![],
                requires_strict_schema: false,
                streaming_safe: true,
                parallel_safe: true,
                modalities: vec![aria_core::ToolModality::Image],
            },
        ];
        let model_capability = ModelCapabilityProfile {
            model_ref: ModelRef::new("openrouter", "openai/gpt-4o-mini"),
            adapter_family: aria_core::AdapterFamily::OpenAiCompatible,
            tool_calling: CapabilitySupport::Supported,
            parallel_tool_calling: CapabilitySupport::Supported,
            streaming: CapabilitySupport::Supported,
            vision: CapabilitySupport::Unsupported,
            json_mode: CapabilitySupport::Supported,
            max_context_tokens: Some(128000),
            tool_schema_mode: ToolSchemaMode::StrictJsonSchema,
            tool_result_mode: ToolResultMode::NativeStructured,
            supports_images: CapabilitySupport::Unsupported,
            supports_audio: CapabilitySupport::Unsupported,
            source: aria_core::CapabilitySourceKind::ProviderCatalog,
            source_detail: None,
            observed_at_us: 1,
            expires_at_us: None,
        };

        let selected = select_prompt_tool_window(
            "read the file",
            &active_tools,
            &registry,
            &embedder,
            Some(&model_capability),
            PromptBudget {
                tool_count: 1,
                ..PromptBudget::default()
            },
        );
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "text_tool");
    }
}
