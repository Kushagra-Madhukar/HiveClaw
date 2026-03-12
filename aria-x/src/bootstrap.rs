// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

pub(crate) fn run_main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime build failed")
        .block_on(actual_main());
}

fn node_supports_ingress(role: &str) -> bool {
    matches!(
        role.trim().to_ascii_lowercase().as_str(),
        "orchestrator" | "combined" | "all" | "ingress"
    )
}

fn node_supports_outbound(role: &str) -> bool {
    matches!(
        role.trim().to_ascii_lowercase().as_str(),
        "orchestrator" | "combined" | "all" | "outbound"
    )
}

fn node_supports_scheduler(role: &str) -> bool {
    matches!(
        role.trim().to_ascii_lowercase().as_str(),
        "orchestrator" | "combined" | "all" | "scheduler"
    )
}

fn spawn_supervised_adapter<F, Fut>(
    adapter_name: &'static str,
    channel: GatewayChannel,
    make_future: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: core::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut attempt = 0u32;
        loop {
            attempt = attempt.saturating_add(1);
            crate::channel_health::mark_channel_adapter_state(channel, "starting");
            crate::channel_health::record_channel_health_event(
                channel,
                crate::channel_health::ChannelHealthEventKind::AdapterStarted,
            );
            let join = tokio::spawn(make_future()).await;
            match join {
                Ok(()) => {
                    crate::channel_health::record_channel_health_event(
                        channel,
                        crate::channel_health::ChannelHealthEventKind::AdapterExited,
                    );
                    warn!(
                        adapter = adapter_name,
                        attempt = attempt,
                        "Adapter exited; scheduling restart"
                    );
                }
                Err(err) => {
                    crate::channel_health::record_channel_health_event(
                        channel,
                        crate::channel_health::ChannelHealthEventKind::AdapterPanicked,
                    );
                    warn!(
                        adapter = adapter_name,
                        attempt = attempt,
                        error = %err,
                        "Adapter task panicked; scheduling restart"
                    );
                }
            }
            crate::channel_health::record_channel_health_event(
                channel,
                crate::channel_health::ChannelHealthEventKind::AdapterRestarted,
            );
            let backoff_secs = u64::from(attempt.min(5));
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs.max(1))).await;
        }
    })
}

async fn actual_main() {
    // Load .env from CWD and ~/.aria/.env before config (does not override existing vars)
    load_env();

    let args: Vec<String> = std::env::args().collect();
    let runtime_env = load_runtime_env_config().unwrap_or_else(|err| {
        eprintln!("[aria-x] Failed to resolve runtime environment config: {}", err);
        std::process::exit(1);
    });

    let startup_mode = crate::tui::parse_startup_mode(&args, runtime_env.config_path.clone());
    let tui_mode = matches!(startup_mode, crate::tui::StartupMode::Tui { .. });
    let config_path = match &startup_mode {
        crate::tui::StartupMode::Runtime { config_path }
        | crate::tui::StartupMode::Tui { config_path, .. } => config_path.clone(),
    };
    let runtime_config_path = if config_path.trim().is_empty() {
        default_runtime_config_path()
    } else {
        resolve_config_path(&config_path).with_extension("runtime.json")
    };

    if tui_mode {
        let attach_url = match &startup_mode {
            crate::tui::StartupMode::Tui { attach_url, .. } => attach_url.as_deref(),
            _ => None,
        };
        if let Err(err) = crate::tui::run_tui_mode(&config_path, attach_url).await {
            eprintln!("[aria-x] TUI failed: {}", err);
            std::process::exit(1);
        }
        return;
    }

    println!("[aria-x] Loading config from: {}", config_path);

    let config = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[aria-x] Failed to load config '{}' (cwd: {}): {}",
                config_path,
                std::env::current_dir().unwrap_or_default().display(),
                e
            );
            let _ = std::io::stderr().flush();
            std::process::exit(1);
        }
    };

    if let Some(output) = run_channel_onboarding_command(&config.path, &args) {
        match output {
            Ok(text) => {
                println!("{}", text);
                return;
            }
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        }
    }

    if let Err(err) = validate_config(&config) {
        eprintln!("[aria-x] Config validation error: {}", err);
        eprintln!("[aria-x] For Telegram: set TELEGRAM_BOT_TOKEN or add telegram_token to config");
        let _ = std::io::stderr().flush();
        std::process::exit(1);
    }
    let config = Arc::new(config);
    install_app_runtime(Arc::clone(&config));

    RuntimeStore::configure_operator_retention(
        config.ssmu.operator_skill_signature_max_rows,
        config.ssmu.operator_shell_exec_audit_max_rows,
        config.ssmu.operator_scope_denial_max_rows,
        config.ssmu.operator_request_policy_audit_max_rows,
        config.ssmu.operator_repair_fallback_audit_max_rows,
        config.ssmu.operator_streaming_decision_audit_max_rows,
        config.ssmu.operator_browser_action_audit_max_rows,
        config.ssmu.operator_browser_challenge_event_max_rows,
    );

    match run_admin_inspect_command(&config, &args) {
        Ok(Some(json)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json)
                    .unwrap_or_else(|_| "{\"error\":\"serialize failed\"}".into())
            );
            return;
        }
        Ok(None) => {}
        Err(err) => {
            eprintln!("[aria-x] Inspect command failed: {}", err);
            std::process::exit(1);
        }
    }

    // Init tracing (RUST_LOG overrides config)
    init_tracing(&config);

    info!(
        node = %config.node.id,
        role = %config.node.role,
        instance_id = %runtime_instance_id(),
        llm = %config.llm.backend,
        model = %config.llm.model,
        "Config loaded"
    );
    let feature_flags = runtime_feature_flags();
    info!(
        multi_channel_gateway = feature_flags.multi_channel_gateway,
        append_only_session_log = feature_flags.append_only_session_log,
        resource_leases_enforced = feature_flags.resource_leases_enforced,
        outbox_delivery = feature_flags.outbox_delivery,
        "Runtime feature flags"
    );
    if config.simulator.enabled {
        info!(backend = %config.simulator.backend, "Simulator mode enabled");
    }

    // Initialize Cedar policy engine (fail fast — never run without valid policy)
    let policy_content = match std::fs::read_to_string(&config.policy.policy_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[aria-x] Fatal: failed to read policy file '{}': {}",
                config.policy.policy_path, e
            );
            std::process::exit(1);
        }
    };
    let cedar = match aria_policy::CedarEvaluator::from_policy_str(&policy_content) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[aria-x] Fatal: failed to parse Cedar policies: {}", e);
            std::process::exit(1);
        }
    };
    let cedar = Arc::new(cedar);

    // Initialize Semantic Router with MiniLM-L6-v2 embedder (384-dim SBERT)
    let embedder = Arc::new(
        FastEmbedder::new().unwrap_or_else(|e| {
            warn!(error = %e, "FastEmbedder init failed, falling back to LocalHashEmbedder not available in this path");
            panic!("Cannot initialize embedding model: {}", e);
        })
    );
    let mut router = SemanticRouter::new();
    let agent_store = AgentConfigStore::load_from_dir(&config.agents_dir.path).unwrap_or_default();
    let mut tool_registry = ToolManifestStore::new();
    let mut vector_store = VectorStore::new();

    // Index workspace knowledge documents with real semantic embeddings
    vector_store.index_document(
        "workspace.files",
        "File system tools: list files, read source code, navigate project structure.",
        embedder.embed("list files read source navigate project workspace"),
        "workspace",
        vec!["files".into(), "source".into(), "workspace".into()],
        false,
    );
    vector_store.index_document(
        "workspace.rust",
        "Rust development: cargo build, cargo test, compile crates, fix errors.",
        embedder.embed("rust cargo build test compile crates errors"),
        "workspace",
        vec!["rust".into(), "cargo".into(), "build".into()],
        false,
    );
    vector_store.index_document(
        "security.policy",
        "Cedar policy engine: authorization decisions, access control, denied paths.",
        embedder.embed("security authorization cedar policy access control"),
        "policy",
        vec!["security".into(), "authorization".into(), "cedar".into()],
        false,
    );
    if agent_store.is_empty() {
        // Bootstrap fallback agents when no TOML configs found
        let _ = router.register_agent_text(
            "developer",
            "Write code, read files, search codebase, run tests, execute shell commands",
            &*embedder,
        );
        let _ = router.register_agent_text(
            "researcher",
            "Search the web, fetch URLs, summarise documents, query knowledge base",
            &*embedder,
        );
        warn!(
            path = %config.agents_dir.path,
            "No agent configs found; using bootstrap agents"
        );
    } else {
        // Register each loaded agent and index its full description + system prompt
        for cfg in agent_store.all() {
            // Register agent embedding using full description for better routing
            let _ = router.register_agent_text(&cfg.id, &cfg.description, &*embedder);

            // Index the agent as a knowledge document in the vector store
            let agent_doc_text = format!("{} {}", cfg.description, cfg.system_prompt);
            vector_store.index_document(
                format!("agent.{}", cfg.id),
                format!("{}: {}", cfg.id, cfg.description),
                embedder.embed(&agent_doc_text),
                "agent",
                vec![cfg.id.clone()],
                false,
            );

            // Register tools with real descriptions and schemas
            for tool_name in &cfg.base_tool_names {
                if !runtime_exposes_base_tool(tool_name) {
                    warn!(tool = %tool_name, agent = %cfg.id, "Skipping unavailable base tool during bootstrap registration");
                    continue;
                }
                let (desc, schema) = match tool_name.as_str() {
                    "read_file" => (
                        "Read the contents of a file at the given path. Returns the file content as text.",
                        r#"{"path": {"type":"string","description":"File path to read"}}"#,
                    ),
                    "write_file" => (
                        "Write content to a file at the given path. Creates the file if it does not exist.",
                        r#"{"path": {"type":"string","description":"File path to write to"}, "content": {"type":"string","description":"Text content to write"}}"#,
                    ),
                    "search_codebase" => (
                        "Search the codebase for a pattern or keyword. Returns matching file paths and snippets.",
                        r#"{"query": {"type":"string","description":"Search pattern or keyword"}}"#,
                    ),
                    "run_tests" => (
                        "Run the test suite and return pass/fail results.",
                        r#"{"target": {"type":"string","description":"Crate or test name to run, or empty for all"}}"#,
                    ),
                    "run_shell" => (
                        "Execute a shell command and return stdout/stderr output.",
                        r#"{"command": {"type":"string","description":"Shell command to run"}}"#,
                    ),
                    "search_web" => (
                        "Search the web for information about a query. Returns a summary of top results.",
                        r#"{"query": {"type":"string","description":"Web search query"}}"#,
                    ),
                    "fetch_url" => (
                        "Fetch the content of a URL and return it as text.",
                        r#"{"url": {"type":"string","description":"URL to fetch"}}"#,
                    ),
                    "set_domain_access_decision" => (
                        "Persist a domain access decision for a target agent. This is sensitive and requires human approval.",
                        r#"{"domain": {"type":"string","description":"Domain or URL to normalize and store"}, "decision": {"type":"string","enum":["allow_once","allow_for_session","allow_always","deny_once","deny_always"],"description":"Decision to persist"}, "action_family": {"type":"string","enum":["fetch","crawl","screenshot","interactive_read","interactive_write","login","download"],"description":"Action family controlled by the decision"}, "scope": {"type":"string","enum":["domain","session","request"],"description":"Storage scope override"}, "agent_id": {"type":"string","description":"Optional target agent id; defaults to the invoking agent"}, "reason": {"type":"string","description":"Optional audit note"}}"#,
                    ),
                    "browser_profile_create" => (
                        "Create a managed browser profile for later authenticated or read-only browsing.",
                        r#"{"profile_id": {"type":"string","description":"Stable profile id"}, "display_name": {"type":"string","description":"Optional human-friendly name"}, "mode": {"type":"string","enum":["ephemeral","managed_persistent","attached_external","extension_bound"],"description":"Browser profile mode"}, "engine": {"type":"string","enum":["chromium","chrome","edge","safari_bridge"],"description":"Browser engine"}, "allowed_domains": {"type":"array","items":{"type":"string"},"description":"Optional default domain allowlist"}, "auth_enabled": {"type":"boolean","description":"Whether the profile can be used for authenticated flows"}, "write_enabled": {"type":"boolean","description":"Whether the profile can be used for write actions"}, "persistent": {"type":"boolean","description":"Whether the profile is persistent"}, "attached_source": {"type":"string","description":"Optional external browser/profile source identifier for attached profiles"}, "extension_binding_id": {"type":"string","description":"Optional extension binding id for extension-bound profiles"}}"#,
                    ),
                    "browser_profile_list" => (
                        "List managed browser profiles available to the runtime.",
                        r#"{}"#,
                    ),
                    "browser_profile_use" => (
                        "Bind a managed browser profile to the current session and agent.",
                        r#"{"profile_id": {"type":"string","description":"Managed browser profile id to bind for the current session"}}"#,
                    ),
                    "browser_session_start" => (
                        "Launch a managed browser session using a stored browser profile.",
                        r#"{"profile_id": {"type":"string","description":"Optional managed profile id; defaults to the current session binding"}, "url": {"type":"string","description":"Optional start URL"}} "#,
                    ),
                    "browser_session_list" => (
                        "List browser sessions for the current agent and session.",
                        r#"{}"#,
                    ),
                    "browser_session_status" => (
                        "Inspect a specific browser session record by id.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id to inspect"}}"#,
                    ),
                    "browser_session_cleanup" => (
                        "Mark stale launched browser sessions as exited when their process is no longer alive.",
                        r#"{"browser_session_id": {"type":"string","description":"Optional managed browser session id to limit cleanup output"}} "#,
                    ),
                    "browser_session_persist_state" => (
                        "Persist the current browser storage state for a managed browser session as an encrypted state snapshot.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id to persist state for"}}"#,
                    ),
                    "browser_session_restore_state" => (
                        "Restore the latest encrypted browser storage state for the managed profile backing a browser session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id to restore state into"}}"#,
                    ),
                    "browser_session_pause" => (
                        "Pause a managed browser session after a challenge or human handoff boundary.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id to pause"}}"#,
                    ),
                    "browser_session_resume" => (
                        "Resume a previously paused managed browser session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id to resume"}}"#,
                    ),
                    "browser_session_record_challenge" => (
                        "Record a detected browser challenge event for a managed browser session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "challenge": {"type":"string","enum":["captcha","mfa","bot_defense","login_required","unknown"],"description":"Detected challenge kind"}, "url": {"type":"string","description":"Optional page URL"}, "message": {"type":"string","description":"Optional challenge message"}}"#,
                    ),
                    "browser_login_status" => (
                        "List persisted browser login state records for the current agent/session, optionally filtered by browser session id or domain.",
                        r#"{"browser_session_id": {"type":"string","description":"Optional managed browser session id to filter"}, "domain": {"type":"string","description":"Optional domain or URL to normalize and filter"}} "#,
                    ),
                    "browser_login_begin_manual" => (
                        "Mark a managed browser session as waiting for manual login on a target domain and pause the session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "domain": {"type":"string","description":"Domain or URL being authenticated"}, "notes": {"type":"string","description":"Optional login notes"}} "#,
                    ),
                    "browser_login_complete_manual" => (
                        "Mark a manually-assisted login as completed for a managed browser session and resume the session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "domain": {"type":"string","description":"Domain or URL that was authenticated"}, "credential_key_names": {"type":"array","items":{"type":"string"},"description":"Optional vault key names used for this login"}, "notes": {"type":"string","description":"Optional login notes"}} "#,
                    ),
                    "browser_login_fill_credentials" => (
                        "Fill approved credentials from the vault into a managed browser session through the browser automation bridge without exposing secret values to the model.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "domain": {"type":"string","description":"Domain or URL being authenticated"}, "credentials": {"type":"array","description":"Credential fill descriptors","items":{"type":"object","properties":{"key_name":{"type":"string"},"selector":{"type":"string"},"field":{"type":"string"}},"required":["key_name"],"additionalProperties":false}}}"#,
                    ),
                    "browser_open" => (
                        "Open a URL in a managed browser profile and start a browser session.",
                        r#"{"profile_id": {"type":"string","description":"Optional managed profile id; defaults to the current binding"}, "url": {"type":"string","description":"URL to open in the browser session"}}"#,
                    ),
                    "browser_act" => (
                        "Perform a typed browser action. Navigate and wait are implemented; click/type/select/scroll remain gated until the DOM automation backend is enabled.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "action": {"type":"string","enum":["navigate","wait","click","type","select","scroll"],"description":"Browser action to perform"}, "url": {"type":"string","description":"Target URL for navigate"}, "selector": {"type":"string","description":"Target selector for click/type/select/scroll"}, "text": {"type":"string","description":"Input text for type"}, "value": {"type":"string","description":"Selected value for select"}, "millis": {"type":"integer","description":"Wait duration in milliseconds for wait"}}"#,
                    ),
                    "browser_snapshot" => (
                        "Fetch and persist an HTML snapshot for a page within a managed browser session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "url": {"type":"string","description":"URL to snapshot"}}"#,
                    ),
                    "browser_screenshot" => (
                        "Capture a real PNG screenshot for a page within a managed browser session using the configured browser engine.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "url": {"type":"string","description":"URL to capture as a screenshot"}}"#,
                    ),
                    "browser_extract" => (
                        "Fetch and persist extracted page text for a page within a managed browser session.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "url": {"type":"string","description":"URL to extract"}}"#,
                    ),
                    "browser_download" => (
                        "Download a URL into a managed browser session artifact with audit.",
                        r#"{"browser_session_id": {"type":"string","description":"Managed browser session id"}, "url": {"type":"string","description":"URL to download"}, "filename": {"type":"string","description":"Optional output filename override"}}"#,
                    ),
                    "web_fetch" => (
                        "Fetch a URL over HTTP and return the response body and content type.",
                        r#"{"url": {"type":"string","description":"URL to fetch"}}"#,
                    ),
                    "web_extract" => (
                        "Fetch a URL over HTTP and return extracted text content.",
                        r#"{"url": {"type":"string","description":"URL to fetch and extract"}}"#,
                    ),
                    "crawl_page" => (
                        "Crawl a single page, extract text, and update website memory for the domain.",
                        r#"{"url": {"type":"string","description":"Page URL to crawl"}, "capture_screenshots": {"type":"boolean","description":"Reserved for future screenshot capture"}, "change_detection": {"type":"boolean","description":"Reserved for future change detection controls"}}"#,
                    ),
                    "crawl_site" => (
                        "Crawl a site within the requested scope, extract text from discovered pages, and update website memory.",
                        r#"{"url": {"type":"string","description":"Seed site URL to crawl"}, "scope": {"type":"string","enum":["single_page","same_origin","allowlisted_domains","scheduled_watch_allowed"],"description":"Crawl scope to apply"}, "allowed_domains": {"type":"array","items":{"type":"string"},"description":"Optional allowlisted domains for allowlisted_domains scope"}, "max_depth": {"type":"integer","description":"Maximum crawl depth"}, "max_pages": {"type":"integer","description":"Maximum number of pages to crawl"}, "capture_screenshots": {"type":"boolean","description":"Reserved for future screenshot capture"}, "change_detection": {"type":"boolean","description":"Reserved for future change detection controls"}}"#,
                    ),
                    "watch_page" => (
                        "Schedule periodic monitoring for a single page and summarize meaningful changes over time.",
                        r#"{"url": {"type":"string","description":"Page URL to monitor"}, "schedule": {"type":"object","description":"Structured schedule object. Examples: {\"kind\":\"at\",\"at\":\"2026-08-28T19:00:00+05:30\"}, {\"kind\":\"every\",\"seconds\":300}, {\"kind\":\"daily\",\"hour\":9,\"minute\":0,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"weekly\",\"weekday\":\"mon\",\"hour\":10,\"minute\":30,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"cron\",\"expr\":\"0 0 * * * *\",\"timezone\":\"Asia/Kolkata\"}"}, "agent_id": {"type":"string","description":"Agent that should execute the watch checks"}, "capture_screenshots": {"type":"boolean","description":"Whether to capture screenshots during checks"}, "change_detection": {"type":"boolean","description":"Whether to summarize only meaningful changes"}}"#,
                    ),
                    "watch_site" => (
                        "Schedule periodic monitoring for a site within the same domain and summarize meaningful changes.",
                        r#"{"url": {"type":"string","description":"Site URL to monitor"}, "schedule": {"type":"object","description":"Structured schedule object. Examples: {\"kind\":\"at\",\"at\":\"2026-08-28T19:00:00+05:30\"}, {\"kind\":\"every\",\"seconds\":300}, {\"kind\":\"daily\",\"hour\":9,\"minute\":0,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"weekly\",\"weekday\":\"mon\",\"hour\":10,\"minute\":30,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"cron\",\"expr\":\"0 0 * * * *\",\"timezone\":\"Asia/Kolkata\"}"}, "agent_id": {"type":"string","description":"Agent that should execute the watch checks"}, "capture_screenshots": {"type":"boolean","description":"Whether to capture screenshots during checks"}, "change_detection": {"type":"boolean","description":"Whether to summarize only meaningful changes"}}"#,
                    ),
                    "list_watch_jobs" => (
                        "List persisted page and site watch jobs for the current agent.",
                        r#"{}"#,
                    ),
                    "summarise_doc" => (
                        "Summarise a long document into concise bullet points.",
                        r#"{"text": {"type":"string","description":"Document text to summarise"}}"#,
                    ),
                    "query_rag" => (
                        "Query the local RAG knowledge base for relevant context about a topic.",
                        r#"{"query": {"type":"string","description":"Topic or question to search for"}}"#,
                    ),
                    "manage_cron" => (
                        "Manage scheduled jobs. Supports add, update, delete, list. Use a structured schedule object with kind=at/every/daily/weekly/cron. DO NOT use tool/agent prefixes in response tool field.",
                        r#"{"action": {"type":"string","enum":["add","update","delete","list"],"description":"CRUD action to perform"}, "id": {"type":"string","description":"Unique job ID (required for update/delete)"}, "agent_id": {"type":"string","description":"Agent ID to trigger"}, "prompt": {"type":"string","description":"Prompt to send"}, "schedule": {"type":"object","description":"Structured schedule object. Examples: {\"kind\":\"at\",\"at\":\"2026-08-28T19:00:00+05:30\"}, {\"kind\":\"every\",\"seconds\":120}, {\"kind\":\"daily\",\"hour\":19,\"minute\":30,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"weekly\",\"weekday\":\"sat\",\"hour\":11,\"minute\":0,\"interval_weeks\":2,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"cron\",\"expr\":\"0 30 19 * * *\",\"timezone\":\"Asia/Kolkata\"}"}}"#,
                    ),
                    "schedule_message" | "set_reminder" => (
                        "Schedule reminder behavior. Modes: notify (default, sends message at due time), defer (run task prompt at due time via agent), both (notify and defer).",
                        r#"{"task": {"type":"string","description":"Reminder text or deferred task prompt"}, "schedule": {"type":"object","description":"Structured schedule object. Examples: {\"kind\":\"at\",\"at\":\"2026-08-28T19:00:00+05:30\"}, {\"kind\":\"every\",\"seconds\":120}, {\"kind\":\"daily\",\"hour\":19,\"minute\":30,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"weekly\",\"weekday\":\"sat\",\"hour\":11,\"minute\":0,\"interval_weeks\":2,\"timezone\":\"Asia/Kolkata\"}, {\"kind\":\"cron\",\"expr\":\"0 30 19 * * *\",\"timezone\":\"Asia/Kolkata\"}"}, "mode": {"type":"string","enum":["notify","defer","both"],"description":"Execution mode"}, "deferred_prompt": {"type":"string","description":"Optional task prompt executed at trigger time when mode is defer/both"}, "agent_id": {"type":"string","description":"Agent to execute deferred task with"}}"#,
                    ),
                    _ => ("Execute a tool operation.", "{}"),
                };
                tool_registry
                    .register_with_embedding(
                    CachedTool {
                        name: tool_name.clone(),
                        description: desc.into(),
                        parameters_schema: schema.into(),
                        embedding: Vec::new(),
                        requires_strict_schema: false,
                        streaming_safe: false,
                        parallel_safe: true,
                        modalities: vec![aria_core::ToolModality::Text],
                    },
                    &embedder,
                )
                .unwrap_or_else(|e| panic!("invalid built-in tool schema for {}: {}", tool_name, e));
                // Index tool with real description text
                vector_store.index_tool_description(
                    tool_name.clone(), // Use clean tool name as ID
                    desc.to_string(),
                    embedder.embed(&format!("{} {}", tool_name, desc)),
                    tool_name,
                    vec![cfg.id.clone()],
                );
            }
        }
        info!(
            count = agent_store.len(),
            path = %config.agents_dir.path,
            "Loaded agent profiles"
        );
    }

    for tool_name in [
        "browser_profile_create",
        "browser_profile_list",
        "browser_profile_use",
        "browser_session_start",
        "browser_session_list",
        "browser_session_status",
        "browser_open",
        "browser_snapshot",
        "browser_extract",
        "browser_screenshot",
        "browser_act",
        "browser_download",
        "crawl_page",
        "crawl_site",
        "watch_page",
        "watch_site",
        "set_domain_access_decision",
    ] {
        register_discoverable_tool(
            &mut tool_registry,
            &mut vector_store,
            &*embedder,
            tool_name,
            "runtime",
        );
    }

    // Register meta tool: search_tool_registry
    let search_desc =
        "Search the tool registry and hot-swap the best matching tool for the current task.";
    tool_registry
        .register_with_embedding(
            CachedTool {
                name: "search_tool_registry".into(),
                description: search_desc.into(),
                parameters_schema:
                    r#"{"query": {"type":"string","description":"Description of the capability you need"}}"#
                        .into(),
                embedding: Vec::new(),
                requires_strict_schema: false,
                streaming_safe: false,
                parallel_safe: true,
                modalities: vec![aria_core::ToolModality::Text],
            },
            &embedder,
        )
        .unwrap_or_else(|e| panic!("invalid search_tool_registry schema: {}", e));
    vector_store.index_tool_description(
        "search_tool_registry", // Use clean tool name as ID
        search_desc.to_string(),
        embedder.embed("search tool registry find best tool capability"),
        "search_tool_registry",
        vec!["registry".into(), "meta".into()],
    );
    tool_registry
        .validate_strict_startup_contract()
        .unwrap_or_else(|e| panic!("tool registry startup validation failed: {}", e));
    // NOTE: sensor.bootstrap.imu removed — irrelevant for non-robotics agents.
    // Sensor annotations are only indexed when robotics_ctrl agent is active.
    let route_cfg = RouteConfig {
        confidence_threshold: config.router.confidence_threshold,
        tie_break_gap: config.router.tie_break_gap,
    };
    let router_index = router.build_index(route_cfg);
    let llm_pool = LlmBackendPool::new(
        vec!["primary".into(), "fallback".into()],
        Duration::from_secs(30),
    );
    // Initialize Credential Vault
    let master_key_raw = config.runtime.master_key.clone().unwrap_or_else(|| {
        eprintln!("[aria-x] Fatal: ARIA_MASTER_KEY is required");
        std::process::exit(1);
    });
    let mut master_key = [0u8; 32];
    let key_bytes = master_key_raw.as_bytes();
    for i in 0..32.min(key_bytes.len()) {
        master_key[i] = key_bytes[i];
    }
    let vault = Arc::new(CredentialVault::new(&config.vault.storage_path, master_key));

    // Check for --vault-set command
    if let Some(pos) = args.iter().position(|a| a == "--vault-set") {
        if args.len() > pos + 2 {
            let key_name = &args[pos + 1];
            let secret_value = &args[pos + 2];
            let allowed_domains = vec![
                "openrouter.ai".to_string(),
                "openai.com".to_string(),
                "anthropic.com".to_string(),
            ];
            if let Err(e) = vault.store_secret("system", key_name, secret_value, allowed_domains) {
                error!("Failed to store secret in vault: {}", e);
                std::process::exit(1);
            }
            info!("Successfully stored secret '{}' in vault", key_name);
            std::process::exit(0);
        } else {
            error!("Usage: --vault-set <key_name> <secret_value>");
            std::process::exit(1);
        }
    }

    let registry = Arc::new(Mutex::new(ProviderRegistry::new()));
    {
        let mut reg = registry.lock().await;
        reg.register(Arc::new(backends::ollama::OllamaProvider {
            base_url: config.runtime.ollama_host.clone(),
        }));

        // Resolve remote API keys: Vault -> Env -> Placeholder
        let openrouter_key = match vault.retrieve_global_secret("openrouter_key", "openrouter_ai") {
            Ok(_) => SecretRef::Vault {
                key_name: "openrouter_key".to_string(),
                vault: (*vault).clone(),
            },
            Err(_) => {
                if let Some(key) = config.runtime.openrouter_api_key.clone() {
                    SecretRef::Literal(key)
                } else {
                    SecretRef::Literal("sk-or-placeholder".to_string())
                }
            }
        };
        let openai_key = match vault.retrieve_global_secret("openai_key", "api.openai.com") {
            Ok(_) => SecretRef::Vault {
                key_name: "openai_key".to_string(),
                vault: (*vault).clone(),
            },
            Err(_) => SecretRef::Literal(
                config
                    .runtime
                    .openai_api_key
                    .clone()
                    .unwrap_or_else(|| "sk-openai-placeholder".to_string()),
            ),
        };
        let anthropic_key = match vault.retrieve_global_secret("anthropic_key", "api.anthropic.com")
        {
            Ok(_) => SecretRef::Vault {
                key_name: "anthropic_key".to_string(),
                vault: (*vault).clone(),
            },
            Err(_) => SecretRef::Literal(
                config
                    .runtime
                    .anthropic_api_key
                    .clone()
                    .unwrap_or_else(|| "sk-ant-placeholder".to_string()),
            ),
        };
        let gemini_key = match vault
            .retrieve_global_secret("gemini_key", "generativelanguage.googleapis.com")
        {
            Ok(_) => SecretRef::Vault {
                key_name: "gemini_key".to_string(),
                vault: (*vault).clone(),
            },
            Err(_) => SecretRef::Literal(
                config
                    .runtime
                    .gemini_api_key
                    .clone()
                    .unwrap_or_else(|| "gemini-placeholder".to_string()),
            ),
        };

        reg.register(Arc::new(backends::openrouter::OpenRouterProvider {
            api_key: openrouter_key,
            site_url: "aria-x".into(),
            site_title: "ARIA-X".into(),
        }));
        reg.register(Arc::new(backends::openai::OpenAiProvider {
            api_key: openai_key,
            base_url: "https://api.openai.com/v1".into(),
        }));
        reg.register(Arc::new(backends::anthropic::AnthropicProvider {
            api_key: anthropic_key,
            base_url: "https://api.anthropic.com/v1".into(),
        }));
        reg.register(Arc::new(backends::gemini::GeminiProvider {
            api_key: gemini_key,
            base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
        }));
    }

    match run_live_admin_inspect_command(&config, &args, &registry).await {
        Ok(Some(json)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json)
                    .unwrap_or_else(|_| "{\"error\":\"serialize failed\"}".into())
            );
            return;
        }
        Ok(None) => {}
        Err(err) => {
            eprintln!("[aria-x] Live inspect command failed: {}", err);
            std::process::exit(1);
        }
    }

    match config.llm.backend.to_lowercase().as_str() {
        "ollama" => {
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            let profile = resolve_model_capability_profile(
                &registry,
                Path::new(&config.ssmu.sessions_dir),
                Some(&config.llm),
                "ollama",
                &config.llm.model,
                now_us,
            )
            .await;
            if let Some(profile) = profile {
                let reg = registry.lock().await;
                if let Ok(ollama) = reg.create_backend_with_profile(&profile) {
                    llm_pool.register_backend(
                        "primary",
                        reg.create_backend_with_profile(&profile)
                            .unwrap_or_else(|_| Box::new(OllamaBackend::new(config.runtime.ollama_host.clone(), config.llm.model.clone()))),
                    );
                    llm_pool.register_backend("fallback", ollama);
                } else {
                    let ollama = OllamaBackend::new(config.runtime.ollama_host.clone(), config.llm.model.clone());
                    llm_pool.register_backend("primary", Box::new(ollama.clone()));
                    llm_pool.register_backend("fallback", Box::new(ollama));
                }
            } else {
                let ollama = OllamaBackend::new(config.runtime.ollama_host.clone(), config.llm.model.clone());
                llm_pool.register_backend("primary", Box::new(ollama.clone()));
                llm_pool.register_backend("fallback", Box::new(ollama));
            }
            info!(model = %config.llm.model, host = %config.runtime.ollama_host, "LLM: Ollama");
        }
        "openrouter" => {
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            let profile = resolve_model_capability_profile(
                &registry,
                Path::new(&config.ssmu.sessions_dir),
                Some(&config.llm),
                "openrouter",
                &config.llm.model,
                now_us,
            )
            .await;
            let reg = registry.lock().await;
            if let Some(profile) = profile {
                if let Ok(openrouter) = reg.create_backend_with_profile(&profile) {
                    llm_pool.register_backend("primary", openrouter.clone());
                    llm_pool.register_backend("fallback", openrouter);
                    info!(model = %config.llm.model, "LLM: OpenRouter (REST)");
                } else {
                    warn!("Failed to create OpenRouter backend with capability profile, falling back");
                    llm_pool.register_backend("primary", Box::new(LocalMockLLM));
                    llm_pool.register_backend("fallback", Box::new(LocalMockLLM));
                }
            } else if let Ok(openrouter) = reg.create_backend("openrouter", &config.llm.model) {
                llm_pool.register_backend("primary", openrouter.clone());
                llm_pool.register_backend("fallback", openrouter);
                info!(model = %config.llm.model, "LLM: OpenRouter (REST)");
            } else {
                warn!("Failed to create OpenRouter backend, falling back to mock");
                llm_pool.register_backend("primary", Box::new(LocalMockLLM));
                llm_pool.register_backend("fallback", Box::new(LocalMockLLM));
            }
        }
        "openai" | "anthropic" | "gemini" => {
            let provider_id = config.llm.backend.to_lowercase();
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            let profile = resolve_model_capability_profile(
                &registry,
                Path::new(&config.ssmu.sessions_dir),
                Some(&config.llm),
                &provider_id,
                &config.llm.model,
                now_us,
            )
            .await;
            let reg = registry.lock().await;
            if let Some(profile) = profile {
                if let Ok(backend) = reg.create_backend_with_profile(&profile) {
                    llm_pool.register_backend("primary", backend.clone());
                    llm_pool.register_backend("fallback", backend);
                    info!(provider = %provider_id, model = %config.llm.model, "LLM: remote provider");
                } else {
                    warn!(provider = %provider_id, "Failed to create backend with capability profile, falling back");
                    llm_pool.register_backend("primary", Box::new(LocalMockLLM));
                    llm_pool.register_backend("fallback", Box::new(LocalMockLLM));
                }
            } else if let Ok(backend) = reg.create_backend(&provider_id, &config.llm.model) {
                llm_pool.register_backend("primary", backend.clone());
                llm_pool.register_backend("fallback", backend);
                info!(provider = %provider_id, model = %config.llm.model, "LLM: remote provider");
            } else {
                warn!(provider = %provider_id, "Failed to create backend, falling back to mock");
                llm_pool.register_backend("primary", Box::new(LocalMockLLM));
                llm_pool.register_backend("fallback", Box::new(LocalMockLLM));
            }
        }
        _ => {
            llm_pool.register_backend("primary", Box::new(LocalMockLLM));
            llm_pool.register_backend("fallback", Box::new(LocalMockLLM));
            info!("LLM: mock (set backend=ollama/openrouter/openai/anthropic/gemini)");
        }
    }
    let llm_pool = Arc::new(llm_pool);

    // Initialize Session Memory
    let session_db_path = session_runtime_db_path(Path::new(&config.ssmu.sessions_dir));
    let session_memory = aria_ssmu::SessionMemory::new_sqlite_backed(100, &session_db_path);
    let load_report = session_memory
        .load_from_sqlite(&session_db_path)
        .or_else(|_| session_memory.load_from_dir(&config.ssmu.sessions_dir));
    if let Ok(report) = load_report {
        info!(
            loaded = report.loaded_sessions,
            skipped = report.skipped_files,
            "Loaded persisted sessions"
        );
        if report.loaded_sessions > 0 {
            let embedder_clone = Arc::clone(&embedder);
            let _ = session_memory
                .index_session_summaries_to(&mut vector_store, move |s| embedder_clone.embed(s));
            let _ = session_memory.save_to_sqlite(&session_db_path);
        }
    }
    // Build dynamic PageIndex: one node per loaded agent + bootstrap system nodes
    let page_index = build_dynamic_page_index(&agent_store);
    let page_index = Arc::new(page_index);
    let vector_store = Arc::new(vector_store);
    let session_tool_caches: Arc<SessionToolCacheStore> = Arc::new(SessionToolCacheStore::new(
        config.runtime.session_tool_cache_max_entries,
    ));

    // --- HookRegistry Setup for non-Telegram interfaces ---
    let session_locks = Arc::new(dashmap::DashMap::new());
    let embed_semaphore = Arc::new(tokio::sync::Semaphore::new(2));
    let mut hooks = HookRegistry::new();
    hooks.register_message_pre(Box::new(|req, vector_store, page_index| {
        let request_text = request_text_from_content(&req.content);
        Box::pin(async move {
            let hybrid =
                HybridMemoryEngine::new(&vector_store, &page_index, QueryPlannerConfig::default())
                    .retrieve(&request_text, &local_embed(&request_text, 64), 3, 3);
            let vector_context = hybrid.vector_context.join("\n");
            let page_context = hybrid
                .page_context
                .into_iter()
                .map(|n| format!("- {}: {}", n.title, n.summary))
                .collect::<Vec<_>>()
                .join("\n");
            let rag_context = format!(
                "Plan: {:?}\nVector Context:\n{}\n\nPageIndex Context:\n{}",
                hybrid.plan, vector_context, page_context
            );
            Ok(rag_context)
        })
    }));
    let hooks = Arc::new(hooks);

    // Build keyword index for BM25 hybrid search (RRF)
    let keyword_index = Arc::new(KeywordIndex::new().expect("Failed to create keyword index"));
    {
        // Batch-index all documents that are already in the vector store
        let mut kw_docs: Vec<(String, String)> = Vec::new();
        kw_docs.push((
            "workspace.files".into(),
            "File system tools: list files, read source code, navigate project structure.".into(),
        ));
        kw_docs.push((
            "workspace.rust".into(),
            "Rust development: cargo build, cargo test, compile crates, fix errors.".into(),
        ));
        kw_docs.push((
            "security.policy".into(),
            "Cedar policy engine: authorization decisions, access control, denied paths.".into(),
        ));
        for cfg in agent_store.all() {
            kw_docs.push((
                format!("agent.{}", cfg.id),
                format!("{} {}", cfg.description, cfg.system_prompt),
            ));
            for tool_name in &cfg.base_tool_names {
                if !runtime_exposes_base_tool(tool_name) {
                    continue;
                }
                let desc = match tool_name.as_str() {
                    "read_file" => "Read the contents of a file at the given path.",
                    "write_file" => "Write content to a file at the given path.",
                    "search_codebase" => "Search the codebase for a pattern or keyword.",
                    "run_tests" => "Run the test suite and return pass/fail results.",
                    "run_shell" => "Execute a shell command and return stdout/stderr output.",
                    "search_web" => "Search the web for information about a query.",
                    "fetch_url" => "Fetch the content of a URL and return it as text.",
                    "set_domain_access_decision" =>
                        "Persist a domain access decision for a target agent.",
                    "browser_profile_create" =>
                        "Create a managed browser profile for later browsing flows.",
                    "browser_profile_list" => "List managed browser profiles.",
                    "browser_profile_use" =>
                        "Bind a managed browser profile to the current session and agent.",
                    "browser_session_start" =>
                        "Launch a managed browser session using a stored profile.",
                    "browser_session_list" => "List managed browser sessions.",
                    "browser_session_status" => "Inspect a managed browser session record.",
                    "browser_session_cleanup" =>
                        "Mark stale launched browser sessions as exited after process death.",
                    "browser_session_persist_state" =>
                        "Persist encrypted browser session storage state for a managed session.",
                    "browser_session_restore_state" =>
                        "Restore encrypted browser session storage state for a managed session.",
                    "browser_session_pause" =>
                        "Pause a managed browser session after a challenge boundary.",
                    "browser_session_resume" =>
                        "Resume a previously paused managed browser session.",
                    "browser_session_record_challenge" =>
                        "Record a challenge event for a managed browser session.",
                    "browser_login_status" =>
                        "List persisted browser login state records for the current agent and session.",
                    "browser_login_begin_manual" =>
                        "Pause a managed browser session and mark manual login as pending.",
                    "browser_login_complete_manual" =>
                        "Mark a managed browser login flow as completed and authenticated.",
                    "browser_login_fill_credentials" =>
                        "Fill approved vault credentials into a managed browser session without exposing secret values to the model.",
                    "browser_open" => "Open a URL in a managed browser session.",
                    "browser_act" =>
                        "Perform a typed browser action against a managed browser session.",
                    "browser_snapshot" =>
                        "Persist an HTML snapshot for a page in a managed browser session.",
                    "browser_screenshot" =>
                        "Capture a real PNG screenshot for a page in a managed browser session.",
                    "browser_extract" =>
                        "Persist extracted page text for a page in a managed browser session.",
                    "browser_download" =>
                        "Download a URL into a managed browser session artifact with audit.",
                    "web_fetch" => "Fetch a URL over HTTP and return the raw response body.",
                    "web_extract" =>
                        "Fetch a URL over HTTP and return extracted page text.",
                    "crawl_page" =>
                        "Crawl a single page, extract text, and update website memory for the domain.",
                    "crawl_site" =>
                        "Crawl a site within scope, extract discovered pages, and update website memory.",
                    "watch_page" =>
                        "Schedule periodic monitoring for a single page and summarize meaningful changes.",
                    "watch_site" =>
                        "Schedule periodic monitoring for a site within the same domain and summarize meaningful changes.",
                    "list_watch_jobs" => "List persisted page and site watch jobs.",
                    "summarise_doc" => "Summarise a long document into concise bullet points.",
                    "query_rag" => "Query the local RAG knowledge base for relevant context.",
                    _ => "Execute a tool operation.",
                };
                kw_docs.push((format!("tool.{}", tool_name), desc.into()));
            }
        }
        kw_docs.push((
            "tool.search_tool_registry".into(),
            "Search the tool registry and hot-swap the best matching tool.".into(),
        ));
        if let Err(e) = keyword_index.add_documents_batch(&kw_docs) {
            warn!(error = %e, "Failed to populate keyword index");
        } else {
            info!(
                count = kw_docs.len(),
                "Keyword index populated for hybrid RAG"
            );
        }
    }

    // Initialize Credential Vault
    let master_key_raw = config.runtime.master_key.clone().unwrap_or_else(|| {
        eprintln!("[aria-x] Fatal: ARIA_MASTER_KEY is required");
        std::process::exit(1);
    });
    let mut master_key = [0u8; 32];
    let key_bytes = master_key_raw.as_bytes();
    for i in 0..32.min(key_bytes.len()) {
        master_key[i] = key_bytes[i];
    }
    let vault = Arc::new(CredentialVault::new(&config.vault.storage_path, master_key));

    // Check for --vault-set command
    if let Some(pos) = args.iter().position(|a| a == "--vault-set") {
        if args.len() > pos + 2 {
            let key_name = &args[pos + 1];
            let secret_value = &args[pos + 2];
            let allowed_domains = vec![
                "openrouter.ai".to_string(),
                "openai.com".to_string(),
                "anthropic.com".to_string(),
            ];
            if let Err(e) = vault.store_secret("system", key_name, secret_value, allowed_domains) {
                error!("Failed to store secret in vault: {}", e);
                std::process::exit(1);
            }
            info!("Successfully stored secret '{}' in vault", key_name);
            std::process::exit(0);
        } else {
            error!("Usage: --vault-set <key_name> <secret_value>");
            std::process::exit(1);
        }
    }

    let mut bad_patterns = vec![
        "sk-".to_string(),
        "ghp_".to_string(),
        "AKIA".to_string(),
        "ignore all previous instructions".to_string(),
        "system prompt".to_string(),
    ];
    // Add all secrets from the vault to the leak scanner patterns
    if let Ok(secrets) = vault.decrypt_all() {
        for s in secrets {
            if s.len() > 5 {
                bad_patterns.push(s);
            }
        }
    }
    let firewall = Arc::new(aria_safety::DfaFirewall::new(bad_patterns));

    let shared_config = Arc::clone(&config);
    let agent_store = Arc::new(agent_store);
    let tool_registry = Arc::new(tool_registry);

    // Initialise Scheduler early so it runs for all gateways
    let (tx_cron, rx_cron) = tokio::sync::mpsc::channel::<aria_intelligence::CronCommand>(64);
    if shared_config.scheduler.enabled {
        let boot_job_count = seed_scheduler_runtime_store(
            Path::new(&shared_config.ssmu.sessions_dir),
            &shared_config.scheduler.jobs,
        )
        .unwrap_or(0);
        info!(jobs = boot_job_count, "Scheduler enabled");
    } else {
        info!(
            "Scheduler preloaded jobs disabled; runtime scheduler remains active for dynamic reminders"
        );
    }
    let node_role = shared_config.node.role.clone();
    let _scheduler_commands = if node_supports_scheduler(&node_role) {
        Some(spawn_scheduler_command_processor(
            Path::new(&shared_config.ssmu.sessions_dir).to_path_buf(),
            rx_cron,
        ))
    } else {
        info!(role = %node_role, "Skipping scheduler command processor for non-scheduler node role");
        None
    };

    // Spawn Background Scheduler Processor
    let sc_config = Arc::clone(&shared_config);
    let sc_router_index = router_index.clone();
    let sc_embedder = Arc::clone(&embedder);
    let sc_llm_pool = Arc::clone(&llm_pool);
    let sc_cedar = Arc::clone(&cedar);
    let sc_agent_store = Arc::clone(&agent_store);
    let sc_tool_registry = Arc::clone(&tool_registry);
    let sc_session_memory = session_memory.clone();
    let sc_page_index = Arc::clone(&page_index);
    let sc_vector_store = Arc::clone(&vector_store);
    let sc_keyword_index = Arc::clone(&keyword_index);
    let sc_firewall = Arc::clone(&firewall);
    let sc_vault = Arc::clone(&vault);
    let sc_tx_cron = tx_cron.clone();
    let sc_registry = Arc::clone(&registry);
    let sc_caches = Arc::clone(&session_tool_caches);
    let sc_hooks = Arc::clone(&hooks);
    let sc_locks = Arc::clone(&session_locks);
    let sc_semaphore = Arc::clone(&embed_semaphore);
    let sc_worker_id = scheduler_worker_id(&sc_config);

    if node_supports_scheduler(&sc_config.node.role) {
    tokio::spawn(async move {
        info!(role = %sc_config.node.role, "Background scheduler processor started");
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut due_tick = tokio::time::interval(std::time::Duration::from_secs(
            sc_config.scheduler.tick_seconds.max(1),
        ));
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    debug!("Background scheduler heartbeat: alive");
                }
                _ = due_tick.tick() => {
                    let sessions_dir = std::path::Path::new(&sc_config.ssmu.sessions_dir);
                    let scheduler_shard = if sc_config.cluster.is_cluster() {
                        let total_shards = sc_config.cluster.scheduler_shards.max(1);
                        Some((
                            scheduler_shard_for_node(&sc_config.node.id, total_shards),
                            total_shards,
                        ))
                    } else {
                        None
                    };
                    if sc_config.cluster.is_cluster()
                        && sc_config.cluster.scheduler_shards <= 1
                        && !try_acquire_scheduler_leadership(
                            sessions_dir,
                            &sc_worker_id,
                            0,
                            sc_config.scheduler.tick_seconds.saturating_mul(4).max(30),
                        )
                        .await
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    let events = match poll_due_job_events_from_store(
                        sessions_dir,
                        &sc_worker_id,
                        sc_config.scheduler.tick_seconds.saturating_mul(4).max(30),
                        scheduler_shard,
                    )
                    .await {
                        Ok(events) => events,
                        Err(err) => {
                            error!(error = %err, "Failed to poll due job events from runtime store");
                            continue;
                        }
                    };
                    for ev in events {
                        info!(job_id = %ev.job_id, agent_id = %ev.agent_id, prompt = %ev.prompt, "Scheduled prompt fired (background)");

                        let session_id = execution_session_id_for_scheduled_event(&ev);
                        let session_uuid = uuid::Uuid::from_bytes(session_id);
                        let _ = sc_session_memory.update_overrides(
                            session_uuid,
                            Some(ev.agent_id.clone()),
                            None,
                        );

                        let req = aria_core::AgentRequest {
                            request_id: *uuid::Uuid::new_v4().as_bytes(),
                            session_id,
                            channel: ev.channel.unwrap_or(aria_core::GatewayChannel::Unknown),
                            user_id: ev.user_id.unwrap_or_else(|| "system".to_string()),
                            content: aria_core::MessageContent::Text(ev.prompt.clone()),
                            tool_runtime_policy: None,
                            timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                        };

                        if matches!(ev.kind, ScheduledJobKind::Notify) {
                            send_universal_response(&req, &ev.prompt, &sc_config).await;
                            let _ = sc_tx_cron
                                .send(aria_intelligence::CronCommand::UpdateStatus {
                                    id: ev.job_id.clone(),
                                    status: aria_intelligence::ScheduledJobStatus::Completed,
                                    detail: None,
                                    timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                                })
                                .await;
                            let _ = persist_scheduler_job_snapshot(
                                &sc_tx_cron,
                                sessions_dir,
                                &ev.job_id,
                            )
                            .await;
                            let _ = RuntimeStore::for_sessions_dir(sessions_dir)
                                .release_job_lease(&ev.job_id, &sc_worker_id);
                            continue;
                        }

                        match process_request(
                            &req,
                            &sc_config.learning,
                            &sc_router_index,
                            &*sc_embedder,
                            &sc_llm_pool,
                            &sc_cedar,
                            &*sc_agent_store,
                            &*sc_tool_registry,
                            &sc_session_memory,
                            &sc_page_index,
                            &sc_vector_store,
                            &sc_keyword_index,
                            &sc_firewall,
                            &sc_vault,
                            &sc_tx_cron,
                            &sc_registry,
                            sc_caches.as_ref(),
                            &*sc_hooks,
                            &sc_locks,
                            &sc_semaphore,
                            sc_config.llm.max_tool_rounds,
                            None,
                            Some(&Arc::new(std::sync::atomic::AtomicBool::new(false))),
                            std::path::Path::new(&sc_config.ssmu.sessions_dir),
                            sc_config.policy.whitelist.clone(),
                            sc_config.policy.forbid.clone(),
                            resolve_request_timezone(&sc_config, &req.user_id),
                        )
                        .await
                        {
                            Ok(aria_intelligence::OrchestratorResult::Completed(text)) => {
                                send_universal_response(&req, &text, &sc_config).await;
                                let _ = sc_tx_cron
                                    .send(aria_intelligence::CronCommand::UpdateStatus {
                                        id: ev.job_id.clone(),
                                        status: aria_intelligence::ScheduledJobStatus::Completed,
                                        detail: None,
                                        timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                                    })
                                    .await;
                                let _ = persist_scheduler_job_snapshot(
                                    &sc_tx_cron,
                                    sessions_dir,
                                    &ev.job_id,
                                )
                                .await;
                                let _ = RuntimeStore::for_sessions_dir(sessions_dir)
                                    .release_job_lease(&ev.job_id, &sc_worker_id);
                            }
                            Ok(aria_intelligence::OrchestratorResult::AgentElevationRequired { agent_id, message }) => {
                                let approval_result = aria_intelligence::OrchestratorResult::AgentElevationRequired {
                                    agent_id: agent_id.clone(),
                                    message: message.clone(),
                                };
                                let approval_text = persist_pending_approval_for_result(
                                    sessions_dir,
                                    &req,
                                    &approval_result,
                                )
                                .map(|(_, text)| text)
                                .unwrap_or(message);
                                send_universal_response(&req, &approval_text, &sc_config).await;
                                let _ = sc_tx_cron
                                    .send(aria_intelligence::CronCommand::UpdateStatus {
                                        id: ev.job_id.clone(),
                                        status: aria_intelligence::ScheduledJobStatus::ApprovalRequired,
                                        detail: Some(format!("Agent elevation required for {}", agent_id)),
                                        timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                                    })
                                    .await;
                                let _ = persist_scheduler_job_snapshot(
                                    &sc_tx_cron,
                                    sessions_dir,
                                    &ev.job_id,
                                )
                                .await;
                                let _ = RuntimeStore::for_sessions_dir(sessions_dir)
                                    .release_job_lease(&ev.job_id, &sc_worker_id);
                            }
                            Ok(result @ aria_intelligence::OrchestratorResult::ToolApprovalRequired { .. }) => {
                                let approval_text = persist_pending_approval_for_result(
                                    sessions_dir,
                                    &req,
                                    &result,
                                )
                                .map(|(_, text)| text)
                                .unwrap_or_else(|_| {
                                    "Scheduled task requires approval.".to_string()
                                });
                                send_universal_response(
                                    &req,
                                    &approval_text,
                                    &sc_config,
                                )
                                .await;
                                let _ = sc_tx_cron
                                    .send(aria_intelligence::CronCommand::UpdateStatus {
                                        id: ev.job_id.clone(),
                                        status:
                                            aria_intelligence::ScheduledJobStatus::ApprovalRequired,
                                        detail: Some(
                                            "Scheduled task requires approval".to_string(),
                                        ),
                                        timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                                    })
                                    .await;
                                let _ = persist_scheduler_job_snapshot(
                                    &sc_tx_cron,
                                    sessions_dir,
                                    &ev.job_id,
                                )
                                .await;
                                let _ = RuntimeStore::for_sessions_dir(sessions_dir)
                                    .release_job_lease(&ev.job_id, &sc_worker_id);
                            }
                            Err(e) => {
                                let detail = e.to_string();
                                error!(error = %detail, "Background scheduler orchestrator error");
                                let _ = sc_tx_cron
                                    .send(aria_intelligence::CronCommand::UpdateStatus {
                                        id: ev.job_id.clone(),
                                        status: aria_intelligence::ScheduledJobStatus::Failed,
                                        detail: Some(detail),
                                        timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                                    })
                                    .await;
                                let _ = persist_scheduler_job_snapshot(
                                    &sc_tx_cron,
                                    sessions_dir,
                                    &ev.job_id,
                                )
                                .await;
                                let _ = RuntimeStore::for_sessions_dir(sessions_dir)
                                    .release_job_lease(&ev.job_id, &sc_worker_id);
                            }
                        }
                    }
                }
            }
        }
    });
    } else {
        info!(role = %shared_config.node.role, "Skipping background scheduler processor for node role");
    }

    let ar_config = Arc::clone(&shared_config);
    let ar_router_index = router_index.clone();
    let ar_embedder = Arc::clone(&embedder);
    let ar_llm_pool = Arc::clone(&llm_pool);
    let ar_cedar = Arc::clone(&cedar);
    let ar_agent_store = Arc::clone(&agent_store);
    let ar_tool_registry = Arc::clone(&tool_registry);
    let ar_session_memory = session_memory.clone();
    let ar_page_index = Arc::clone(&page_index);
    let ar_vector_store = Arc::clone(&vector_store);
    let ar_keyword_index = Arc::clone(&keyword_index);
    let ar_firewall = Arc::clone(&firewall);
    let ar_vault = Arc::clone(&vault);
    let ar_tx_cron = tx_cron.clone();
    let ar_registry = Arc::clone(&registry);
    let ar_caches = Arc::clone(&session_tool_caches);
    let ar_hooks = Arc::clone(&hooks);
    let ar_locks = Arc::clone(&session_locks);
    let ar_semaphore = Arc::clone(&embed_semaphore);

    tokio::spawn(async move {
        info!("Background sub-agent processor started");
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            tick.tick().await;
            let sessions_dir = std::path::Path::new(&ar_config.ssmu.sessions_dir);
            match process_next_queued_agent_run(sessions_dir, |run| {
                let ar_config = Arc::clone(&ar_config);
                let ar_llm_pool = Arc::clone(&ar_llm_pool);
                let ar_cedar = Arc::clone(&ar_cedar);
                let ar_agent_store = Arc::clone(&ar_agent_store);
                let ar_tool_registry = Arc::clone(&ar_tool_registry);
                let ar_page_index = Arc::clone(&ar_page_index);
                let ar_vector_store = Arc::clone(&ar_vector_store);
                let ar_keyword_index = Arc::clone(&ar_keyword_index);
                let ar_firewall = Arc::clone(&ar_firewall);
                let ar_vault = Arc::clone(&ar_vault);
                let ar_registry = Arc::clone(&ar_registry);
                let ar_caches = Arc::clone(&ar_caches);
                let ar_hooks = Arc::clone(&ar_hooks);
                let ar_locks = Arc::clone(&ar_locks);
                let ar_semaphore = Arc::clone(&ar_semaphore);
                let ar_router_index = ar_router_index.clone();
                let ar_embedder = Arc::clone(&ar_embedder);
                let ar_session_memory = ar_session_memory.clone();
                let ar_tx_cron = ar_tx_cron.clone();
                async move {
                    let child_session_id = agent_run_session_id(&run.run_id);
                    let child_session_uuid = uuid::Uuid::from_bytes(child_session_id);
                    let _ = ar_session_memory.update_overrides(
                        child_session_uuid,
                        Some(run.agent_id.clone()),
                        None,
                    );
                    let req = aria_core::AgentRequest {
                        request_id: *uuid::Uuid::new_v4().as_bytes(),
                        session_id: child_session_id,
                        channel: aria_core::GatewayChannel::Unknown,
                        user_id: run.user_id.clone(),
                        content: aria_core::MessageContent::Text(run.request_text.clone()),
                        tool_runtime_policy: None,
                        timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
                    };

                    match process_request(
                        &req,
                        &ar_config.learning,
                        &ar_router_index,
                        &*ar_embedder,
                        &ar_llm_pool,
                        &ar_cedar,
                        &*ar_agent_store,
                        &*ar_tool_registry,
                        &ar_session_memory,
                        &ar_page_index,
                        &ar_vector_store,
                        &ar_keyword_index,
                        &ar_firewall,
                        &ar_vault,
                        &ar_tx_cron,
                        &ar_registry,
                        ar_caches.as_ref(),
                        &*ar_hooks,
                        &ar_locks,
                        &ar_semaphore,
                        ar_config.llm.max_tool_rounds,
                        None,
                        Some(&Arc::new(std::sync::atomic::AtomicBool::new(false))),
                        sessions_dir,
                        ar_config.policy.whitelist.clone(),
                        ar_config.policy.forbid.clone(),
                        resolve_request_timezone(&ar_config, &run.user_id),
                    )
                    .await
                    {
                        Ok(aria_intelligence::OrchestratorResult::Completed(text)) => Ok(text),
                        Ok(aria_intelligence::OrchestratorResult::AgentElevationRequired {
                            message,
                            ..
                        }) => Err(message),
                        Ok(aria_intelligence::OrchestratorResult::ToolApprovalRequired {
                            call,
                            ..
                        }) => Err(format!(
                            "sub-agent requires approval for tool '{}'",
                            call.name
                        )),
                        Err(err) => Err(err.to_string()),
                    }
                }
            })
            .await
            {
                Ok(Some(run)) => {
                    info!(
                        run_id = %run.run_id,
                        agent_id = %run.agent_id,
                        status = ?run.status,
                        "Processed queued sub-agent run"
                    );
                }
                Ok(None) => {}
                Err(err) => {
                    error!(error = %err, "Failed to process queued sub-agent run");
                }
            }
        }
    });

    let enabled_adapters = configured_gateway_adapters(&shared_config.gateway);
    let telegram_enabled = enabled_adapters.iter().any(|adapter| adapter == "telegram");
    let cli_enabled = enabled_adapters.iter().any(|adapter| adapter == "cli");
    let websocket_enabled = enabled_adapters.iter().any(|adapter| adapter == "websocket");
    let whatsapp_enabled = enabled_adapters.iter().any(|adapter| adapter == "whatsapp");
    let health_store_dir = shared_config.ssmu.sessions_dir.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let snapshots = crate::channel_health::snapshot_channel_health();
            if snapshots.is_empty() {
                continue;
            }
            let now_us = chrono::Utc::now().timestamp_micros() as u64;
            let _ = RuntimeStore::for_sessions_dir(Path::new(&health_store_dir))
                .append_channel_health_snapshot(&snapshots, now_us);
            info!(channels = ?snapshots, "Channel runtime health snapshot");
        }
    });
    if shared_config.features.outbox_delivery
        && shared_config
            .rollout
            .feature_enabled_for_node(&shared_config.node.id, "outbox_delivery")
        && node_supports_outbound(&shared_config.node.role)
    {
        let retry_config = Arc::clone(&shared_config);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match retry_failed_outbound_deliveries_once(&retry_config, 64).await {
                    Ok(recovered) if recovered > 0 => {
                        info!(recovered = recovered, "Recovered failed outbound deliveries");
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(error = %err, "Outbound retry worker failed");
                    }
                }
            }
        });
    } else if shared_config.features.outbox_delivery {
        info!(role = %shared_config.node.role, "Skipping outbound retry worker for non-outbound node role");
    }

    if !node_supports_ingress(&shared_config.node.role) {
        info!(role = %shared_config.node.role, "Ingress adapters disabled for non-ingress node role");
        return;
    }

    if !cli_enabled {
        if telegram_enabled {
            let tg_config = Arc::clone(&shared_config);
            let tg_runtime_config_path = runtime_config_path.clone();
            let tg_router_index = router_index.clone();
            let tg_embedder = Arc::clone(&embedder);
            let tg_llm_pool = Arc::clone(&llm_pool);
            let tg_cedar = Arc::clone(&cedar);
            let tg_agent_store = (*agent_store).clone();
            let tg_tool_registry = (*tool_registry).clone();
            let tg_session_memory = session_memory.clone();
            let tg_page_index = Arc::clone(&page_index);
            let tg_vector_store = Arc::clone(&vector_store);
            let tg_keyword_index = Arc::clone(&keyword_index);
            let tg_caches = Arc::clone(&session_tool_caches);
            let tg_firewall = Arc::clone(&firewall);
            let tg_vault = Arc::clone(&vault);
            let tg_tx_cron = tx_cron.clone();
            let tg_registry = Arc::clone(&registry);
            spawn_supervised_adapter("telegram", GatewayChannel::Telegram, move || {
                run_telegram_gateway(
                    Arc::clone(&tg_config),
                    tg_runtime_config_path.clone(),
                    tg_router_index.clone(),
                    Arc::clone(&tg_embedder),
                    Arc::clone(&tg_llm_pool),
                    Arc::clone(&tg_cedar),
                    tg_agent_store.clone(),
                    tg_tool_registry.clone(),
                    tg_session_memory.clone(),
                    Arc::clone(&tg_page_index),
                    Arc::clone(&tg_vector_store),
                    Arc::clone(&tg_keyword_index),
                    Arc::clone(&tg_caches),
                    Arc::clone(&tg_firewall),
                    Arc::clone(&tg_vault),
                    tg_tx_cron.clone(),
                    Arc::clone(&tg_registry),
                )
            });
        }
        if websocket_enabled {
            let ws_config = Arc::clone(&shared_config);
            let ws_router_index = router_index.clone();
            let ws_embedder = Arc::clone(&embedder);
            let ws_llm_pool = Arc::clone(&llm_pool);
            let ws_cedar = Arc::clone(&cedar);
            let ws_agent_store = (*agent_store).clone();
            let ws_tool_registry = (*tool_registry).clone();
            let ws_session_memory = session_memory.clone();
            let ws_page_index = Arc::clone(&page_index);
            let ws_vector_store = Arc::clone(&vector_store);
            let ws_keyword_index = Arc::clone(&keyword_index);
            let ws_caches = Arc::clone(&session_tool_caches);
            let ws_firewall = Arc::clone(&firewall);
            let ws_vault = Arc::clone(&vault);
            let ws_tx_cron = tx_cron.clone();
            let ws_registry = Arc::clone(&registry);
            let ws_session_locks = Arc::clone(&session_locks);
            let ws_embed_semaphore = Arc::clone(&embed_semaphore);
            spawn_supervised_adapter("websocket", GatewayChannel::WebSocket, move || {
                run_websocket_gateway(
                    Arc::clone(&ws_config),
                    ws_router_index.clone(),
                    Arc::clone(&ws_embedder),
                    Arc::clone(&ws_llm_pool),
                    Arc::clone(&ws_cedar),
                    ws_agent_store.clone(),
                    ws_tool_registry.clone(),
                    ws_session_memory.clone(),
                    Arc::clone(&ws_page_index),
                    Arc::clone(&ws_vector_store),
                    Arc::clone(&ws_keyword_index),
                    Arc::clone(&ws_caches),
                    Arc::clone(&ws_firewall),
                    Arc::clone(&ws_vault),
                    ws_tx_cron.clone(),
                    Arc::clone(&ws_registry),
                    Arc::clone(&ws_session_locks),
                    Arc::clone(&ws_embed_semaphore),
                )
            });
        }
        if whatsapp_enabled {
            let wa_config = Arc::clone(&shared_config);
            let wa_router_index = router_index.clone();
            let wa_embedder = Arc::clone(&embedder);
            let wa_llm_pool = Arc::clone(&llm_pool);
            let wa_cedar = Arc::clone(&cedar);
            let wa_agent_store = (*agent_store).clone();
            let wa_tool_registry = (*tool_registry).clone();
            let wa_session_memory = session_memory.clone();
            let wa_page_index = Arc::clone(&page_index);
            let wa_vector_store = Arc::clone(&vector_store);
            let wa_keyword_index = Arc::clone(&keyword_index);
            let wa_caches = Arc::clone(&session_tool_caches);
            let wa_firewall = Arc::clone(&firewall);
            let wa_vault = Arc::clone(&vault);
            let wa_tx_cron = tx_cron.clone();
            let wa_registry = Arc::clone(&registry);
            let wa_session_locks = Arc::clone(&session_locks);
            let wa_embed_semaphore = Arc::clone(&embed_semaphore);
            spawn_supervised_adapter("whatsapp", GatewayChannel::WhatsApp, move || {
                run_whatsapp_gateway(
                    Arc::clone(&wa_config),
                    wa_router_index.clone(),
                    Arc::clone(&wa_embedder),
                    Arc::clone(&wa_llm_pool),
                    Arc::clone(&wa_cedar),
                    wa_agent_store.clone(),
                    wa_tool_registry.clone(),
                    wa_session_memory.clone(),
                    Arc::clone(&wa_page_index),
                    Arc::clone(&wa_vector_store),
                    Arc::clone(&wa_keyword_index),
                    Arc::clone(&wa_caches),
                    Arc::clone(&wa_firewall),
                    Arc::clone(&wa_vault),
                    wa_tx_cron.clone(),
                    Arc::clone(&wa_registry),
                    Arc::clone(&wa_session_locks),
                    Arc::clone(&wa_embed_semaphore),
                )
            });
        }

        if telegram_enabled || websocket_enabled || whatsapp_enabled {
            core::future::pending::<()>().await;
            return;
        }
    }

    if telegram_enabled {
        let tg_config = Arc::clone(&shared_config);
        let tg_runtime_config_path = runtime_config_path.clone();
        let tg_router_index = router_index.clone();
        let tg_embedder = Arc::clone(&embedder);
        let tg_llm_pool = Arc::clone(&llm_pool);
        let tg_cedar = Arc::clone(&cedar);
        let tg_agent_store = (*agent_store).clone();
        let tg_tool_registry = (*tool_registry).clone();
        let tg_session_memory = session_memory.clone();
        let tg_page_index = Arc::clone(&page_index);
        let tg_vector_store = Arc::clone(&vector_store);
        let tg_keyword_index = Arc::clone(&keyword_index);
        let tg_caches = Arc::clone(&session_tool_caches);
        let tg_firewall = Arc::clone(&firewall);
        let tg_vault = Arc::clone(&vault);
        let tg_tx_cron = tx_cron.clone();
        let tg_registry = Arc::clone(&registry);
        spawn_supervised_adapter("telegram", GatewayChannel::Telegram, move || {
            run_telegram_gateway(
                Arc::clone(&tg_config),
                tg_runtime_config_path.clone(),
                tg_router_index.clone(),
                Arc::clone(&tg_embedder),
                Arc::clone(&tg_llm_pool),
                Arc::clone(&tg_cedar),
                tg_agent_store.clone(),
                tg_tool_registry.clone(),
                tg_session_memory.clone(),
                Arc::clone(&tg_page_index),
                Arc::clone(&tg_vector_store),
                Arc::clone(&tg_keyword_index),
                Arc::clone(&tg_caches),
                Arc::clone(&tg_firewall),
                Arc::clone(&tg_vault),
                tg_tx_cron.clone(),
                Arc::clone(&tg_registry),
            )
        });
    }

    if websocket_enabled {
        let ws_config = Arc::clone(&shared_config);
        let ws_router_index = router_index.clone();
        let ws_embedder = Arc::clone(&embedder);
        let ws_llm_pool = Arc::clone(&llm_pool);
        let ws_cedar = Arc::clone(&cedar);
        let ws_agent_store = (*agent_store).clone();
        let ws_tool_registry = (*tool_registry).clone();
        let ws_session_memory = session_memory.clone();
        let ws_page_index = Arc::clone(&page_index);
        let ws_vector_store = Arc::clone(&vector_store);
        let ws_keyword_index = Arc::clone(&keyword_index);
        let ws_caches = Arc::clone(&session_tool_caches);
        let ws_firewall = Arc::clone(&firewall);
        let ws_vault = Arc::clone(&vault);
        let ws_tx_cron = tx_cron.clone();
        let ws_registry = Arc::clone(&registry);
        let ws_session_locks = Arc::clone(&session_locks);
        let ws_embed_semaphore = Arc::clone(&embed_semaphore);
        spawn_supervised_adapter("websocket", GatewayChannel::WebSocket, move || {
            run_websocket_gateway(
                Arc::clone(&ws_config),
                ws_router_index.clone(),
                Arc::clone(&ws_embedder),
                Arc::clone(&ws_llm_pool),
                Arc::clone(&ws_cedar),
                ws_agent_store.clone(),
                ws_tool_registry.clone(),
                ws_session_memory.clone(),
                Arc::clone(&ws_page_index),
                Arc::clone(&ws_vector_store),
                Arc::clone(&ws_keyword_index),
                Arc::clone(&ws_caches),
                Arc::clone(&ws_firewall),
                Arc::clone(&ws_vault),
                ws_tx_cron.clone(),
                Arc::clone(&ws_registry),
                Arc::clone(&ws_session_locks),
                Arc::clone(&ws_embed_semaphore),
            )
        });
    }

    if whatsapp_enabled {
        let wa_config = Arc::clone(&shared_config);
        let wa_router_index = router_index.clone();
        let wa_embedder = Arc::clone(&embedder);
        let wa_llm_pool = Arc::clone(&llm_pool);
        let wa_cedar = Arc::clone(&cedar);
        let wa_agent_store = (*agent_store).clone();
        let wa_tool_registry = (*tool_registry).clone();
        let wa_session_memory = session_memory.clone();
        let wa_page_index = Arc::clone(&page_index);
        let wa_vector_store = Arc::clone(&vector_store);
        let wa_keyword_index = Arc::clone(&keyword_index);
        let wa_caches = Arc::clone(&session_tool_caches);
        let wa_firewall = Arc::clone(&firewall);
        let wa_vault = Arc::clone(&vault);
        let wa_tx_cron = tx_cron.clone();
        let wa_registry = Arc::clone(&registry);
        let wa_session_locks = Arc::clone(&session_locks);
        let wa_embed_semaphore = Arc::clone(&embed_semaphore);
        spawn_supervised_adapter("whatsapp", GatewayChannel::WhatsApp, move || {
            run_whatsapp_gateway(
                Arc::clone(&wa_config),
                wa_router_index.clone(),
                Arc::clone(&wa_embedder),
                Arc::clone(&wa_llm_pool),
                Arc::clone(&wa_cedar),
                wa_agent_store.clone(),
                wa_tool_registry.clone(),
                wa_session_memory.clone(),
                Arc::clone(&wa_page_index),
                Arc::clone(&wa_vector_store),
                Arc::clone(&wa_keyword_index),
                Arc::clone(&wa_caches),
                Arc::clone(&wa_firewall),
                Arc::clone(&wa_vault),
                wa_tx_cron.clone(),
                Arc::clone(&wa_registry),
                Arc::clone(&wa_session_locks),
                Arc::clone(&wa_embed_semaphore),
            )
        });
    }

    if !cli_enabled {
        error!(
            adapters = ?enabled_adapters,
            "No supported foreground gateway adapter enabled. Supported now: cli, telegram, websocket, whatsapp"
        );
        return;
    }

    // Wire Adapters — CLI mode
    let gateway = CliGateway;
    const CLI_INGRESS_QUEUE_CAPACITY: usize = 256;
    const CLI_INGRESS_PARTITIONS: usize = 4;
    let (ingress_bridge, ingress_receivers, ingress_metrics) =
        PartitionedIngressQueueBridge::<AgentRequest>::new(
            CLI_INGRESS_PARTITIONS,
            CLI_INGRESS_QUEUE_CAPACITY,
        );

    let worker_shared_config = Arc::clone(&shared_config);
    let worker_router_index = router_index.clone();
    let worker_embedder = Arc::clone(&embedder);
    let worker_llm_pool = Arc::clone(&llm_pool);
    let worker_cedar = Arc::clone(&cedar);
    let worker_agent_store = (*agent_store).clone();
    let worker_tool_registry = (*tool_registry).clone();
    let worker_session_memory = session_memory.clone();
    let worker_page_index = Arc::clone(&page_index);
    let worker_vector_store = Arc::clone(&vector_store);
    let worker_keyword_index = Arc::clone(&keyword_index);
    let worker_firewall = Arc::clone(&firewall);
    let worker_vault = Arc::clone(&vault);
    let worker_tx_cron = tx_cron.clone();
    let worker_registry = Arc::clone(&registry);
    let worker_session_tool_caches = Arc::clone(&session_tool_caches);
    let worker_hooks = Arc::clone(&hooks);
    let worker_session_locks = Arc::clone(&session_locks);
    let worker_embed_semaphore = Arc::clone(&embed_semaphore);
    let mut cli_ingress_workers = Vec::new();
    for (lane_idx, mut ingress_rx) in ingress_receivers.into_iter().enumerate() {
        let ingress_bridge_worker = ingress_bridge.lane(lane_idx);
        let worker_shared_config = Arc::clone(&worker_shared_config);
        let worker_router_index = worker_router_index.clone();
        let worker_embedder = Arc::clone(&worker_embedder);
        let worker_llm_pool = Arc::clone(&worker_llm_pool);
        let worker_cedar = Arc::clone(&worker_cedar);
        let worker_agent_store = worker_agent_store.clone();
        let worker_tool_registry = worker_tool_registry.clone();
        let worker_session_memory = worker_session_memory.clone();
        let worker_page_index = Arc::clone(&worker_page_index);
        let worker_vector_store = Arc::clone(&worker_vector_store);
        let worker_keyword_index = Arc::clone(&worker_keyword_index);
        let worker_firewall = Arc::clone(&worker_firewall);
        let worker_vault = Arc::clone(&worker_vault);
        let worker_tx_cron = worker_tx_cron.clone();
        let worker_registry = Arc::clone(&worker_registry);
        let worker_session_tool_caches = Arc::clone(&worker_session_tool_caches);
        let worker_hooks = Arc::clone(&worker_hooks);
        let worker_session_locks = Arc::clone(&worker_session_locks);
        let worker_embed_semaphore = Arc::clone(&worker_embed_semaphore);
        cli_ingress_workers.push(tokio::spawn(async move {
            while let Some(req) = ingress_rx.recv().await {
                ingress_bridge_worker.mark_dequeued();
                crate::channel_health::record_channel_health_event(
                    req.channel,
                    crate::channel_health::ChannelHealthEventKind::IngressDequeued,
                );
                process_cli_ingress_request(
                    &req,
                    &worker_shared_config,
                    &worker_router_index,
                    worker_embedder.as_ref(),
                    &worker_llm_pool,
                    &worker_cedar,
                    &worker_agent_store,
                    &worker_tool_registry,
                    &worker_session_memory,
                    &worker_page_index,
                    &worker_vector_store,
                    &worker_keyword_index,
                    &worker_firewall,
                    &worker_vault,
                    &worker_tx_cron,
                    &worker_registry,
                    worker_session_tool_caches.as_ref(),
                    worker_hooks.as_ref(),
                    &worker_session_locks,
                    &worker_embed_semaphore,
                )
                .await;
            }
        }));
    }

    info!("All subsystems wired (Gateway → Router → Orchestrator → Exec)");
    info!("Interactive CLI started (press Ctrl+C or send SIGTERM to exit)");

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM — shutting down gracefully");
                    }
                }
            } else {
                tokio::signal::ctrl_c().await.ok();
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
    };
    tokio::pin!(shutdown);
    loop {
        let req = tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            req_res = gateway.receive() => {
                match req_res {
                    Ok(r) => {
                        let request_text = request_text_from_content(&r.content);
                        if request_text.eq_ignore_ascii_case("exit") {
                            info!("Exiting...");
                            break;
                        }
                        r
                    },
                    Err(_) => continue,
                }
            }
        };
        let mut req = req;
        apply_session_scope_policy(&mut req, &shared_config);

        if let Some(reply) = handle_cli_approval_command(
            &req,
            &shared_config,
            &session_memory,
            &vault,
            &cedar,
            &tx_cron,
        )
        .await
        {
            send_universal_response(&req, &reply, &shared_config).await;
            continue;
        }

        if let Some(output) = handle_runtime_control_command(
            &req,
            &shared_config,
            &session_memory,
            None,
        )
        .await
        {
            send_universal_response(&req, &output.text, &shared_config).await;
            continue;
        }

        if let Some(reply) = handle_cli_control_command(
            &req,
            &shared_config,
            &*agent_store,
            &session_memory,
        ) {
            send_universal_response(&req, &reply, &shared_config).await;
            continue;
        }

        let key = req.session_id;
        if ingress_bridge.try_enqueue_by_key(req, &key).is_err() {
            crate::channel_health::record_channel_health_event(
                GatewayChannel::Cli,
                crate::channel_health::ChannelHealthEventKind::IngressDropped,
            );
            warn!(
                queue_depth = ingress_metrics
                    .iter()
                    .map(|metrics| metrics.queue_depth.load(Ordering::Relaxed))
                    .sum::<usize>(),
                "CLI ingress queue full/closed; dropping request"
            );
        } else {
            crate::channel_health::record_channel_health_event(
                GatewayChannel::Cli,
                crate::channel_health::ChannelHealthEventKind::IngressEnqueued,
            );
        }
    }

    drop(ingress_bridge);
    for worker in cli_ingress_workers {
        let _ = worker.await;
    }

    // Cleanup
    if let Ok(saved) = session_memory.save_to_sqlite(session_runtime_db_path(Path::new(
        &shared_config.ssmu.sessions_dir,
    ))) {
        info!(saved = saved, "Persisted sessions");
    }
    drop(session_memory);
    drop(page_index);
    drop(vector_store);
    drop(router);
    info!("Shutdown complete. Goodbye!");
}

fn run_channel_onboarding_command(
    config_path: &Path,
    args: &[String],
) -> Option<Result<String, String>> {
    if args.len() < 3 || args.get(1).map(String::as_str) != Some("channels") {
        return None;
    }
    let subcommand = args.get(2).map(String::as_str).unwrap_or_default();
    Some(match subcommand {
        "list" => list_configured_channels(config_path),
        "status" => list_channel_status(config_path),
        "add" => match args.get(3) {
            Some(channel) => add_configured_channel(config_path, channel),
            None => Err("Usage: channels add <channel>".into()),
        },
        "remove" => match args.get(3) {
            Some(channel) => remove_configured_channel(config_path, channel),
            None => Err("Usage: channels remove <channel>".into()),
        },
        _ => Err("Usage: channels <add|list|status|remove> [channel]".into()),
    })
}

fn list_configured_channels(config_path: &Path) -> Result<String, String> {
    let config = load_config(config_path.to_string_lossy().as_ref())
        .map_err(|e| format!("load config failed: {}", e))?;
    let manifests = configured_gateway_adapters(&config.gateway)
        .into_iter()
        .filter_map(|adapter| parse_gateway_channel_label(&adapter))
        .map(aria_core::builtin_channel_plugin_manifest)
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&manifests).map_err(|e| format!("serialize channels failed: {}", e))
}

fn list_channel_status(config_path: &Path) -> Result<String, String> {
    let config = load_config(config_path.to_string_lossy().as_ref())
        .map_err(|e| format!("load config failed: {}", e))?;
    let statuses = configured_gateway_adapters(&config.gateway)
        .into_iter()
        .map(|adapter| {
            let channel = parse_gateway_channel_label(&adapter).unwrap_or(aria_core::GatewayChannel::Unknown);
            let manifest = aria_core::builtin_channel_plugin_manifest(channel);
            let validation = aria_core::validate_channel_plugin_manifest(&manifest);
            serde_json::json!({
                "adapter": adapter,
                "plugin_id": manifest.plugin_id,
                "transport": manifest.transport,
                "approval_capable": manifest.approval_capable,
                "fallback_mode": manifest.fallback_mode,
                "valid": validation.is_ok(),
                "error": validation.err(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&statuses).map_err(|e| format!("serialize channel status failed: {}", e))
}

fn add_configured_channel(config_path: &Path, channel: &str) -> Result<String, String> {
    let normalized = channel.trim().to_ascii_lowercase();
    let parsed = parse_gateway_channel_label(&normalized)
        .ok_or_else(|| format!("unknown channel '{}'", channel))?;
    let manifest = aria_core::builtin_channel_plugin_manifest(parsed);
    aria_core::validate_channel_plugin_manifest(&manifest)?;
    let mut doc = std::fs::read_to_string(config_path)
        .map_err(|e| format!("read config failed: {}", e))?
        .parse::<toml_edit::DocumentMut>()
        .map_err(|e| format!("parse config failed: {}", e))?;
    if doc.get("gateway").is_none() {
        doc["gateway"] = toml_edit::Item::Table(toml_edit::Table::new());
    }
    let gateway = doc["gateway"]
        .as_table_mut()
        .ok_or_else(|| "gateway config must be a table".to_string())?;
    let current = gateway
        .get("adapters")
        .and_then(|item| item.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut adapters = current;
    if !adapters.iter().any(|entry| entry == &normalized) {
        adapters.push(normalized.clone());
        adapters.sort();
        adapters.dedup();
    }
    let mut arr = toml_edit::Array::default();
    for adapter in adapters {
        arr.push(adapter);
    }
    gateway["adapters"] = toml_edit::value(arr);
    std::fs::write(config_path, doc.to_string()).map_err(|e| format!("write config failed: {}", e))?;
    Ok(format!("added channel '{}'", normalized))
}

fn remove_configured_channel(config_path: &Path, channel: &str) -> Result<String, String> {
    let normalized = channel.trim().to_ascii_lowercase();
    let mut doc = std::fs::read_to_string(config_path)
        .map_err(|e| format!("read config failed: {}", e))?
        .parse::<toml_edit::DocumentMut>()
        .map_err(|e| format!("parse config failed: {}", e))?;
    if doc.get("gateway").is_none() {
        doc["gateway"] = toml_edit::Item::Table(toml_edit::Table::new());
    }
    let gateway = doc["gateway"]
        .as_table_mut()
        .ok_or_else(|| "gateway config must be a table".to_string())?;
    let current = gateway
        .get("adapters")
        .and_then(|item| item.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let filtered = current
        .into_iter()
        .filter(|entry| entry != &normalized)
        .collect::<Vec<_>>();
    let mut arr = toml_edit::Array::default();
    for adapter in filtered {
        arr.push(adapter);
    }
    gateway["adapters"] = toml_edit::value(arr);
    std::fs::write(config_path, doc.to_string()).map_err(|e| format!("write config failed: {}", e))?;
    Ok(format!("removed channel '{}'", normalized))
}

#[allow(clippy::too_many_arguments)]
async fn process_cli_ingress_request(
    req: &AgentRequest,
    config: &ResolvedAppConfig,
    router_index: &RouterIndex,
    embedder: &FastEmbedder,
    llm_pool: &Arc<LlmBackendPool>,
    cedar: &Arc<aria_policy::CedarEvaluator>,
    agent_store: &AgentConfigStore,
    tool_registry: &ToolManifestStore,
    session_memory: &aria_ssmu::SessionMemory,
    page_index: &Arc<PageIndexTree>,
    vector_store: &Arc<VectorStore>,
    keyword_index: &Arc<KeywordIndex>,
    firewall: &Arc<aria_safety::DfaFirewall>,
    vault: &Arc<aria_vault::CredentialVault>,
    tx_cron: &tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
    registry: &Arc<Mutex<ProviderRegistry>>,
    session_tool_caches: &SessionToolCacheStore,
    hooks: &HookRegistry,
    session_locks: &Arc<dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    embed_semaphore: &Arc<tokio::sync::Semaphore>,
) {
    if let Some(reply) = handle_cli_approval_command(
        req,
        config,
        session_memory,
        vault,
        cedar,
        tx_cron,
    )
    .await
    {
        send_universal_response(req, &reply, config).await;
        return;
    }

    if let Some(reply) = handle_cli_control_command(req, config, agent_store, session_memory) {
        send_universal_response(req, &reply, config).await;
        return;
    }

    if let Some(output) = handle_runtime_control_command(req, config, session_memory, None).await {
        send_universal_response(req, &output.text, config).await;
        return;
    }

    match process_request(
        req,
        &config.learning,
        router_index,
        embedder,
        llm_pool,
        cedar,
        agent_store,
        tool_registry,
        session_memory,
        page_index,
        vector_store,
        keyword_index,
        firewall,
        vault,
        tx_cron,
        registry,
        session_tool_caches,
        hooks,
        session_locks,
        embed_semaphore,
        config.llm.max_tool_rounds,
        None,
        Some(&Arc::new(std::sync::atomic::AtomicBool::new(false))),
        std::path::Path::new(&config.ssmu.sessions_dir),
        config.policy.whitelist.clone(),
        config.policy.forbid.clone(),
        resolve_request_timezone(config, &req.user_id),
    )
    .await
    {
        Ok(aria_intelligence::OrchestratorResult::Completed(text)) => {
            send_universal_response(req, &text, config).await;
        }
        Ok(aria_intelligence::OrchestratorResult::AgentElevationRequired { agent_id, message }) => {
            let result = aria_intelligence::OrchestratorResult::AgentElevationRequired {
                agent_id,
                message,
            };
            let approval_text = persist_pending_approval_for_result(
                Path::new(&config.ssmu.sessions_dir),
                req,
                &result,
            )
            .map(|(_, text)| text)
            .unwrap_or_else(|_| "Approval required.".to_string());
            send_universal_response(req, &approval_text, config).await;
        }
        Ok(aria_intelligence::OrchestratorResult::ToolApprovalRequired {
            call,
            pending_prompt,
        }) => {
            let result = aria_intelligence::OrchestratorResult::ToolApprovalRequired {
                call,
                pending_prompt,
            };
            let approval_text = persist_pending_approval_for_result(
                Path::new(&config.ssmu.sessions_dir),
                req,
                &result,
            )
            .map(|(_, text)| text)
            .unwrap_or_else(|_| "Approval required.".to_string());
            send_universal_response(req, &approval_text, config).await;
        }
        Err(e) => {
            let raw = e.to_string();
            let message = if let Ok((_, approval_text)) =
                persist_pending_approval_for_error(Path::new(&config.ssmu.sessions_dir), req, &raw)
            {
                format!(
                    "{}\n\n{}",
                    format_orchestrator_error_for_user(&raw),
                    approval_text
                )
            } else {
                format_orchestrator_error_for_user(&raw)
            };
            send_universal_response(req, &message, config).await;
            error!(error = %e, "Orchestrator error");
        }
    };
}

fn register_discoverable_tool(
    tool_registry: &mut ToolManifestStore,
    vector_store: &mut VectorStore,
    embedder: &impl EmbeddingModel,
    tool_name: &str,
    owner_tag: &str,
) {
    let (desc, schema) = match tool_name {
        "browser_profile_create" => (
            "Create a managed browser profile for later authenticated or read-only browsing.",
            r#"{"profile_id": {"type":"string","description":"Stable profile id"}, "display_name": {"type":"string","description":"Optional human-friendly name"}, "mode": {"type":"string","enum":["ephemeral","managed_persistent","attached_external","extension_bound"],"description":"Browser profile mode"}, "engine": {"type":"string","enum":["chromium","chrome","edge","safari_bridge"],"description":"Browser engine"}, "allowed_domains": {"type":"array","items":{"type":"string"}}, "auth_enabled": {"type":"boolean"}, "write_enabled": {"type":"boolean"}, "persistent": {"type":"boolean"}, "attached_source": {"type":"string"}, "extension_binding_id": {"type":"string"}}"#,
        ),
        "browser_profile_list" => ("List managed browser profiles available to the runtime.", r#"{}"#),
        "browser_profile_use" => ("Bind a managed browser profile to the current session and agent.", r#"{"profile_id":{"type":"string"}}"#),
        "browser_session_start" => ("Launch a managed browser session using a stored browser profile.", r#"{"profile_id":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_session_list" => ("List browser sessions for the current agent and session.", r#"{}"#),
        "browser_session_status" => ("Inspect a specific browser session record by id.", r#"{"browser_session_id":{"type":"string"}}"#),
        "browser_open" => ("Open a URL in a managed browser session.", r#"{"browser_session_id":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_snapshot" => ("Capture a DOM snapshot artifact for a URL or browser session.", r#"{"browser_session_id":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_extract" => ("Extract readable text from a URL or browser session.", r#"{"browser_session_id":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_screenshot" => ("Capture a screenshot artifact for a URL or browser session.", r#"{"browser_session_id":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_act" => ("Perform a typed browser action like navigate, click, type, select, scroll, or wait.", r#"{"browser_session_id":{"type":"string"},"action":{"type":"string"},"selector":{"type":"string"},"value":{"type":"string"},"url":{"type":"string"}}"#),
        "browser_download" => ("Download remote content through a managed browser workflow.", r#"{"browser_session_id":{"type":"string"},"url":{"type":"string"}}"#),
        "crawl_page" => ("Crawl a single page, extract content, and update website memory.", r#"{"url":{"type":"string"},"capture_screenshots":{"type":"boolean"},"change_detection":{"type":"boolean"}}"#),
        "crawl_site" => ("Crawl a site within the requested scope and update website memory.", r#"{"url":{"type":"string"},"scope":{"type":"string"},"allowed_domains":{"type":"array","items":{"type":"string"}},"max_depth":{"type":"integer"},"max_pages":{"type":"integer"},"capture_screenshots":{"type":"boolean"},"change_detection":{"type":"boolean"}}"#),
        "watch_page" => ("Schedule periodic monitoring for a single page.", r#"{"url":{"type":"string"},"schedule":{"type":"object"},"agent_id":{"type":"string"},"capture_screenshots":{"type":"boolean"},"change_detection":{"type":"boolean"}}"#),
        "watch_site" => ("Schedule periodic monitoring for a site.", r#"{"url":{"type":"string"},"schedule":{"type":"object"},"agent_id":{"type":"string"},"capture_screenshots":{"type":"boolean"},"change_detection":{"type":"boolean"}}"#),
        "set_domain_access_decision" => ("Persist a domain access decision for a target agent.", r#"{"domain":{"type":"string"},"decision":{"type":"string"},"action_family":{"type":"string"},"scope":{"type":"string"},"agent_id":{"type":"string"},"reason":{"type":"string"}}"#),
        _ => return,
    };

    tool_registry
        .register_with_embedding(
        CachedTool {
            name: tool_name.to_string(),
            description: desc.into(),
            parameters_schema: schema.into(),
            embedding: Vec::new(),
            requires_strict_schema: false,
            streaming_safe: false,
            parallel_safe: true,
            modalities: vec![aria_core::ToolModality::Text],
        },
        embedder,
    )
    .unwrap_or_else(|e| panic!("invalid discoverable tool schema for {}: {}", tool_name, e));
    vector_store.index_tool_description(
        tool_name.to_string(),
        desc.to_string(),
        embedder.embed(&format!("{} {}", tool_name, desc)),
        tool_name,
        vec![owner_tag.to_string()],
    );
}

fn handle_cli_control_command(
    req: &AgentRequest,
    config: &Config,
    agent_store: &AgentConfigStore,
    session_memory: &aria_ssmu::SessionMemory,
) -> Option<String> {
    handle_shared_control_command(req, config, agent_store, session_memory).map(|output| output.text)
}

async fn handle_runtime_control_command(
    req: &AgentRequest,
    config: &Config,
    session_memory: &aria_ssmu::SessionMemory,
    session_steering_tx: Option<
        &dashmap::DashMap<
            String,
            tokio::sync::mpsc::Sender<aria_intelligence::SteeringCommand>,
        >,
    >,
) -> Option<ControlCommandOutput> {
    let text = req.content.as_text()?.trim();
    let intent = aria_core::parse_control_intent(text, req.channel)?;
    let sessions_dir = Path::new(&config.ssmu.sessions_dir);
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    let session_uuid = uuid::Uuid::from_bytes(req.session_id);

    let plain = |text: String| ControlCommandOutput {
        text,
        parse_mode: None,
        reply_markup: None,
    };

    match intent {
        aria_core::ControlIntent::ListRuns => Some(match store.list_agent_runs_for_session(session_uuid) {
            Ok(runs) if runs.is_empty() => plain("No sub-agent runs found for this session.".into()),
            Ok(runs) => {
                let mut lines = vec!["Sub-agent runs for this session:".to_string()];
                for run in runs {
                    lines.push(format!(
                        "• {} [{}] agent={} created_at={}",
                        run.run_id,
                        serde_json::to_string(&run.status)
                            .unwrap_or_else(|_| "\"unknown\"".into())
                            .replace('"', ""),
                        run.agent_id,
                        run.created_at_us
                    ));
                }
                plain(lines.join("\n"))
            }
            Err(err) => plain(format!("Failed to list runs: {}", err)),
        }),
        aria_core::ControlIntent::InspectRun { run_id } => Some(match run_id {
            None => plain("Usage: /run <run_id>".into()),
            Some(run_id) => match store.read_agent_run(&run_id) {
                Ok(run) => plain(format!(
                    "Run {}\nstatus={}\nagent={}\nrequested_by={}\ncreated_at={}\nstarted_at={:?}\nfinished_at={:?}\nresult={}",
                    run.run_id,
                    serde_json::to_string(&run.status)
                        .unwrap_or_else(|_| "\"unknown\"".into())
                        .replace('"', ""),
                    run.agent_id,
                    run.requested_by_agent.unwrap_or_else(|| "user".into()),
                    run.created_at_us,
                    run.started_at_us,
                    run.finished_at_us,
                    run.result
                        .and_then(|r| r.response_summary.or(r.error))
                        .unwrap_or_else(|| "<none>".into())
                )),
                Err(err) => plain(format!("Failed to read run '{}': {}", run_id, err)),
            },
        }),
        aria_core::ControlIntent::InspectRunEvents { run_id } => Some(match run_id {
            None => plain("Usage: /run_events <run_id>".into()),
            Some(run_id) => match store.list_agent_run_events(&run_id) {
                Ok(events) if events.is_empty() => plain(format!("No events for run '{}'.", run_id)),
                Ok(events) => {
                    let mut lines = vec![format!("Events for run {}:", run_id)];
                    for event in events {
                        lines.push(format!(
                            "• {} [{}] {}",
                            event.event_id,
                            serde_json::to_string(&event.kind)
                                .unwrap_or_else(|_| "\"unknown\"".into())
                                .replace('"', ""),
                            event.summary
                        ));
                    }
                    plain(lines.join("\n"))
                }
                Err(err) => plain(format!("Failed to list run events: {}", err)),
            },
        }),
        aria_core::ControlIntent::InspectMailbox { run_id } => Some(match run_id {
            None => plain("Usage: /mailbox <run_id>".into()),
            Some(run_id) => match store.list_agent_mailbox_messages(&run_id) {
                Ok(messages) if messages.is_empty() => {
                    plain(format!("No mailbox messages for run '{}'.", run_id))
                }
                Ok(messages) => {
                    let mut lines = vec![format!("Mailbox for run {}:", run_id)];
                    for msg in messages {
                        lines.push(format!(
                            "• from={} to={} delivered={} {}",
                            msg.from_agent_id.as_deref().unwrap_or("unknown"),
                            msg.to_agent_id.as_deref().unwrap_or("unknown"),
                            msg.delivered,
                            msg.body
                        ));
                    }
                    plain(lines.join("\n"))
                }
                Err(err) => plain(format!("Failed to read mailbox: {}", err)),
            },
        }),
        aria_core::ControlIntent::CancelRun { run_id } => Some(match run_id {
            None => plain("Usage: /run_cancel <run_id>".into()),
            Some(run_id) => {
                let now_us = chrono::Utc::now().timestamp_micros() as u64;
                match store.cancel_agent_run(&run_id, "cancelled by user command", now_us) {
                    Ok(Some(run)) => plain(format!("Run '{}' is now {:?}.", run.run_id, run.status)),
                    Ok(None) => plain(format!("Run '{}' not found.", run_id)),
                    Err(err) => plain(format!("Failed to cancel run: {}", err)),
                }
            }
        }),
        aria_core::ControlIntent::RetryRun { run_id } => Some(match run_id {
            None => plain("Usage: /run_retry <run_id>".into()),
            Some(run_id) => match store.read_agent_run(&run_id) {
                Ok(original) => {
                    let now_us = chrono::Utc::now().timestamp_micros() as u64;
                    let new_run_id = format!("run-{}", uuid::Uuid::new_v4());
                    let retried = AgentRunRecord {
                        run_id: new_run_id.clone(),
                        parent_run_id: original
                            .parent_run_id
                            .clone()
                            .or_else(|| Some(original.run_id.clone())),
                        session_id: original.session_id,
                        user_id: original.user_id.clone(),
                        requested_by_agent: original.requested_by_agent.clone(),
                        agent_id: original.agent_id.clone(),
                        status: AgentRunStatus::Queued,
                        request_text: original.request_text.clone(),
                        inbox_on_completion: original.inbox_on_completion,
                        max_runtime_seconds: original.max_runtime_seconds,
                        created_at_us: now_us,
                        started_at_us: None,
                        finished_at_us: None,
                        result: None,
                    };
                    match store.upsert_agent_run(&retried, now_us) {
                        Err(err) => plain(format!("Failed to queue retry run: {}", err)),
                        Ok(()) => match store.append_agent_run_event(&AgentRunEvent {
                            event_id: format!("evt-{}", uuid::Uuid::new_v4()),
                            run_id: retried.run_id.clone(),
                            kind: AgentRunEventKind::Queued,
                            summary: format!("Run retried from '{}'", original.run_id),
                            created_at_us: now_us,
                        }) {
                            Err(err) => plain(format!("Retry run queued but event write failed: {}", err)),
                            Ok(()) => plain(format!(
                                "Retry queued: new run '{}' created from '{}'.",
                                retried.run_id, original.run_id
                            )),
                        },
                    }
                }
                Err(err) => plain(format!("Retry lookup failed: {}", err)),
            },
        }),
        aria_core::ControlIntent::InstallSkill { signed_module_json } => {
            Some(match signed_module_json {
                None => plain("Usage: /install_skill <SignedModule JSON>".into()),
                Some(json_part) => match serde_json::from_str::<aria_skill_runtime::SignedModule>(&json_part) {
                    Ok(signed) => {
                        if let Err(err) = aria_skill_runtime::verify_module(&signed) {
                            plain(format!("Verification failed: {}", err))
                        } else {
                            let hash = aria_skill_runtime::wasm_module_hash(&signed.bytes);
                            let hex_hash = hex::encode(&hash[..8]);
                            let target = format!("./tools/{}.wasm", hex_hash);
                            match std::fs::write(&target, &signed.bytes) {
                                Ok(()) => plain(format!("Skill installed successfully as '{}'.", target)),
                                Err(err) => plain(format!("Failed to save tool: {}", err)),
                            }
                        }
                    }
                    Err(err) => plain(format!("Invalid SignedModule JSON: {}", err)),
                },
            })
        }
        aria_core::ControlIntent::StopCurrent => Some(match session_steering_tx {
            Some(map) => {
                if let Some(tx) = map.get(&session_uuid.to_string()) {
                    let _ = tx.send(aria_intelligence::SteeringCommand::Abort).await;
                    plain("Signal sent: aborting current operation.".into())
                } else {
                    plain("No active operation to stop.".into())
                }
            }
            None => plain("Stop is not available on this runtime path.".into()),
        }),
        aria_core::ControlIntent::Pivot { instructions } => Some(match session_steering_tx {
            Some(map) => {
                let Some(instructions) = instructions else {
                    return Some(plain("Usage: /pivot <new instructions>".into()));
                };
                if let Some(tx) = map.get(&session_uuid.to_string()) {
                    let _ = tx
                        .send(aria_intelligence::SteeringCommand::Pivot(instructions.clone()))
                        .await;
                    plain("Signal sent: pivoting current operation.".into())
                } else {
                    plain("No active operation to pivot.".into())
                }
            }
            None => plain("Pivot is not available on this runtime path.".into()),
        }),
        _ => {
            let _ = session_memory;
            None
        }
    }
}

#[derive(Debug, Clone)]
struct ControlCommandOutput {
    text: String,
    parse_mode: Option<&'static str>,
    reply_markup: Option<serde_json::Value>,
}

fn render_agent_list_for_channel(
    channel: GatewayChannel,
    sessions_dir: &Path,
    agent_store: &AgentConfigStore,
    current_agent: Option<&str>,
) -> ControlCommandOutput {
    let presence_by_agent = RuntimeStore::for_sessions_dir(sessions_dir)
        .list_agent_presence()
        .unwrap_or_default()
        .into_iter()
        .map(|record| (record.agent_id.clone(), record))
        .collect::<std::collections::HashMap<_, _>>();
    match channel {
        GatewayChannel::Telegram => {
            let escape = |s: &str| -> String {
                s.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
            };
            let mut lines = vec!["<b>Available agents:</b>".to_string()];
            let mut keyboard = Vec::new();
            for cfg in agent_store.all() {
                let presence = presence_by_agent.get(&cfg.id);
                let presence_note = presence
                    .map(|record| {
                        format!(
                            " [{}{}]",
                            serde_json::to_string(&record.availability)
                                .unwrap_or_else(|_| "\"available\"".into())
                                .replace('"', ""),
                            if record.active_run_count == 0 {
                                String::new()
                            } else {
                                format!(", active={}", record.active_run_count)
                            }
                        )
                    })
                    .unwrap_or_default();
                lines.push(format!(
                    "• <b>{}</b>{}: {}",
                    escape(&cfg.id),
                    escape(&presence_note),
                    escape(&cfg.description)
                ));
                keyboard.push(vec![serde_json::json!({
                    "text": format!("Switch to {}", cfg.id),
                    "callback_data": format!("/agent {}", cfg.id)
                })]);
            }
            if let Some(agent) = current_agent {
                lines.push(format!("\n<b>Current agent:</b> {}", escape(agent)));
            }
            ControlCommandOutput {
                text: lines.join("\n"),
                parse_mode: Some("HTML"),
                reply_markup: Some(serde_json::json!({ "inline_keyboard": keyboard })),
            }
        }
        _ => {
            let mut lines = vec!["Available agents:".to_string()];
            for cfg in agent_store.all() {
                let presence = presence_by_agent.get(&cfg.id);
                let suffix = presence
                    .map(|record| {
                        let availability = serde_json::to_string(&record.availability)
                            .unwrap_or_else(|_| "\"available\"".into())
                            .replace('"', "");
                        if record.active_run_count == 0 {
                            format!(" [{}]", availability)
                        } else {
                            format!(" [{}, active={}]", availability, record.active_run_count)
                        }
                    })
                    .unwrap_or_default();
                lines.push(format!(" - {}{}: {}", cfg.id, suffix, cfg.description));
            }
            if let Some(agent) = current_agent {
                lines.push(format!("Current agent override: {}", agent));
            }
            ControlCommandOutput {
                text: lines.join("\n"),
                parse_mode: None,
                reply_markup: None,
            }
        }
    }
}

fn render_session_summary_for_channel(
    channel: GatewayChannel,
    session_uuid: uuid::Uuid,
    current_agent: Option<&str>,
    current_model: Option<&str>,
) -> ControlCommandOutput {
    match channel {
        GatewayChannel::Telegram => ControlCommandOutput {
            text: format!(
                "<b>Session</b> <code>{}</code>\nagent_override={}\nmodel_override={}",
                session_uuid,
                current_agent.unwrap_or("<default>"),
                current_model.unwrap_or("<default>"),
            ),
            parse_mode: Some("HTML"),
            reply_markup: None,
        },
        _ => ControlCommandOutput {
            text: format!(
                "Session {}\nagent_override={}\nmodel_override={}",
                session_uuid,
                current_agent.unwrap_or("<default>"),
                current_model.unwrap_or("<default>"),
            ),
            parse_mode: None,
            reply_markup: None,
        },
    }
}

fn render_pending_approvals_for_channel(
    channel: GatewayChannel,
    pending: Vec<(usize, aria_core::ApprovalRecord, ApprovalDisplayDescriptor, String)>,
) -> ControlCommandOutput {
    if pending.is_empty() {
        return ControlCommandOutput {
            text: "No pending approvals.".to_string(),
            parse_mode: None,
            reply_markup: None,
        };
    }

    match channel {
        GatewayChannel::Telegram => {
            let mut lines = vec!["<b>Pending approvals:</b>".to_string()];
            let mut keyboard = Vec::new();
            for (idx, record, descriptor, handle) in pending.into_iter().take(10) {
                let target = descriptor
                    .target_summary
                    .as_deref()
                    .map(|value| format!(" ({})", value))
                    .unwrap_or_default();
                lines.push(format!(
                    "{}. {}{} [<code>{}</code>]",
                    idx, descriptor.action_summary, target, handle
                ));
                keyboard.push(vec![
                    serde_json::json!({
                        "text": format!("Approve {}", idx),
                        "callback_data": format!("/approve {}", handle)
                    }),
                    serde_json::json!({
                        "text": format!("Deny {}", idx),
                        "callback_data": format!("/deny {}", handle)
                    }),
                    serde_json::json!({
                        "text": record.tool_name,
                        "callback_data": format!("/approve {}", handle)
                    }),
                ]);
            }
            ControlCommandOutput {
                text: lines.join("\n"),
                parse_mode: Some("HTML"),
                reply_markup: Some(serde_json::json!({ "inline_keyboard": keyboard })),
            }
        }
        _ => {
            let mut lines = vec!["Pending approvals:".to_string()];
            for (idx, record, descriptor, handle) in pending {
                let target = descriptor
                    .target_summary
                    .as_deref()
                    .map(|value| format!(" ({})", value))
                    .unwrap_or_default();
                lines.push(format!(
                    " {}. {}{} [#{} | {}]",
                    idx, descriptor.action_summary, target, handle, record.approval_id
                ));
            }
            lines.push(
                "Approve with `/approve <number>`, `/approve <handle>`, or `/approve <approval_id>`."
                    .to_string(),
            );
            lines.push(
                "Deny with `/deny <number>`, `/deny <handle>`, or `/deny <approval_id>`."
                    .to_string(),
            );
            ControlCommandOutput {
                text: lines.join("\n"),
                parse_mode: None,
                reply_markup: None,
            }
        }
    }
}

fn handle_shared_control_command(
    req: &AgentRequest,
    config: &Config,
    agent_store: &AgentConfigStore,
    session_memory: &aria_ssmu::SessionMemory,
) -> Option<ControlCommandOutput> {
    let text = req.content.as_text()?.trim();
    if text.is_empty() {
        return None;
    }
    let intent = aria_core::parse_control_intent(text, req.channel)?;
    let session_uuid = uuid::Uuid::from_bytes(req.session_id);
    let (current_agent, current_model) = get_effective_session_overrides(
        session_memory,
        req.session_id,
        req.channel,
        &req.user_id,
    )
    .unwrap_or((None, None));
    let current_agent = normalize_override_value(current_agent);
    let current_model = normalize_override_value(current_model);

    match intent {
        aria_core::ControlIntent::ListAgents => {
            return Some(render_agent_list_for_channel(
                req.channel,
                Path::new(&config.ssmu.sessions_dir),
                agent_store,
                current_agent.as_deref(),
            ));
        }
        aria_core::ControlIntent::InspectSession => {
            return Some(render_session_summary_for_channel(
                req.channel,
                session_uuid,
                current_agent.as_deref(),
                current_model.as_deref(),
            ));
        }
        aria_core::ControlIntent::ListApprovals => {
            let pending = list_cli_pending_approvals(
                Path::new(&config.ssmu.sessions_dir),
                req.session_id,
                &req.user_id,
            );
            return Some(render_pending_approvals_for_channel(req.channel, pending));
        }
        aria_core::ControlIntent::SwitchAgent {
            agent_id: Some(agent_name),
        } => {
            if matches!(agent_name.as_str(), "clear" | "reset") {
                let _ = persist_session_overrides(
                    session_memory,
                    req.session_id,
                    req.channel,
                    &req.user_id,
                    Some(String::new()),
                    Some(String::new()),
                );
                record_learning_reward(
                    &config.learning,
                    Path::new(&config.ssmu.sessions_dir),
                    req.request_id,
                    req.session_id,
                    RewardKind::OverrideApplied,
                    Some("agent override cleared".to_string()),
                    req.timestamp_us,
                );
                return Some(ControlCommandOutput {
                    text: match req.channel {
                        GatewayChannel::Telegram => {
                            "Override cleared. Session is now using the default omni routing."
                                .to_string()
                        }
                        _ => {
                            "Override cleared. Session is now using the default omni routing."
                                .to_string()
                        }
                    },
                    parse_mode: None,
                    reply_markup: None,
                });
            }
            if agent_store.get(&agent_name).is_some() {
                let _ = persist_session_overrides(
                    session_memory,
                    req.session_id,
                    req.channel,
                    &req.user_id,
                    Some(agent_name.clone()),
                    None,
                );
                record_learning_reward(
                    &config.learning,
                    Path::new(&config.ssmu.sessions_dir),
                    req.request_id,
                    req.session_id,
                    RewardKind::OverrideApplied,
                    Some(format!("agent override set to {}", agent_name)),
                    req.timestamp_us,
                );
                return Some(ControlCommandOutput {
                    text: format!("Session override set to agent: {}.", agent_name),
                    parse_mode: None,
                    reply_markup: None,
                });
            }
            return Some(ControlCommandOutput {
                text: format!("Agent '{}' not found. Use /agents to list.", agent_name),
                parse_mode: None,
                reply_markup: None,
            });
        }
        aria_core::ControlIntent::SwitchAgent { agent_id: None } => {
            return Some(ControlCommandOutput {
                text:
                    "Usage: /agent <persona_name> (for example: /agent developer, /agent omni)"
                        .to_string(),
                parse_mode: None,
                reply_markup: None,
            });
        }
        _ => {}
    }

    // Parsed as control intent but not handled by CLI control router;
    // caller may route it to dedicated handlers (e.g. approval flow).
    if text.starts_with('/') {
        return None;
    }

    None
}

fn list_cli_pending_approvals(
    sessions_dir: &Path,
    session_id: [u8; 16],
    user_id: &str,
) -> Vec<(usize, aria_core::ApprovalRecord, ApprovalDisplayDescriptor, String)> {
    RuntimeStore::for_sessions_dir(sessions_dir)
        .list_approvals(
            Some(session_id),
            Some(user_id),
            Some(aria_core::ApprovalStatus::Pending),
        )
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, record)| {
            let descriptor = build_approval_descriptor(&record);
            let handle = ensure_approval_handle(sessions_dir, &record)
                .unwrap_or_else(|_| record.approval_id.clone());
            (idx + 1, record, descriptor, handle)
        })
        .collect()
}

fn resolve_cli_approval_id(
    sessions_dir: &Path,
    session_id: [u8; 16],
    user_id: &str,
    token: &str,
) -> Result<String, String> {
    if token.chars().all(|c| c.is_ascii_digit()) {
        let index = token
            .parse::<usize>()
            .map_err(|_| format!("Invalid approval selection '{}'.", token))?;
        let pending = list_cli_pending_approvals(sessions_dir, session_id, user_id);
        let Some((_, record, _, _)) = pending.into_iter().find(|(idx, _, _, _)| *idx == index) else {
            return Err(format!("No pending approval at index {}.", index));
        };
        Ok(record.approval_id)
    } else {
        resolve_approval_selector(sessions_dir, session_id, user_id, token)
    }
}

fn apply_session_scope_policy(req: &mut AgentRequest, config: &Config) {
    let scoped = aria_core::derive_scoped_session_id(
        req.session_id,
        req.channel,
        &req.user_id,
        config.gateway.session_scope_policy,
    );
    req.session_id = scoped;
}

async fn handle_cli_approval_command(
    req: &AgentRequest,
    config: &Config,
    session_memory: &aria_ssmu::SessionMemory,
    vault: &Arc<aria_vault::CredentialVault>,
    cedar: &Arc<aria_policy::CedarEvaluator>,
    tx_cron: &tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
) -> Option<String> {
    let text = req.content.as_text()?.trim();
    if text.is_empty() {
        return None;
    }
    let (approving, selector) = match aria_core::parse_control_intent(text, req.channel) {
        Some(aria_core::ControlIntent::ResolveApproval {
            decision,
            target: Some(target),
            ..
        }) => (
            matches!(decision, aria_core::ApprovalResolutionDecision::Approve),
            target,
        ),
        Some(aria_core::ControlIntent::ResolveApproval { target: None, .. }) => {
            return Some(
                "Usage: /approve <approval_id|number> or /deny <approval_id|number>".to_string(),
            );
        }
        _ => return None,
    };

    let sessions_dir = Path::new(&config.ssmu.sessions_dir);
    let approval_id =
        match resolve_cli_approval_id(sessions_dir, req.session_id, &req.user_id, &selector) {
            Ok(id) => id,
            Err(err) => return Some(err),
        };
    let decision = if approving {
        aria_core::ApprovalResolutionDecision::Approve
    } else {
        aria_core::ApprovalResolutionDecision::Deny
    };
    let record = match resolve_approval_record(sessions_dir, &approval_id, decision) {
        Ok(record) => record,
        Err(err) => return Some(err),
    };

    if !approving {
        return Some(format!("Denied approval '{}'.", approval_id));
    }
    if record.tool_name == AGENT_ELEVATION_TOOL_NAME {
        let requested_agent = serde_json::from_str::<serde_json::Value>(&record.arguments_json)
            .ok()
            .and_then(|value| {
                value
                    .get("agent_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| record.agent_id.clone());
        let now_us = chrono::Utc::now().timestamp_micros() as u64;
        let grant = aria_core::ElevationGrant {
            session_id: req.session_id,
            user_id: req.user_id.clone(),
            agent_id: requested_agent.clone(),
            granted_at_us: now_us,
            expires_at_us: Some(now_us + 3_600_000_000),
        };
        let _ = write_elevation_grant(sessions_dir, &grant);
        let _ = persist_session_overrides(
            session_memory,
            req.session_id,
            req.channel,
            &req.user_id,
            Some(requested_agent.clone()),
            None,
        );
        return Some(format!(
            "Approved elevation for agent '{}'.",
            requested_agent
        ));
    }
    let (current_agent, _) = get_effective_session_overrides(
        session_memory,
        req.session_id,
        req.channel,
        &req.user_id,
    )
    .unwrap_or((None, None));
    let invoking_agent = normalize_override_value(current_agent).unwrap_or_else(|| "omni".into());
    let executor = MultiplexToolExecutor::new(
        vault.clone(),
        invoking_agent,
        req.session_id,
        req.user_id.clone(),
        req.channel,
        tx_cron.clone(),
        session_memory.clone(),
        cedar.clone(),
        sessions_dir.to_path_buf(),
        None,
        None,
        resolve_request_timezone(config, &req.user_id),
    );
    let call = aria_intelligence::ToolCall {
        invocation_id: None,
        name: record.tool_name.clone(),
        arguments: record.arguments_json.clone(),
    };
    let result = executor.execute(&call).await;
    Some(match result {
        Ok(value) => format!(
            "Approved '{}'.\n{}",
            record.tool_name,
            value.render_for_prompt()
        ),
        Err(err) => format!(
            "Approved '{}', but execution failed: {}",
            record.tool_name, err
        ),
    })
}

pub(crate) fn format_orchestrator_error_for_user(message: &str) -> String {
    if let Some(path) = message
        .strip_prefix("tool error: tool 'read_file' denied by policy for resource '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!(
            "Access denied: read_file is not permitted for '{}'.",
            path
        );
    }
    if let Some(path) = message
        .strip_prefix("tool error: tool 'write_file' denied by policy for resource '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!(
            "Access denied: write_file is not permitted for '{}'.",
            path
        );
    }
    if let Some(resource) = message
        .strip_prefix("tool error: policy denied action 'web_domain_fetch' on resource '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!(
            "Domain access is not approved for '{}'. Approve the domain first, then retry.",
            resource
        );
    }
    if let Some(resource) = message
        .strip_prefix("tool error: policy denied action 'web_domain_crawl' on resource '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!(
            "Crawl access is not approved for '{}'. Approve the domain first, then retry.",
            resource
        );
    }
    if let Some(resource) = message
        .strip_prefix("tool error: policy denied action 'browser_profile_use' on resource '")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return format!(
            "Browser profile access denied for '{}'.",
            resource
        );
    }
    if let Some(tool) = message.strip_prefix("tool error: APPROVAL_REQUIRED::") {
        return format!(
            "Approval required before '{}' can run. Inspect pending approvals and approve the request, then retry.",
            tool
        );
    }
    message.to_string()
}
