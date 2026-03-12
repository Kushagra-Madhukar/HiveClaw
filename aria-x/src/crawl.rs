async fn crawl_urls(
    seed_url: &str,
    scope: aria_core::CrawlScope,
    max_depth: u16,
    max_pages: u32,
    allowed_domains: &[String],
) -> Result<Vec<(reqwest::Url, String)>, OrchestratorError> {
    let seed = reqwest::Url::parse(seed_url)
        .map_err(|e| OrchestratorError::ToolError(format!("Invalid URL '{}': {}", seed_url, e)))?;
    let seed_domain = url_host_key(&seed)?;
    let allowed: HashSet<String> = if allowed_domains.is_empty() {
        std::iter::once(seed_domain.clone()).collect()
    } else {
        allowed_domains.iter().map(|d| d.to_ascii_lowercase()).collect()
    };

    let mut queue = VecDeque::from([(seed.clone(), 0u16)]);
    let mut seen = HashSet::new();
    let mut pages = Vec::new();

    while let Some((url, depth)) = queue.pop_front() {
        if pages.len() >= max_pages as usize {
            break;
        }
        let url_key = url.as_str().to_string();
        if !seen.insert(url_key) {
            continue;
        }
        let domain = url_host_key(&url)?;
        if !allowed.contains(&domain) {
            continue;
        }
        let (body, _content_type) = fetch_web_document(url.as_str()).await?;
        pages.push((url.clone(), body.clone()));

        if depth >= max_depth || matches!(scope, aria_core::CrawlScope::SinglePage) {
            continue;
        }

        for next in extract_html_links(&url, &body) {
            let next_domain = url_host_key(&next)?;
            let permitted = match scope {
                aria_core::CrawlScope::SinglePage => false,
                aria_core::CrawlScope::SameOrigin => next_domain == seed_domain,
                aria_core::CrawlScope::AllowlistedDomains => allowed.contains(&next_domain),
                aria_core::CrawlScope::ScheduledWatchAllowed => next_domain == seed_domain,
            };
            if permitted {
                queue.push_back((next, depth + 1));
            }
        }
    }

    Ok(pages)
}

fn update_website_memory_from_crawl(
    store: &RuntimeStore,
    sessions_dir: &Path,
    seed: &reqwest::Url,
    pages: &[(reqwest::Url, String)],
    action_label: &str,
) -> Result<(aria_core::WebsiteMemoryRecord, Vec<String>), OrchestratorError> {
    let domain = url_host_key(seed)?;
    let existing = store
        .list_website_memory(Some(&domain))
        .map_err(OrchestratorError::ToolError)?
        .into_iter()
        .next();
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    let mut known_paths: Vec<String> = existing
        .as_ref()
        .map(|record| record.known_paths.clone())
        .unwrap_or_default();
    let mut successful_actions: Vec<String> = existing
        .as_ref()
        .map(|record| record.last_successful_actions.clone())
        .unwrap_or_default();
    let mut page_hashes: BTreeMap<String, String> = existing
        .as_ref()
        .map(|record| record.page_hashes.clone())
        .unwrap_or_default();
    let mut changed_paths = Vec::new();

    for (url, _) in pages {
        let path = path_for_url(url);
        if !known_paths.iter().any(|value| value == &path) {
            known_paths.push(path);
        }
    }
    if !successful_actions.iter().any(|value| value == action_label) {
        successful_actions.push(action_label.to_string());
    }
    for (url, body) in pages {
        let path = path_for_url(url);
        let text = extract_html_content_for_url(Some(url.as_str()), body).text;
        let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
        let previous = page_hashes.insert(path.clone(), hash.clone());
        if previous.as_deref() != Some(hash.as_str()) {
            changed_paths.push(path);
        }
    }

    let record = aria_core::WebsiteMemoryRecord {
        record_id: existing
            .as_ref()
            .map(|record| record.record_id.clone())
            .unwrap_or_else(|| format!("site-{}", domain)),
        domain,
        canonical_home_url: format!("{}://{}", seed.scheme(), seed.host_str().unwrap_or_default()),
        known_paths,
        known_selectors: existing
            .as_ref()
            .map(|record| record.known_selectors.clone())
            .unwrap_or_default(),
        known_login_entrypoints: existing
            .as_ref()
            .map(|record| record.known_login_entrypoints.clone())
            .unwrap_or_default(),
        known_search_patterns: existing
            .as_ref()
            .map(|record| record.known_search_patterns.clone())
            .unwrap_or_default(),
        last_successful_actions: successful_actions,
        page_hashes,
        render_required: existing
            .as_ref()
            .map(|record| record.render_required)
            .unwrap_or(false),
        challenge_frequency: existing
            .as_ref()
            .map(|record| record.challenge_frequency)
            .unwrap_or(aria_core::BrowserChallengeFrequency::Unknown),
        last_seen_at_us: now_us,
        updated_at_us: now_us,
    };
    store
        .upsert_website_memory(&record, now_us)
        .map_err(OrchestratorError::ToolError)?;
    let _ = enforce_web_storage_policy(sessions_dir);
    Ok((record, changed_paths))
}

fn capture_crawl_screenshot_artifact(
    sessions_dir: &Path,
    session_id: aria_core::Uuid,
    agent_id: &str,
    crawl_id: &str,
    target_url: &str,
) -> Result<aria_core::BrowserArtifactRecord, OrchestratorError> {
    let artifact_dir = browser_session_artifacts_root(sessions_dir, crawl_id);
    std::fs::create_dir_all(&artifact_dir).map_err(|e| {
        OrchestratorError::ToolError(format!(
            "Failed to prepare crawl screenshot directory '{}': {}",
            artifact_dir.display(),
            e
        ))
    })?;
    let profile_dir = artifact_dir.join("profile");
    std::fs::create_dir_all(&profile_dir).map_err(|e| {
        OrchestratorError::ToolError(format!(
            "Failed to prepare crawl screenshot profile directory '{}': {}",
            profile_dir.display(),
            e
        ))
    })?;
    let artifact_path = artifact_dir.join(format!("screenshot-{}.png", uuid::Uuid::new_v4()));
    let command = build_browser_screenshot_command(
        aria_core::BrowserEngine::Chromium,
        &profile_dir,
        target_url,
        &artifact_path,
    )?;
    run_browser_command(&command)?;
    if !artifact_path.exists() {
        return Err(OrchestratorError::ToolError(format!(
            "crawl screenshot command completed but no artifact was written to '{}'",
            artifact_path.display()
        )));
    }
    validate_artifact_size_limit(
        aria_core::BrowserArtifactKind::Screenshot,
        std::fs::metadata(&artifact_path)
            .map(|meta| meta.len())
            .unwrap_or(0),
    )?;
    run_artifact_scan(
        &artifact_path,
        aria_core::BrowserArtifactKind::Screenshot,
        "image/png",
    )?;
    Ok(aria_core::BrowserArtifactRecord {
        artifact_id: format!("browser-artifact-{}", uuid::Uuid::new_v4()),
        browser_session_id: crawl_id.to_string(),
        session_id,
        agent_id: agent_id.to_string(),
        profile_id: "crawl-screenshot".into(),
        kind: aria_core::BrowserArtifactKind::Screenshot,
        mime_type: "image/png".into(),
        storage_path: artifact_path.to_string_lossy().to_string(),
        metadata: serde_json::json!({
            "url": target_url,
            "crawl_id": crawl_id,
        }),
        created_at_us: chrono::Utc::now().timestamp_micros() as u64,
    })
}

async fn capture_crawl_screenshot_artifact_async(
    sessions_dir: PathBuf,
    session_id: aria_core::Uuid,
    agent_id: String,
    crawl_id: String,
    target_url: String,
) -> Result<aria_core::BrowserArtifactRecord, OrchestratorError> {
    run_blocking_browser_task("crawl screenshot capture", move || {
        capture_crawl_screenshot_artifact(
            &sessions_dir,
            session_id,
            &agent_id,
            &crawl_id,
            &target_url,
        )
    })
    .await
}

fn enforce_watch_job_rate_limits(
    sessions_dir: &Path,
    agent_id: &str,
    target_url: &str,
) -> Result<(), OrchestratorError> {
    let policy = web_rate_policy();
    let target = reqwest::Url::parse(target_url).map_err(|e| {
        OrchestratorError::ToolError(format!("Invalid URL '{}': {}", target_url, e))
    })?;
    let domain = url_host_key(&target)?;
    let jobs = RuntimeStore::for_sessions_dir(sessions_dir)
        .list_watch_jobs()
        .map_err(OrchestratorError::ToolError)?;
    let agent_jobs = jobs.iter().filter(|job| job.agent_id == agent_id).count();
    if agent_jobs >= policy.watch_max_jobs_per_agent {
        return Err(OrchestratorError::ToolError(format!(
            "watch job limit reached for agent '{}': {} active jobs (max {})",
            agent_id, agent_jobs, policy.watch_max_jobs_per_agent
        )));
    }
    let domain_jobs = jobs
        .iter()
        .filter(|job| {
            reqwest::Url::parse(&job.target_url)
                .ok()
                .and_then(|parsed| url_host_key(&parsed).ok())
                .as_deref()
                == Some(domain.as_str())
        })
        .count();
    if domain_jobs >= policy.watch_max_jobs_per_domain {
        return Err(OrchestratorError::ToolError(format!(
            "watch job limit reached for domain '{}': {} active jobs (max {})",
            domain, domain_jobs, policy.watch_max_jobs_per_domain
        )));
    }
    Ok(())
}
