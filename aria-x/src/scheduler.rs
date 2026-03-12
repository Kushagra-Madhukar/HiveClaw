fn session_runtime_db_path(sessions_dir: &Path) -> PathBuf {
    sessions_dir.join("runtime_state.sqlite")
}

fn scheduler_boot_jobs(
    sessions_dir: &Path,
    config_jobs: &[ScheduledPromptConfig],
) -> Vec<ScheduledPromptJob> {
    let persisted_jobs = RuntimeStore::for_sessions_dir(sessions_dir)
        .list_job_snapshots::<ScheduledPromptJob>()
        .unwrap_or_default();
    if !persisted_jobs.is_empty() {
        return persisted_jobs;
    }

    config_jobs
        .iter()
        .filter_map(|job| {
            ScheduleSpec::parse(&job.schedule).map(|spec| ScheduledPromptJob {
                id: job.id.clone(),
                agent_id: job.agent_id.clone(),
                creator_agent: None,
                executor_agent: Some(job.agent_id.clone()),
                notifier_agent: None,
                prompt: job.prompt.clone(),
                schedule_str: job.schedule.clone(),
                kind: ScheduledJobKind::Orchestrate,
                schedule: spec,
                session_id: None,
                user_id: None,
                channel: None,
                status: aria_intelligence::ScheduledJobStatus::Scheduled,
                last_run_at_us: None,
                last_error: None,
                audit_log: Vec::new(),
            })
        })
        .collect()
}

async fn persist_scheduler_job_snapshot(
    tx_cron: &tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
    sessions_dir: &Path,
    job_id: &str,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx_cron
        .send(aria_intelligence::CronCommand::List(reply_tx))
        .await
        .map_err(|e| format!("list scheduler jobs failed: {}", e))?;
    let jobs = reply_rx
        .await
        .map_err(|e| format!("receive scheduler jobs failed: {}", e))?;
    if let Some(job) = jobs.into_iter().find(|job| job.id == job_id) {
        RuntimeStore::for_sessions_dir(sessions_dir).upsert_job_snapshot(
            &job.id,
            &job,
            chrono::Utc::now().timestamp_micros() as u64,
        )?;
    }
    Ok(())
}

async fn load_authoritative_scheduler_jobs(
    tx_cron: &tokio::sync::mpsc::Sender<aria_intelligence::CronCommand>,
    sessions_dir: Option<&Path>,
) -> Result<Vec<ScheduledPromptJob>, String> {
    if let Some(dir) = sessions_dir {
        return RuntimeStore::for_sessions_dir(dir).list_job_snapshots::<ScheduledPromptJob>();
    }

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx_cron
        .send(aria_intelligence::CronCommand::List(reply_tx))
        .await
        .map_err(|e| format!("list scheduler jobs failed: {}", e))?;
    reply_rx
        .await
        .map_err(|e| format!("receive scheduler jobs failed: {}", e))
}

fn normalize_job_for_store(mut job: ScheduledPromptJob) -> ScheduledPromptJob {
    if job.audit_log.is_empty() {
        job.append_audit(
            "scheduled",
            Some(format!(
                "kind={:?} agent={}",
                job.kind,
                job.effective_agent_id()
            )),
            chrono::Utc::now().timestamp_micros() as u64,
        );
    }
    job.status = aria_intelligence::ScheduledJobStatus::Scheduled;
    job
}

fn update_job_snapshot_status(
    mut job: ScheduledPromptJob,
    status: aria_intelligence::ScheduledJobStatus,
    detail: Option<String>,
    timestamp_us: u64,
) -> ScheduledPromptJob {
    job.status = status.clone();
    if matches!(
        status,
        aria_intelligence::ScheduledJobStatus::Failed
            | aria_intelligence::ScheduledJobStatus::ApprovalRequired
    ) {
        job.last_error = detail.clone();
    } else {
        job.last_error = None;
    }
    job.append_audit(
        match status {
            aria_intelligence::ScheduledJobStatus::Scheduled => "scheduled",
            aria_intelligence::ScheduledJobStatus::Dispatched => "dispatched",
            aria_intelligence::ScheduledJobStatus::Completed => "completed",
            aria_intelligence::ScheduledJobStatus::Failed => "failed",
            aria_intelligence::ScheduledJobStatus::ApprovalRequired => "approval_required",
        },
        detail,
        timestamp_us,
    );
    job
}

fn seed_scheduler_runtime_store(
    sessions_dir: &Path,
    config_jobs: &[ScheduledPromptConfig],
) -> Result<usize, String> {
    let boot_jobs = scheduler_boot_jobs(sessions_dir, config_jobs);
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    for job in &boot_jobs {
        store.upsert_job_snapshot(&job.id, job, now_us)?;
    }
    Ok(boot_jobs.len())
}

fn spawn_scheduler_command_processor(
    sessions_dir: PathBuf,
    mut command_rx: tokio::sync::mpsc::Receiver<aria_intelligence::CronCommand>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let store = RuntimeStore::for_sessions_dir(&sessions_dir);
        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                aria_intelligence::CronCommand::Add(job) => {
                    let job = normalize_job_for_store(job);
                    let _ = store.upsert_job_snapshot(
                        &job.id,
                        &job,
                        chrono::Utc::now().timestamp_micros() as u64,
                    );
                }
                aria_intelligence::CronCommand::Remove(id) => {
                    let _ = store.delete_job_snapshot(&id);
                    let _ = store.clear_job_lease(&id);
                }
                aria_intelligence::CronCommand::UpdateStatus {
                    id,
                    status,
                    detail,
                    timestamp_us,
                } => {
                    if let Ok(mut jobs) = store.list_job_snapshots::<ScheduledPromptJob>() {
                        if let Some(job) = jobs.drain(..).find(|job| job.id == id) {
                            let updated =
                                update_job_snapshot_status(job, status, detail, timestamp_us);
                            let _ = store.upsert_job_snapshot(&id, &updated, timestamp_us);
                        }
                    }
                }
                aria_intelligence::CronCommand::List(reply) => {
                    let jobs = store
                        .list_job_snapshots::<ScheduledPromptJob>()
                        .unwrap_or_default();
                    let _ = reply.send(jobs);
                }
            }
        }
    })
}

fn scheduler_worker_id(config: &Config) -> String {
    let node_id = if !config.node.id.is_empty() {
        config.node.id.as_str()
    } else {
        "default"
    };
    format!("scheduler:{}:{}", node_id, runtime_instance_id())
}

fn scheduler_shard_for_node(node_id: &str, total_shards: u16) -> u16 {
    use std::hash::{Hash, Hasher};
    let total = total_shards.max(1);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node_id.hash(&mut hasher);
    (hasher.finish() % total as u64) as u16
}

fn scheduler_job_matches_shard(job_id: &str, shard_id: u16, total_shards: u16) -> bool {
    use std::hash::{Hash, Hasher};
    let total = total_shards.max(1);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    job_id.hash(&mut hasher);
    (hasher.finish() % total as u64) as u16 == shard_id
}

async fn try_acquire_scheduler_leadership(
    sessions_dir: &Path,
    worker_id: &str,
    shard_id: u16,
    lease_seconds: u64,
) -> Result<bool, String> {
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    let lease_until_us = now_us + lease_seconds.max(1) * 1_000_000;
    Ok(RuntimeStore::for_sessions_dir(sessions_dir)
        .try_acquire_resource_lease(
            &format!("scheduler:leader:{}", shard_id),
            "exclusive",
            worker_id,
            now_us,
            lease_until_us,
        )?
        .is_some())
}

async fn try_claim_scheduler_job_execution(
    sessions_dir: &Path,
    worker_id: &str,
    job_id: &str,
    lease_seconds: u64,
) -> Result<bool, String> {
    let now_us = chrono::Utc::now().timestamp_micros() as u64;
    let lease_until_us = now_us + lease_seconds.max(1) * 1_000_000;
    RuntimeStore::for_sessions_dir(sessions_dir).try_acquire_job_lease(
        job_id,
        worker_id,
        now_us,
        lease_until_us,
    )
}

fn job_reference_time(
    job: &ScheduledPromptJob,
    snapshot_updated_at_us: u64,
) -> chrono::DateTime<chrono::Utc> {
    let micros = job.last_run_at_us.unwrap_or(snapshot_updated_at_us) as i64;
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros).unwrap_or_else(chrono::Utc::now)
}

fn is_job_due(
    job: &ScheduledPromptJob,
    snapshot_updated_at_us: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    if job.schedule.is_once()
        && matches!(
            job.status,
            aria_intelligence::ScheduledJobStatus::Dispatched
                | aria_intelligence::ScheduledJobStatus::Completed
                | aria_intelligence::ScheduledJobStatus::Failed
                | aria_intelligence::ScheduledJobStatus::ApprovalRequired
        )
    {
        return false;
    }
    let next_fire = job
        .schedule
        .next_fire(job_reference_time(job, snapshot_updated_at_us));
    next_fire <= now
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;

    fn test_config(node_id: &str) -> Config {
        let mut cfg: Config = toml::from_str(
            r#"
            [llm]
            backend = "mock"
            model = "test"
            max_tool_rounds = 5

            [policy]
            policy_path = "./policy.cedar"

            [gateway]
            adapter = "cli"

            [mesh]
            mode = "peer"
            endpoints = []
            "#,
        )
        .expect("parse config");
        cfg.node.id = node_id.to_string();
        cfg
    }

    #[test]
    fn scheduler_worker_id_includes_runtime_instance_identity() {
        let cfg = test_config("node-a");
        let worker_id = scheduler_worker_id(&cfg);
        assert!(worker_id.starts_with("scheduler:node-a:"));
        assert!(worker_id.split(':').count() >= 3);
    }

    #[test]
    fn scheduler_shard_functions_are_deterministic() {
        let shard = scheduler_shard_for_node("node-a", 4);
        assert_eq!(shard, scheduler_shard_for_node("node-a", 4));
        assert_eq!(
            scheduler_job_matches_shard("job-1", shard, 4),
            scheduler_job_matches_shard("job-1", shard, 4)
        );
    }
}

async fn poll_due_job_events_from_store(
    sessions_dir: &Path,
    worker_id: &str,
    lease_seconds: u64,
    scheduler_shard: Option<(u16, u16)>,
) -> Result<Vec<aria_intelligence::ScheduledPromptEvent>, String> {
    let store = RuntimeStore::for_sessions_dir(sessions_dir);
    let snapshot_records = store.list_job_snapshot_records::<ScheduledPromptJob>()?;
    let now = chrono::Utc::now();
    let now_us = now.timestamp_micros() as u64;
    let mut events = Vec::new();

    for record in snapshot_records {
        let mut job = record.job;
        let Some(spec) = ScheduleSpec::parse(&job.schedule_str) else {
            continue;
        };
        job.schedule = spec;
        if let Some((shard_id, total_shards)) = scheduler_shard {
            if !scheduler_job_matches_shard(&job.id, shard_id, total_shards) {
                continue;
            }
        }
        if !is_job_due(&job, record.updated_at_us, now) {
            continue;
        }
        if !try_claim_scheduler_job_execution(sessions_dir, worker_id, &job.id, lease_seconds)
            .await?
        {
            continue;
        }

        job.status = aria_intelligence::ScheduledJobStatus::Dispatched;
        job.last_run_at_us = Some(now_us);
        job.append_audit("dispatched", Some(format!("worker={}", worker_id)), now_us);
        store.upsert_job_snapshot(&job.id, &job, now_us)?;

        events.push(aria_intelligence::ScheduledPromptEvent {
            job_id: job.id.clone(),
            agent_id: job.effective_agent_id().to_string(),
            creator_agent: job.creator_agent.clone(),
            executor_agent: job.executor_agent.clone(),
            notifier_agent: job.notifier_agent.clone(),
            prompt: job.prompt.clone(),
            kind: job.kind.clone(),
            session_id: job.session_id,
            user_id: job.user_id.clone(),
            channel: job.channel,
        });
    }

    Ok(events)
}

fn execution_session_id_for_scheduled_event(
    ev: &aria_intelligence::ScheduledPromptEvent,
) -> [u8; 16] {
    if matches!(ev.kind, ScheduledJobKind::Orchestrate) {
        // Orchestrated background runs are isolated from live chat context by job id.
        scheduled_session_id(&ev.job_id)
    } else {
        ev.session_id.unwrap_or_else(|| scheduled_session_id(&ev.job_id))
    }
}
