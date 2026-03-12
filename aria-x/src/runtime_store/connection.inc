impl RuntimeStore {
    const SQLITE_POOL_MAX_SIZE: usize = 8;

    pub fn configure_operator_retention(
        max_skill_signature_rows: u32,
        max_shell_exec_audit_rows: u32,
        max_scope_denial_rows: u32,
        max_request_policy_audit_rows: u32,
        max_repair_fallback_audit_rows: u32,
        max_streaming_decision_audit_rows: u32,
        max_browser_action_audit_rows: u32,
        max_browser_challenge_event_rows: u32,
    ) {
        SKILL_SIGNATURE_RETENTION_ROWS.store(max_skill_signature_rows, Ordering::Relaxed);
        SHELL_EXEC_AUDIT_RETENTION_ROWS.store(max_shell_exec_audit_rows, Ordering::Relaxed);
        SCOPE_DENIAL_RETENTION_ROWS.store(max_scope_denial_rows, Ordering::Relaxed);
        REQUEST_POLICY_AUDIT_RETENTION_ROWS
            .store(max_request_policy_audit_rows, Ordering::Relaxed);
        REPAIR_FALLBACK_AUDIT_RETENTION_ROWS
            .store(max_repair_fallback_audit_rows, Ordering::Relaxed);
        STREAMING_DECISION_AUDIT_RETENTION_ROWS
            .store(max_streaming_decision_audit_rows, Ordering::Relaxed);
        BROWSER_ACTION_AUDIT_RETENTION_ROWS
            .store(max_browser_action_audit_rows, Ordering::Relaxed);
        BROWSER_CHALLENGE_EVENT_RETENTION_ROWS
            .store(max_browser_challenge_event_rows, Ordering::Relaxed);
    }

    pub fn for_sessions_dir(sessions_dir: &Path) -> Self {
        Self {
            path: sessions_dir.join("runtime_state.sqlite"),
        }
    }

    fn connection_cache() -> &'static dashmap::DashMap<PathBuf, Arc<RuntimeStorePool>> {
        static CONNECTIONS: OnceLock<dashmap::DashMap<PathBuf, Arc<RuntimeStorePool>>> =
            OnceLock::new();
        CONNECTIONS.get_or_init(dashmap::DashMap::new)
    }

    #[cfg(test)]
    fn cached_connection_total_for_path(path: &Path) -> Option<usize> {
        Self::connection_cache()
            .get(path)
            .map(|entry| entry.value().total.load(Ordering::SeqCst))
    }

    fn schema_init_cache() -> &'static dashmap::DashSet<PathBuf> {
        static INITIALIZED: OnceLock<dashmap::DashSet<PathBuf>> = OnceLock::new();
        INITIALIZED.get_or_init(dashmap::DashSet::new)
    }

    fn open_initialized_connection(path: &Path) -> Result<Connection, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create runtime store dir failed: {}", e))?;
        }
        let conn = Connection::open(path).map_err(|e| format!("open runtime store failed: {}", e))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("enable WAL failed: {}", e))?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| format!("set synchronous mode failed: {}", e))?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| format!("enable foreign keys failed: {}", e))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| format!("set sqlite busy timeout failed: {}", e))?;
        if Self::schema_init_cache().insert(path.to_path_buf()) {
            Self {
                path: path.to_path_buf(),
            }
            .init_schema(&conn)?;
        }
        Ok(conn)
    }

    fn pool_for_path(path: &Path) -> Result<Arc<RuntimeStorePool>, String> {
        let cache = Self::connection_cache();
        if let Some(existing) = cache.get(path) {
            return Ok(existing.value().clone());
        }
        let pool = Arc::new(RuntimeStorePool {
            path: path.to_path_buf(),
            available: Mutex::new(Vec::new()),
            total: AtomicUsize::new(0),
            max_size: Self::SQLITE_POOL_MAX_SIZE,
            waiters: Condvar::new(),
        });
        let entry = cache.entry(path.to_path_buf()).or_insert_with(|| pool.clone());
        Ok(entry.value().clone())
    }

    fn connect(&self) -> Result<StoreConnection, String> {
        let pool = Self::pool_for_path(&self.path)?;
        loop {
            let pooled_conn = {
                let mut guard = pool
                    .available
                    .lock()
                    .map_err(|_| "lock runtime store pool failed".to_string())?;
                guard.pop()
            };
            if let Some(conn) = pooled_conn {
                return Ok(StoreConnection {
                    pool: pool.clone(),
                    conn: Some(conn),
                });
            }

            let current_total = pool.total.load(Ordering::Relaxed);
            if current_total < pool.max_size {
                if pool
                    .total
                    .compare_exchange(
                        current_total,
                        current_total + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    match Self::open_initialized_connection(&pool.path) {
                        Ok(conn) => {
                            return Ok(StoreConnection {
                                pool,
                                conn: Some(conn),
                            });
                        }
                        Err(err) => {
                            pool.total.fetch_sub(1, Ordering::SeqCst);
                            return Err(err);
                        }
                    }
                }
                continue;
            }

            let guard = pool
                .available
                .lock()
                .map_err(|_| "lock runtime store pool failed".to_string())?;
            let mut guard = pool
                .waiters
                .wait(guard)
                .map_err(|_| "wait for runtime store pool failed".to_string())?;
            if let Some(conn) = guard.pop() {
                drop(guard);
                return Ok(StoreConnection {
                    pool: pool.clone(),
                    conn: Some(conn),
                });
            }
        }
    }
}
