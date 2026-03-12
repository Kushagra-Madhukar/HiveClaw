use super::*;

impl RuntimeStore {
    #[allow(dead_code)]
    pub fn upsert_browser_profile(
        &self,
        profile: &BrowserProfile,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(profile)
            .map_err(|e| format!("serialize browser profile failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_profiles (profile_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(profile_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![profile.profile_id, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("upsert browser profile failed: {}", e))?;
        Ok(())
    }

    pub fn list_browser_profiles(&self) -> Result<Vec<BrowserProfile>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT payload_json FROM browser_profiles ORDER BY updated_at_us DESC")
            .map_err(|e| format!("prepare browser profile query failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query browser profiles failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read browser profile row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse browser profile failed: {}", e))?,
            );
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn upsert_browser_profile_binding(
        &self,
        binding: &BrowserProfileBindingRecord,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(binding)
            .map_err(|e| format!("serialize browser profile binding failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_profile_bindings
             (binding_id, session_id, agent_id, profile_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(binding_id) DO UPDATE SET
               session_id=excluded.session_id,
               agent_id=excluded.agent_id,
               profile_id=excluded.profile_id,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                binding.binding_id,
                binding.session_id.to_vec(),
                binding.agent_id,
                binding.profile_id,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert browser profile binding failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_profile_bindings(
        &self,
        session_id: Option<aria_core::Uuid>,
        agent_id: Option<&str>,
    ) -> Result<Vec<BrowserProfileBindingRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, agent_id) {
            (Some(session_id), Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_profile_bindings
                         WHERE session_id=?1 AND agent_id=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser profile binding query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser profile bindings failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser profile binding row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser profile binding failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_profile_bindings
                         WHERE session_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser profile binding query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser profile bindings failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser profile binding row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser profile binding failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_profile_bindings
                         WHERE agent_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser profile binding query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser profile bindings failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser profile binding row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser profile binding failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_profile_bindings ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser profile binding query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser profile bindings failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser profile binding row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser profile binding failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn upsert_browser_session(
        &self,
        browser_session: &BrowserSessionRecord,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(browser_session)
            .map_err(|e| format!("serialize browser session failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_sessions
             (browser_session_id, session_id, agent_id, profile_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(browser_session_id) DO UPDATE SET
               session_id=excluded.session_id,
               agent_id=excluded.agent_id,
               profile_id=excluded.profile_id,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                browser_session.browser_session_id,
                browser_session.session_id.to_vec(),
                browser_session.agent_id,
                browser_session.profile_id,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert browser session failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_sessions(
        &self,
        session_id: Option<aria_core::Uuid>,
        agent_id: Option<&str>,
    ) -> Result<Vec<BrowserSessionRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, agent_id) {
            (Some(session_id), Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_sessions
                         WHERE session_id=?1 AND agent_id=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser sessions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_sessions
                         WHERE session_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser sessions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_sessions
                         WHERE agent_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser sessions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_sessions ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser sessions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn upsert_browser_session_state(
        &self,
        state: &BrowserSessionStateRecord,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(state)
            .map_err(|e| format!("serialize browser session state failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_session_states
             (state_id, browser_session_id, session_id, agent_id, profile_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(state_id) DO UPDATE SET
               browser_session_id=excluded.browser_session_id,
               session_id=excluded.session_id,
               agent_id=excluded.agent_id,
               profile_id=excluded.profile_id,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                state.state_id,
                state.browser_session_id,
                state.session_id.to_vec(),
                state.agent_id,
                state.profile_id,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert browser session state failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_session_states(
        &self,
        session_id: Option<aria_core::Uuid>,
        browser_session_id: Option<&str>,
    ) -> Result<Vec<BrowserSessionStateRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, browser_session_id) {
            (Some(session_id), Some(browser_session_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_session_states
                         WHERE session_id=?1 AND browser_session_id=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), browser_session_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser session states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session state failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_session_states
                         WHERE session_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser session states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session state failed: {}", e))?,
                    );
                }
            }
            (None, Some(browser_session_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_session_states
                         WHERE browser_session_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![browser_session_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser session states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session state failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_session_states ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser session state query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser session states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser session state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser session state failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    pub fn delete_browser_session_state(&self, state_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM browser_session_states WHERE state_id=?1",
            params![state_id],
        )
        .map_err(|e| format!("delete browser session state failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn append_browser_artifact(&self, artifact: &BrowserArtifactRecord) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(artifact)
            .map_err(|e| format!("serialize browser artifact failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_artifacts
             (artifact_id, browser_session_id, session_id, agent_id, profile_id, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                artifact.artifact_id,
                artifact.browser_session_id,
                artifact.session_id.to_vec(),
                artifact.agent_id,
                artifact.profile_id,
                payload,
                artifact.created_at_us as i64
            ],
        )
        .map_err(|e| format!("append browser artifact failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_artifacts(
        &self,
        session_id: Option<aria_core::Uuid>,
        browser_session_id: Option<&str>,
    ) -> Result<Vec<BrowserArtifactRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, browser_session_id) {
            (Some(session_id), Some(browser_session_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_artifacts
                         WHERE session_id=?1 AND browser_session_id=?2 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser artifact query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), browser_session_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser artifacts failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser artifact row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser artifact failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_artifacts
                         WHERE session_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser artifact query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser artifacts failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser artifact row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser artifact failed: {}", e))?,
                    );
                }
            }
            (None, Some(browser_session_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_artifacts
                         WHERE browser_session_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser artifact query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![browser_session_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser artifacts failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser artifact row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser artifact failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_artifacts ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser artifact query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser artifacts failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser artifact row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser artifact failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    pub fn delete_browser_artifact(&self, artifact_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM browser_artifacts WHERE artifact_id=?1",
            params![artifact_id],
        )
        .map_err(|e| format!("delete browser artifact failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn append_browser_action_audit(
        &self,
        audit: &BrowserActionAuditRecord,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(audit)
            .map_err(|e| format!("serialize browser action audit failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_action_audits
             (audit_id, browser_session_id, session_id, agent_id, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                audit.audit_id,
                audit.browser_session_id,
                audit.session_id.to_vec(),
                audit.agent_id,
                payload,
                audit.created_at_us as i64
            ],
        )
        .map_err(|e| format!("append browser action audit failed: {}", e))?;
        self.prune_operator_records(
            SKILL_SIGNATURE_RETENTION_ROWS.load(Ordering::Relaxed),
            SHELL_EXEC_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            SCOPE_DENIAL_RETENTION_ROWS.load(Ordering::Relaxed),
            REQUEST_POLICY_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            REPAIR_FALLBACK_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            STREAMING_DECISION_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            BROWSER_ACTION_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            BROWSER_CHALLENGE_EVENT_RETENTION_ROWS.load(Ordering::Relaxed),
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_action_audits(
        &self,
        session_id: Option<aria_core::Uuid>,
        agent_id: Option<&str>,
    ) -> Result<Vec<BrowserActionAuditRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, agent_id) {
            (Some(session_id), Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_action_audits
                         WHERE session_id=?1 AND agent_id=?2 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser action audit query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser action audits failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser action audit row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser action audit failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_action_audits
                         WHERE session_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser action audit query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser action audits failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser action audit row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser action audit failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_action_audits
                         WHERE agent_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser action audit query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser action audits failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser action audit row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser action audit failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_action_audits ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser action audit query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser action audits failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser action audit row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser action audit failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn append_browser_challenge_event(
        &self,
        event: &BrowserChallengeEvent,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(event)
            .map_err(|e| format!("serialize browser challenge event failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_challenge_events
             (event_id, browser_session_id, session_id, agent_id, profile_id, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                event.event_id,
                event.browser_session_id,
                event.session_id.to_vec(),
                event.agent_id,
                event.profile_id,
                payload,
                event.created_at_us as i64
            ],
        )
        .map_err(|e| format!("append browser challenge event failed: {}", e))?;
        self.prune_operator_records(
            SKILL_SIGNATURE_RETENTION_ROWS.load(Ordering::Relaxed),
            SHELL_EXEC_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            SCOPE_DENIAL_RETENTION_ROWS.load(Ordering::Relaxed),
            REQUEST_POLICY_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            REPAIR_FALLBACK_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            STREAMING_DECISION_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            BROWSER_ACTION_AUDIT_RETENTION_ROWS.load(Ordering::Relaxed),
            BROWSER_CHALLENGE_EVENT_RETENTION_ROWS.load(Ordering::Relaxed),
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_challenge_events(
        &self,
        session_id: Option<aria_core::Uuid>,
        agent_id: Option<&str>,
    ) -> Result<Vec<BrowserChallengeEvent>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, agent_id) {
            (Some(session_id), Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_challenge_events
                         WHERE session_id=?1 AND agent_id=?2 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser challenge event query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser challenge events failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser challenge event row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser challenge event failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_challenge_events
                         WHERE session_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser challenge event query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser challenge events failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser challenge event row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser challenge event failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_challenge_events
                         WHERE agent_id=?1 ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser challenge event query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser challenge events failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser challenge event row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser challenge event failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_challenge_events ORDER BY created_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser challenge event query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser challenge events failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser challenge event row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser challenge event failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn upsert_browser_login_state(
        &self,
        login_state: &BrowserLoginStateRecord,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(login_state)
            .map_err(|e| format!("serialize browser login state failed: {}", e))?;
        conn.execute(
            "INSERT INTO browser_login_states
             (login_state_id, browser_session_id, session_id, agent_id, profile_id, domain, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(login_state_id) DO UPDATE SET
               browser_session_id=excluded.browser_session_id,
               session_id=excluded.session_id,
               agent_id=excluded.agent_id,
               profile_id=excluded.profile_id,
               domain=excluded.domain,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                login_state.login_state_id,
                login_state.browser_session_id,
                login_state.session_id.to_vec(),
                login_state.agent_id,
                login_state.profile_id,
                login_state.domain,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert browser login state failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_browser_login_states(
        &self,
        session_id: Option<aria_core::Uuid>,
        agent_id: Option<&str>,
        domain: Option<&str>,
    ) -> Result<Vec<BrowserLoginStateRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (session_id, agent_id, domain) {
            (Some(session_id), Some(agent_id), Some(domain)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE session_id=?1 AND agent_id=?2 AND domain=?3 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id, domain], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), Some(agent_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE session_id=?1 AND agent_id=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), agent_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None, Some(domain)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE session_id=?1 AND domain=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec(), domain], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id), Some(domain)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE agent_id=?1 AND domain=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id, domain], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (Some(session_id), None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE session_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![session_id.to_vec()], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE agent_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (None, None, Some(domain)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states
                         WHERE domain=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![domain], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
            (None, None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM browser_login_states ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare browser login state query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query browser login states failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read browser login state row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse browser login state failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }
}
