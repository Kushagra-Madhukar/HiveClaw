use super::*;
use sha2::{Digest, Sha256};

impl RuntimeStore {
    pub fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(record)
            .map_err(|e| format!("serialize approval failed: {}", e))?;
        conn.execute(
            "INSERT INTO approvals (approval_id, payload_json) VALUES (?1, ?2)
             ON CONFLICT(approval_id) DO UPDATE SET payload_json=excluded.payload_json",
            params![record.approval_id, payload],
        )
        .map_err(|e| format!("write approval failed: {}", e))?;
        Ok(())
    }

    pub fn read_approval(&self, approval_id: &str) -> Result<ApprovalRecord, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM approvals WHERE approval_id=?1",
                params![approval_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read approval failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse approval failed: {}", e))
    }

    pub fn list_approvals(
        &self,
        session_id: Option<aria_core::Uuid>,
        user_id: Option<&str>,
        status: Option<aria_core::ApprovalStatus>,
    ) -> Result<Vec<ApprovalRecord>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT payload_json FROM approvals")
            .map_err(|e| format!("prepare approval query failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query approvals failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read approval row failed: {}", e))?;
            let record: ApprovalRecord = serde_json::from_str(&payload)
                .map_err(|e| format!("parse approval failed: {}", e))?;
            if session_id.is_some() && Some(record.session_id) != session_id {
                continue;
            }
            if user_id.is_some() && Some(record.user_id.as_str()) != user_id {
                continue;
            }
            if status.is_some() && Some(record.status) != status {
                continue;
            }
            out.push(record);
        }
        out.sort_by_key(|record| std::cmp::Reverse(record.created_at_us));
        Ok(out)
    }

    #[cfg(test)]
    pub fn delete_approval(&self, approval_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM approvals WHERE approval_id=?1",
            params![approval_id],
        )
        .map_err(|e| format!("delete approval failed: {}", e))?;
        Ok(())
    }

    pub fn resolve_approval_handle(
        &self,
        handle_id: &str,
        session_id: aria_core::Uuid,
        user_id: &str,
        now_us: u64,
    ) -> Result<Option<String>, String> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT approval_id FROM approval_handles
             WHERE handle_id=?1 AND session_id=?2 AND user_id=?3 AND expires_at_us >= ?4",
            params![
                handle_id,
                uuid::Uuid::from_bytes(session_id).to_string(),
                user_id,
                now_us as i64
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| format!("resolve approval handle failed: {}", e))
    }

    pub fn resolve_or_create_approval_handle(
        &self,
        approval_id: &str,
        session_id: aria_core::Uuid,
        user_id: &str,
        created_at_us: u64,
        expires_at_us: u64,
    ) -> Result<String, String> {
        let conn = self.connect()?;
        let existing: Option<String> = conn
            .query_row(
                "SELECT handle_id FROM approval_handles WHERE approval_id=?1",
                params![approval_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| format!("read approval handle failed: {}", e))?;
        if let Some(handle_id) = existing {
            return Ok(handle_id);
        }

        let mut hasher = Sha256::new();
        hasher.update(b"aria-approval-handle");
        hasher.update(approval_id.as_bytes());
        let digest = hasher.finalize();
        let seed = hex::encode(&digest[..5]).to_uppercase();
        let session_id = uuid::Uuid::from_bytes(session_id).to_string();
        for attempt in 0..32_u8 {
            let handle_id = if attempt == 0 {
                format!("apv-{}", seed)
            } else {
                format!("apv-{}-{}", seed, attempt)
            };
            let inserted = conn
                .execute(
                    "INSERT OR IGNORE INTO approval_handles
                     (handle_id, approval_id, session_id, user_id, created_at_us, expires_at_us)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        handle_id,
                        approval_id,
                        session_id,
                        user_id,
                        created_at_us as i64,
                        expires_at_us as i64
                    ],
                )
                .map_err(|e| format!("write approval handle failed: {}", e))?;
            if inserted == 1 {
                return Ok(handle_id);
            }
            let bound_approval_id: Option<String> = conn
                .query_row(
                    "SELECT approval_id FROM approval_handles WHERE handle_id=?1",
                    params![handle_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| format!("read approval handle binding failed: {}", e))?;
            if bound_approval_id.as_deref() == Some(approval_id) {
                return Ok(handle_id);
            }
        }
        Err(format!(
            "failed to allocate approval handle for '{}' after 32 attempts",
            approval_id
        ))
    }

    pub fn prune_expired_approval_handles(&self, now_us: u64) -> Result<usize, String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM approval_handles WHERE expires_at_us < ?1",
            params![now_us as i64],
        )
        .map_err(|e| format!("prune approval handles failed: {}", e))
    }

    pub fn upsert_elevation(&self, grant: &ElevationGrant) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(grant)
            .map_err(|e| format!("serialize elevation failed: {}", e))?;
        conn.execute(
            "INSERT INTO elevations (session_id, agent_id, user_id, granted_at_us, expires_at_us, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id, agent_id) DO UPDATE SET
               user_id=excluded.user_id,
               granted_at_us=excluded.granted_at_us,
               expires_at_us=excluded.expires_at_us,
               payload_json=excluded.payload_json",
            params![
                uuid::Uuid::from_bytes(grant.session_id).to_string(),
                grant.agent_id,
                grant.user_id,
                grant.granted_at_us as i64,
                grant.expires_at_us.map(|v| v as i64),
                payload,
            ],
        )
        .map_err(|e| format!("write elevation failed: {}", e))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn read_elevation(
        &self,
        session_id: uuid::Uuid,
        agent_id: &str,
    ) -> Result<ElevationGrant, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM elevations WHERE session_id=?1 AND agent_id=?2",
                params![session_id.to_string(), agent_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read elevation failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse elevation failed: {}", e))
    }

    pub fn has_active_elevation(
        &self,
        session_id: uuid::Uuid,
        user_id: &str,
        agent_id: &str,
        now_us: u64,
    ) -> Result<bool, String> {
        let conn = self.connect()?;
        let row: Option<(String, Option<i64>)> = conn
            .query_row(
                "SELECT user_id, expires_at_us FROM elevations WHERE session_id=?1 AND agent_id=?2",
                params![session_id.to_string(), agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| format!("query active elevation failed: {}", e))?;
        Ok(row
            .filter(|(stored_user_id, _)| stored_user_id == user_id)
            .filter(|(_, expires_at_us)| expires_at_us.map(|ts| ts > now_us as i64).unwrap_or(true))
            .is_some())
    }
}
