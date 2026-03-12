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
            let record: ApprovalRecord =
                serde_json::from_str(&payload).map_err(|e| format!("parse approval failed: {}", e))?;
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

    pub fn delete_approval(&self, approval_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM approvals WHERE approval_id=?1",
            params![approval_id],
        )
        .map_err(|e| format!("delete approval failed: {}", e))?;
        Ok(())
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
