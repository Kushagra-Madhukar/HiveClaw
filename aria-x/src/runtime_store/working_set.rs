use super::*;

impl RuntimeStore {
    pub fn append_working_set_entry(
        &self,
        entry: &aria_core::WorkingSetEntry,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(entry)
            .map_err(|e| format!("serialize working set entry failed: {}", e))?;
        let session_id = entry
            .session_id
            .map(uuid::Uuid::from_bytes)
            .map(|id| id.to_string())
            .unwrap_or_default();
        conn.execute(
            "INSERT INTO working_set_entries (entry_id, session_id, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(entry_id) DO UPDATE SET
               session_id=excluded.session_id,
               payload_json=excluded.payload_json,
               created_at_us=excluded.created_at_us",
            params![entry.entry_id, session_id, payload, entry.created_at_us as i64],
        )
        .map_err(|e| format!("append working set entry failed: {}", e))?;
        Ok(())
    }

    pub fn list_working_set_entries(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<aria_core::WorkingSetEntry>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM working_set_entries
                 WHERE session_id=?1
                 ORDER BY created_at_us DESC
                 LIMIT ?2",
            )
            .map_err(|e| format!("prepare working set query failed: {}", e))?;
        let rows = stmt
            .query_map(params![session_id, limit as i64], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query working set entries failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read working set row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse working set entry failed: {}", e))?,
            );
        }
        Ok(out)
    }
}
