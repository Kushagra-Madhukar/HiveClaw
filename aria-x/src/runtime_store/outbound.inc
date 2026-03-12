impl RuntimeStore {
    pub fn record_dedupe_key_if_new(&self, key: &str, first_seen_us: u64) -> Result<bool, String> {
        let conn = self.connect()?;
        let changed = conn
            .execute(
                "INSERT OR IGNORE INTO dedupe_keys (dedupe_key, first_seen_us) VALUES (?1, ?2)",
                params![key, first_seen_us as i64],
            )
            .map_err(|e| format!("write dedupe key failed: {}", e))?;
        Ok(changed > 0)
    }

    pub fn record_outbound_delivery(
        &self,
        envelope: &OutboundEnvelope,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(envelope)
            .map_err(|e| format!("serialize outbound envelope failed: {}", e))?;
        conn.execute(
            "INSERT INTO outbound_deliveries
             (envelope_id, session_id, channel, recipient_id, status, error, payload_json, recorded_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(envelope_id) DO UPDATE SET
               status=excluded.status,
               error=excluded.error,
               payload_json=excluded.payload_json,
               recorded_at_us=excluded.recorded_at_us",
            params![
                uuid::Uuid::from_bytes(envelope.envelope_id).to_string(),
                uuid::Uuid::from_bytes(envelope.session_id).to_string(),
                format!("{:?}", envelope.channel),
                envelope.recipient_id,
                status,
                error,
                payload,
                envelope.timestamp_us as i64,
            ],
        )
        .map_err(|e| format!("write outbound delivery failed: {}", e))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn read_outbound_delivery(
        &self,
        envelope_id: [u8; 16],
    ) -> Result<OutboundDeliveryRecord, String> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT envelope_id, channel, recipient_id, status, error FROM outbound_deliveries WHERE envelope_id=?1",
            params![uuid::Uuid::from_bytes(envelope_id).to_string()],
            |row| {
                Ok(OutboundDeliveryRecord {
                    envelope_id: row.get(0)?,
                    channel: row.get(1)?,
                    recipient_id: row.get(2)?,
                    status: row.get(3)?,
                    error: row.get(4)?,
                })
            },
        )
        .map_err(|e| format!("read outbound delivery failed: {}", e))
    }

    pub fn upsert_cache_snapshot(
        &self,
        session_id: uuid::Uuid,
        agent_id: &str,
        tools: &[CachedTool],
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(tools)
            .map_err(|e| format!("serialize cache snapshot failed: {}", e))?;
        conn.execute(
            "INSERT INTO cache_snapshots (session_id, agent_id, tools_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(session_id, agent_id) DO UPDATE SET
               tools_json=excluded.tools_json,
               updated_at_us=excluded.updated_at_us",
            params![
                session_id.to_string(),
                agent_id,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("write cache snapshot failed: {}", e))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn read_cache_snapshot(
        &self,
        session_id: uuid::Uuid,
        agent_id: &str,
    ) -> Result<Vec<CachedTool>, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT tools_json FROM cache_snapshots WHERE session_id=?1 AND agent_id=?2",
                params![session_id.to_string(), agent_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read cache snapshot failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse cache snapshot failed: {}", e))
    }

    pub fn upsert_job_snapshot<T: serde::Serialize>(
        &self,
        job_id: &str,
        job: &T,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(job)
            .map_err(|e| format!("serialize job snapshot failed: {}", e))?;
        conn.execute(
            "INSERT INTO job_snapshots (job_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(job_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![job_id, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("write job snapshot failed: {}", e))?;
        Ok(())
    }

    pub fn delete_job_snapshot(&self, job_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute("DELETE FROM job_snapshots WHERE job_id=?1", params![job_id])
            .map_err(|e| format!("delete job snapshot failed: {}", e))?;
        Ok(())
    }

    pub fn list_job_snapshots<T: serde::de::DeserializeOwned>(&self) -> Result<Vec<T>, String> {
        Ok(self
            .list_job_snapshot_records::<T>()?
            .into_iter()
            .map(|record| record.job)
            .collect())
    }

    pub fn list_job_snapshot_records<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Vec<JobSnapshotRecord<T>>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json, updated_at_us FROM job_snapshots ORDER BY updated_at_us ASC",
            )
            .map_err(|e| format!("prepare list job snapshots failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| format!("query job snapshots failed: {}", e))?;

        let mut jobs = Vec::new();
        for row in rows {
            let (payload, updated_at_us) =
                row.map_err(|e| format!("read job snapshot row failed: {}", e))?;
            let job = serde_json::from_str(&payload)
                .map_err(|e| format!("parse job snapshot failed: {}", e))?;
            jobs.push(JobSnapshotRecord {
                job,
                updated_at_us: updated_at_us as u64,
            });
        }
        Ok(jobs)
    }

    pub fn try_acquire_job_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        now_us: u64,
        lease_until_us: u64,
    ) -> Result<bool, String> {
        let mut conn = self.connect()?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("begin lease transaction failed: {}", e))?;
        let existing: Option<(String, i64)> = tx
            .query_row(
                "SELECT worker_id, lease_until_us FROM job_leases WHERE job_id=?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| format!("read job lease failed: {}", e))?;
        let can_acquire = match existing {
            None => true,
            Some((existing_worker_id, existing_lease_until)) => {
                existing_worker_id == worker_id || existing_lease_until <= now_us as i64
            }
        };
        if !can_acquire {
            tx.commit()
                .map_err(|e| format!("commit rejected lease transaction failed: {}", e))?;
            return Ok(false);
        }

        tx.execute(
            "INSERT INTO job_leases (job_id, worker_id, lease_until_us, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(job_id) DO UPDATE SET
               worker_id=excluded.worker_id,
               lease_until_us=excluded.lease_until_us,
               updated_at_us=excluded.updated_at_us",
            params![job_id, worker_id, lease_until_us as i64, now_us as i64],
        )
        .map_err(|e| format!("write job lease failed: {}", e))?;
        tx.commit()
            .map_err(|e| format!("commit lease transaction failed: {}", e))?;
        Ok(true)
    }

    pub fn release_job_lease(&self, job_id: &str, worker_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM job_leases WHERE job_id=?1 AND worker_id=?2",
            params![job_id, worker_id],
        )
        .map_err(|e| format!("delete job lease failed: {}", e))?;
        Ok(())
    }

    pub fn clear_job_lease(&self, job_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute("DELETE FROM job_leases WHERE job_id=?1", params![job_id])
            .map_err(|e| format!("clear job lease failed: {}", e))?;
        Ok(())
    }

}
