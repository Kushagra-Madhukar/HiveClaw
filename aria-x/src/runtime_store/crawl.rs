use super::*;

impl RuntimeStore {
    pub fn upsert_domain_access_decision(
        &self,
        decision: &DomainAccessDecision,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(decision)
            .map_err(|e| format!("serialize domain access decision failed: {}", e))?;
        conn.execute(
            "INSERT INTO domain_access_decisions
             (decision_id, domain, agent_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(decision_id) DO UPDATE SET
               domain=excluded.domain,
               agent_id=excluded.agent_id,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                decision.decision_id,
                decision.domain,
                decision.agent_id,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert domain access decision failed: {}", e))?;
        Ok(())
    }

    pub fn list_domain_access_decisions(
        &self,
        domain: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<Vec<DomainAccessDecision>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        match (domain, agent_id) {
            (Some(domain), Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM domain_access_decisions
                         WHERE domain=?1 AND agent_id=?2 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare domain decision query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![domain, agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query domain decisions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read domain decision row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse domain decision failed: {}", e))?,
                    );
                }
            }
            (Some(domain), None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM domain_access_decisions
                         WHERE domain=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare domain decision query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![domain], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query domain decisions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read domain decision row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse domain decision failed: {}", e))?,
                    );
                }
            }
            (None, Some(agent_id)) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM domain_access_decisions
                         WHERE agent_id=?1 ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare domain decision query failed: {}", e))?;
                let rows = stmt
                    .query_map(params![agent_id], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query domain decisions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read domain decision row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse domain decision failed: {}", e))?,
                    );
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT payload_json FROM domain_access_decisions ORDER BY updated_at_us DESC",
                    )
                    .map_err(|e| format!("prepare domain decision query failed: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("query domain decisions failed: {}", e))?;
                for row in rows {
                    let payload =
                        row.map_err(|e| format!("read domain decision row failed: {}", e))?;
                    out.push(
                        serde_json::from_str(&payload)
                            .map_err(|e| format!("parse domain decision failed: {}", e))?,
                    );
                }
            }
        }
        Ok(out)
    }

    pub fn delete_domain_access_decision(&self, decision_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM domain_access_decisions WHERE decision_id=?1",
            params![decision_id],
        )
        .map_err(|e| format!("delete domain access decision failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn upsert_crawl_job(&self, job: &CrawlJob, updated_at_us: u64) -> Result<(), String> {
        let conn = self.connect()?;
        let payload =
            serde_json::to_string(job).map_err(|e| format!("serialize crawl job failed: {}", e))?;
        conn.execute(
            "INSERT INTO crawl_jobs (crawl_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(crawl_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![job.crawl_id, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("upsert crawl job failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_crawl_jobs(&self) -> Result<Vec<CrawlJob>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT payload_json FROM crawl_jobs ORDER BY updated_at_us DESC")
            .map_err(|e| format!("prepare crawl job query failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query crawl jobs failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read crawl job row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse crawl job failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn delete_crawl_job(&self, crawl_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM crawl_jobs WHERE crawl_id=?1",
            params![crawl_id],
        )
        .map_err(|e| format!("delete crawl job failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn upsert_watch_job(&self, job: &WatchJobRecord, updated_at_us: u64) -> Result<(), String> {
        let conn = self.connect()?;
        let payload =
            serde_json::to_string(job).map_err(|e| format!("serialize watch job failed: {}", e))?;
        conn.execute(
            "INSERT INTO watch_jobs (watch_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(watch_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![job.watch_id, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("upsert watch job failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_watch_jobs(&self) -> Result<Vec<WatchJobRecord>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT payload_json FROM watch_jobs ORDER BY updated_at_us DESC")
            .map_err(|e| format!("prepare watch job query failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query watch jobs failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read watch job row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse watch job failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn delete_watch_job(&self, watch_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM watch_jobs WHERE watch_id=?1",
            params![watch_id],
        )
        .map_err(|e| format!("delete watch job failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn upsert_website_memory(
        &self,
        record: &WebsiteMemoryRecord,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(record)
            .map_err(|e| format!("serialize website memory failed: {}", e))?;
        conn.execute(
            "INSERT INTO website_memory (record_id, domain, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(record_id) DO UPDATE SET
               domain=excluded.domain,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                record.record_id,
                record.domain,
                payload,
                updated_at_us as i64
            ],
        )
        .map_err(|e| format!("upsert website memory failed: {}", e))?;
        Ok(())
    }

    pub fn delete_website_memory(&self, record_id: &str) -> Result<(), String> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM website_memory WHERE record_id=?1",
            params![record_id],
        )
        .map_err(|e| format!("delete website memory failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_website_memory(
        &self,
        domain: Option<&str>,
    ) -> Result<Vec<WebsiteMemoryRecord>, String> {
        let conn = self.connect()?;
        let mut out = Vec::new();
        if let Some(domain) = domain {
            let mut stmt = conn
                .prepare(
                    "SELECT payload_json FROM website_memory
                     WHERE domain=?1 ORDER BY updated_at_us DESC",
                )
                .map_err(|e| format!("prepare website memory query failed: {}", e))?;
            let rows = stmt
                .query_map(params![domain], |row| row.get::<_, String>(0))
                .map_err(|e| format!("query website memory failed: {}", e))?;
            for row in rows {
                let payload = row.map_err(|e| format!("read website memory row failed: {}", e))?;
                out.push(
                    serde_json::from_str(&payload)
                        .map_err(|e| format!("parse website memory failed: {}", e))?,
                );
            }
        } else {
            let mut stmt = conn
                .prepare("SELECT payload_json FROM website_memory ORDER BY updated_at_us DESC")
                .map_err(|e| format!("prepare website memory query failed: {}", e))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| format!("query website memory failed: {}", e))?;
            for row in rows {
                let payload = row.map_err(|e| format!("read website memory row failed: {}", e))?;
                out.push(
                    serde_json::from_str(&payload)
                        .map_err(|e| format!("parse website memory failed: {}", e))?,
                );
            }
        }
        Ok(out)
    }
}
