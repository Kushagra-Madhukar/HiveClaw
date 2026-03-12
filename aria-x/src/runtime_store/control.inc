impl RuntimeStore {
    pub fn upsert_control_document(
        &self,
        entry: &ControlDocumentEntry,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(entry)
            .map_err(|e| format!("serialize control document failed: {}", e))?;
        conn.execute(
            "INSERT INTO control_documents (document_id, workspace_root, kind, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(document_id) DO UPDATE SET
               workspace_root=excluded.workspace_root,
               kind=excluded.kind,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                entry.document_id,
                entry.workspace_root,
                control_document_kind_name(entry.kind),
                payload,
                updated_at_us as i64,
            ],
        )
        .map_err(|e| format!("write control document failed: {}", e))?;
        Ok(())
    }

    pub fn list_control_documents(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<ControlDocumentEntry>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM control_documents WHERE workspace_root=?1 ORDER BY updated_at_us ASC",
            )
            .map_err(|e| format!("prepare list control documents failed: {}", e))?;
        let rows = stmt
            .query_map(params![workspace_root], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query control documents failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read control document row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse control document failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn upsert_compaction_state(
        &self,
        state: &CompactionState,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(state)
            .map_err(|e| format!("serialize compaction state failed: {}", e))?;
        conn.execute(
            "INSERT INTO compaction_state (session_id, status, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(session_id) DO UPDATE SET
               status=excluded.status,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                uuid::Uuid::from_bytes(state.session_id).to_string(),
                compaction_status_name(state.status),
                payload,
                updated_at_us as i64,
            ],
        )
        .map_err(|e| format!("write compaction state failed: {}", e))?;
        Ok(())
    }

    pub fn read_compaction_state(&self, session_id: uuid::Uuid) -> Result<CompactionState, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM compaction_state WHERE session_id=?1",
                params![session_id.to_string()],
                |row| row.get(0),
            )
            .map_err(|e| format!("read compaction state failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse compaction state failed: {}", e))
    }

    pub fn upsert_provider_capability(
        &self,
        profile: &ProviderCapabilityProfile,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(profile)
            .map_err(|e| format!("serialize provider capability failed: {}", e))?;
        conn.execute(
            "INSERT INTO provider_capabilities (provider_id, adapter_family, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(provider_id) DO UPDATE SET
               adapter_family=excluded.adapter_family,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                profile.provider_id,
                format!("{:?}", profile.adapter_family),
                payload,
                updated_at_us as i64,
            ],
        )
        .map_err(|e| format!("write provider capability failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn read_provider_capability(
        &self,
        provider_id: &str,
    ) -> Result<ProviderCapabilityProfile, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM provider_capabilities WHERE provider_id=?1",
                params![provider_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read provider capability failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse provider capability failed: {}", e))
    }

    pub fn list_provider_capabilities(&self) -> Result<Vec<ProviderCapabilityProfile>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM provider_capabilities ORDER BY provider_id ASC",
            )
            .map_err(|e| format!("prepare list provider capabilities failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query provider capabilities failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read provider capability row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse provider capability failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn upsert_model_capability(
        &self,
        profile: &ModelCapabilityProfile,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(profile)
            .map_err(|e| format!("serialize model capability failed: {}", e))?;
        conn.execute(
            "INSERT INTO model_capabilities
             (model_ref, provider_id, model_id, adapter_family, source_kind, observed_at_us, expires_at_us, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(model_ref) DO UPDATE SET
               provider_id=excluded.provider_id,
               model_id=excluded.model_id,
               adapter_family=excluded.adapter_family,
               source_kind=excluded.source_kind,
               observed_at_us=excluded.observed_at_us,
               expires_at_us=excluded.expires_at_us,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![
                profile.model_ref.as_slash_ref(),
                profile.model_ref.provider_id,
                profile.model_ref.model_id,
                format!("{:?}", profile.adapter_family),
                format!("{:?}", profile.source),
                profile.observed_at_us as i64,
                profile.expires_at_us.map(|v| v as i64),
                payload,
                updated_at_us as i64,
            ],
        )
        .map_err(|e| format!("write model capability failed: {}", e))?;
        Ok(())
    }

    pub fn read_model_capability(
        &self,
        model_ref: &str,
    ) -> Result<ModelCapabilityProfile, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM model_capabilities WHERE model_ref=?1",
                params![model_ref],
                |row| row.get(0),
            )
            .map_err(|e| format!("read model capability failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse model capability failed: {}", e))
    }

    #[allow(dead_code)]
    pub fn list_model_capabilities_for_provider(
        &self,
        provider_id: &str,
    ) -> Result<Vec<ModelCapabilityProfile>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM model_capabilities WHERE provider_id=?1 ORDER BY model_id ASC",
            )
            .map_err(|e| format!("prepare list model capabilities failed: {}", e))?;
        let rows = stmt
            .query_map(params![provider_id], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query model capabilities failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read model capability row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse model capability failed: {}", e))?,
            );
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn append_model_capability_probe(
        &self,
        record: &ModelCapabilityProbeRecord,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(record)
            .map_err(|e| format!("serialize model capability probe failed: {}", e))?;
        conn.execute(
            "INSERT INTO model_capability_probes
             (probe_id, model_ref, provider_id, model_id, adapter_family, observed_at_us, expires_at_us, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                record.probe_id,
                record.model_ref.as_slash_ref(),
                record.model_ref.provider_id,
                record.model_ref.model_id,
                format!("{:?}", record.adapter_family),
                record.observed_at_us as i64,
                record.expires_at_us.map(|v| v as i64),
                payload,
            ],
        )
        .map_err(|e| format!("write model capability probe failed: {}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_model_capability_probes(
        &self,
        model_ref: &str,
    ) -> Result<Vec<ModelCapabilityProbeRecord>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM model_capability_probes WHERE model_ref=?1 ORDER BY observed_at_us DESC",
            )
            .map_err(|e| format!("prepare list model capability probes failed: {}", e))?;
        let rows = stmt
            .query_map(params![model_ref], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query model capability probes failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read model capability probe row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse model capability probe failed: {}", e))?,
            );
        }
        Ok(out)
    }

}
