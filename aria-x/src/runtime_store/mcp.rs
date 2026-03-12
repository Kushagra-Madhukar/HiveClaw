use super::*;

impl RuntimeStore {
    pub fn upsert_mcp_server(
        &self,
        profile: &McpServerProfile,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(profile)
            .map_err(|e| format!("serialize mcp server failed: {}", e))?;
        conn.execute(
            "INSERT INTO mcp_servers (server_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(server_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![profile.server_id, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("write mcp server failed: {}", e))?;
        Ok(())
    }

    pub fn list_mcp_servers(&self) -> Result<Vec<McpServerProfile>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare("SELECT payload_json FROM mcp_servers ORDER BY updated_at_us ASC")
            .map_err(|e| format!("prepare list mcp servers failed: {}", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query mcp servers failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read mcp server row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse mcp server failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn upsert_mcp_imported_tool(
        &self,
        tool: &McpImportedTool,
        updated_at_us: u64,
    ) -> Result<(), String> {
        self.upsert_mcp_import_payload(
            &tool.import_id,
            &tool.server_id,
            "tool",
            tool,
            updated_at_us,
        )
    }

    pub fn upsert_mcp_imported_prompt(
        &self,
        prompt: &McpImportedPrompt,
        updated_at_us: u64,
    ) -> Result<(), String> {
        self.upsert_mcp_import_payload(
            &prompt.import_id,
            &prompt.server_id,
            "prompt",
            prompt,
            updated_at_us,
        )
    }

    pub fn upsert_mcp_imported_resource(
        &self,
        resource: &McpImportedResource,
        updated_at_us: u64,
    ) -> Result<(), String> {
        self.upsert_mcp_import_payload(
            &resource.import_id,
            &resource.server_id,
            "resource",
            resource,
            updated_at_us,
        )
    }

    fn upsert_mcp_import_payload<T: serde::Serialize>(
        &self,
        import_id: &str,
        server_id: &str,
        kind: &str,
        payload_value: &T,
        updated_at_us: u64,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(payload_value)
            .map_err(|e| format!("serialize mcp import failed: {}", e))?;
        conn.execute(
            "INSERT INTO mcp_imports (import_id, server_id, kind, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(import_id) DO UPDATE SET
               server_id=excluded.server_id,
               kind=excluded.kind,
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            params![import_id, server_id, kind, payload, updated_at_us as i64],
        )
        .map_err(|e| format!("write mcp import failed: {}", e))?;
        Ok(())
    }

    pub fn list_mcp_imported_tools(&self, server_id: &str) -> Result<Vec<McpImportedTool>, String> {
        self.list_mcp_import_payloads(server_id, "tool")
    }

    pub fn list_mcp_imported_prompts(
        &self,
        server_id: &str,
    ) -> Result<Vec<McpImportedPrompt>, String> {
        self.list_mcp_import_payloads(server_id, "prompt")
    }

    pub fn list_mcp_imported_resources(
        &self,
        server_id: &str,
    ) -> Result<Vec<McpImportedResource>, String> {
        self.list_mcp_import_payloads(server_id, "resource")
    }

    fn list_mcp_import_payloads<T: for<'de> serde::Deserialize<'de>>(
        &self,
        server_id: &str,
        kind: &str,
    ) -> Result<Vec<T>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM mcp_imports WHERE server_id=?1 AND kind=?2 ORDER BY updated_at_us ASC",
            )
            .map_err(|e| format!("prepare list mcp imports failed: {}", e))?;
        let rows = stmt
            .query_map(params![server_id, kind], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query mcp imports failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read mcp import row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse mcp import failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn upsert_mcp_binding(&self, binding: &McpBindingRecord) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(binding)
            .map_err(|e| format!("serialize mcp binding failed: {}", e))?;
        conn.execute(
            "INSERT INTO mcp_bindings (binding_id, agent_id, server_id, target_kind, target_name, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(agent_id, server_id, target_kind, target_name) DO UPDATE SET
               binding_id=excluded.binding_id,
               payload_json=excluded.payload_json,
               created_at_us=excluded.created_at_us",
            rusqlite::params![
                binding.binding_id,
                binding.agent_id,
                binding.server_id,
                format!("{:?}", binding.primitive_kind).to_ascii_lowercase(),
                binding.target_name,
                payload,
                binding.created_at_us as i64,
            ],
        )
        .map_err(|e| format!("write mcp binding failed: {}", e))?;
        Ok(())
    }

    pub fn list_mcp_bindings_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Vec<McpBindingRecord>, String> {
        let conn = self.connect()?;
        let mut stmt = conn
            .prepare(
                "SELECT payload_json FROM mcp_bindings WHERE agent_id=?1 ORDER BY created_at_us ASC",
            )
            .map_err(|e| format!("prepare list mcp bindings failed: {}", e))?;
        let rows = stmt
            .query_map(rusqlite::params![agent_id], |row| row.get::<_, String>(0))
            .map_err(|e| format!("query mcp bindings failed: {}", e))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| format!("read mcp binding row failed: {}", e))?;
            out.push(
                serde_json::from_str(&payload)
                    .map_err(|e| format!("parse mcp binding failed: {}", e))?,
            );
        }
        Ok(out)
    }

    pub fn upsert_mcp_import_cache_record(
        &self,
        record: &McpImportCacheRecord,
    ) -> Result<(), String> {
        let conn = self.connect()?;
        let payload = serde_json::to_string(record)
            .map_err(|e| format!("serialize mcp import cache failed: {}", e))?;
        conn.execute(
            "INSERT INTO mcp_import_cache (server_id, payload_json, updated_at_us)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(server_id) DO UPDATE SET
               payload_json=excluded.payload_json,
               updated_at_us=excluded.updated_at_us",
            rusqlite::params![record.server_id, payload, record.refreshed_at_us as i64],
        )
        .map_err(|e| format!("write mcp import cache failed: {}", e))?;
        Ok(())
    }

    pub fn read_mcp_import_cache_record(
        &self,
        server_id: &str,
    ) -> Result<McpImportCacheRecord, String> {
        let conn = self.connect()?;
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM mcp_import_cache WHERE server_id=?1",
                rusqlite::params![server_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read mcp import cache failed: {}", e))?;
        serde_json::from_str(&payload).map_err(|e| format!("parse mcp import cache failed: {}", e))
    }
}
