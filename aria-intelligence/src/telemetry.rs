use super::*;

// ---------------------------------------------------------------------------
// Telemetry ring buffer + distillation engine
// ---------------------------------------------------------------------------

/// Fixed-capacity ring buffer for `<state, action, reward>` telemetry tuples.
///
/// Backed by a `VecDeque` with simple eviction semantics: inserting into a
/// full buffer drops the oldest entry.
#[derive(Debug)]
pub struct TelemetryRingBuffer {
    capacity: usize,
    entries: VecDeque<TelemetryLog>,
}

impl TelemetryRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TelemetryLog> {
        self.entries.iter()
    }

    /// Push a new telemetry record, evicting the oldest entry if at capacity.
    pub fn push(&mut self, log: TelemetryLog) {
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(log);
    }

    /// Count how many times a particular `mcp_action` appears in the buffer.
    pub fn count_action(&self, action: &str) -> usize {
        self.entries
            .iter()
            .filter(|e| e.mcp_action == action)
            .count()
    }
}

/// Result of a successful distillation run.
#[derive(Debug, Clone)]
pub struct DistilledSkill {
    /// Tool definition injected into the ToolManifestStore.
    pub tool: ToolDefinition,
    /// Skill registration entry added to the SkillManifest.
    pub registration: SkillRegistration,
    /// Compiled + signed Wasm module for deployment.
    pub signed_module: SignedModule,
}

/// Simple deployment interface so the distillation engine can publish signed
/// modules to target nodes without depending directly on the mesh layer.
pub trait DeploymentBus: Send + Sync {
    fn deploy(&self, node_id: &str, signed: &SignedModule) -> Result<(), String>;
}

/// Distillation engine that:
/// - Maintains a ring buffer of TelemetryLog entries
/// - Detects repeated tool chains (by `mcp_action`) over a threshold
/// - Synthesizes a distilled tool definition
/// - Compiles + signs a Wasm module (stub implementation)
/// - Registers the tool and publishes a deployment event via a `DeploymentBus`
pub struct DistillationEngine {
    buffer: TelemetryRingBuffer,
    threshold: usize,
    target_node: String,
    next_skill_id: u32,
}

impl DistillationEngine {
    /// Create a new engine with the specified ring-buffer capacity, pattern
    /// threshold, and default deployment target node.
    pub fn new(capacity: usize, threshold: usize, target_node: impl Into<String>) -> Self {
        Self {
            buffer: TelemetryRingBuffer::new(capacity),
            threshold: threshold.max(1),
            target_node: target_node.into(),
            next_skill_id: 1,
        }
    }

    pub fn buffer(&self) -> &TelemetryRingBuffer {
        &self.buffer
    }

    /// Ingest a new telemetry log and, if the associated `mcp_action` has
    /// reached the configured threshold, return a synthesized distilled skill.
    pub fn log_and_maybe_distill(&mut self, log: TelemetryLog) -> Option<DistilledSkill> {
        let action = log.mcp_action.clone();
        self.buffer.push(log);
        let count = self.buffer.count_action(&action);
        if count >= self.threshold {
            Some(self.distill_for_action(&action))
        } else {
            None
        }
    }

    fn distill_for_action(&mut self, action: &str) -> DistilledSkill {
        let skill_id = self.next_skill_id;
        self.next_skill_id += 1;

        let tool_name = Self::derive_tool_name(action, skill_id);
        let description = format!("Distilled skill for repeated pattern '{}'", action);

        let tool = ToolDefinition {
            name: tool_name.clone(),
            description,
            // Parameters deliberately minimal; production systems can use a
            // richer schema inferred from the original tool calls.
            parameters: r#"{"type":"object","properties":{}}"#.into(),
            embedding: Vec::new(),
            requires_strict_schema: false,
            streaming_safe: false,
            parallel_safe: true,
            modalities: vec![ToolModality::Text],
        };

        let registration = SkillRegistration {
            skill_id: format!("distilled-{}", skill_id),
            tool_name: tool_name.clone(),
            host_node_id: self.target_node.clone(),
        };

        let wasm_bytes = Self::compile_stub_wasm(&tool_name, action);
        let signed_module = Self::sign_stub_module(wasm_bytes);

        DistilledSkill {
            tool,
            registration,
            signed_module,
        }
    }

    fn derive_tool_name(action: &str, skill_id: u32) -> String {
        let mut base = action.replace(['→', ' '], "_");
        if base.is_empty() {
            base = "distilled_tool".into();
        }
        format!("{}_{}", base, skill_id)
    }

    fn compile_stub_wasm(tool_name: &str, action: &str) -> Vec<u8> {
        // Stub compilation pipeline: encode a small deterministic payload
        // that is non-empty and unique per (tool_name, action).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"wasm");
        bytes.extend_from_slice(tool_name.as_bytes());
        bytes.extend_from_slice(b"::");
        bytes.extend_from_slice(action.as_bytes());
        bytes
    }

    fn sign_stub_module(bytes: Vec<u8>) -> SignedModule {
        // Use a fixed dev/build key for deterministic signing.
        // In production, the build pipeline would use a proper CA-signed key.
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[2u8; 32]);
        aria_skill_runtime::sign_module(bytes, &signing_key)
    }

    /// Register a distilled skill into both the tool manifest store and the
    /// skill manifest, then deploy it via the provided `DeploymentBus`.
    pub fn register_and_deploy<B: DeploymentBus>(
        &self,
        distilled: &DistilledSkill,
        tool_store: &mut ToolManifestStore,
        manifest: &mut SkillManifest,
        bus: &B,
    ) -> Result<(), String> {
        let cached = CachedTool {
            name: distilled.tool.name.clone(),
            description: distilled.tool.description.clone(),
            parameters_schema: distilled.tool.parameters.clone(),
            embedding: distilled.tool.embedding.clone(),
            requires_strict_schema: distilled.tool.requires_strict_schema,
            streaming_safe: distilled.tool.streaming_safe,
            parallel_safe: distilled.tool.parallel_safe,
            modalities: distilled.tool.modalities.clone(),
        };
        tool_store.register(cached);
        manifest.registrations.push(distilled.registration.clone());
        bus.deploy(&self.target_node, &distilled.signed_module)
    }
}

/// Trait for an isolated learner which can refine reward models based on a
/// batch of telemetry logs. Implementations are expected to run in a
/// separate process or thread.
#[async_trait::async_trait]
pub trait LearnerBackend: Send + Sync {
    async fn refine_reward_model(&self, batch: Vec<TelemetryLog>) -> Result<(), String>;
}

impl DistillationEngine {
    /// Run a training cycle on the provided learner backend using the current
    /// contents of the ring buffer.
    pub async fn run_training_cycle<L: LearnerBackend>(&self, learner: &L) -> Result<(), String> {
        let batch: Vec<TelemetryLog> = self.buffer.iter().cloned().collect();
        if batch.is_empty() {
            return Ok(());
        }
        learner.refine_reward_model(batch).await
    }
}
