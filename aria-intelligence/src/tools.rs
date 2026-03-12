use super::*;

// ---------------------------------------------------------------------------
// DynamicToolCache
// ---------------------------------------------------------------------------

/// Error type for the tool cache.
#[derive(Debug, PartialEq)]
pub enum CacheError {
    /// The session ceiling (hard limit) has been reached. No more unique
    /// tools may be added to this session.
    CeilingReached {
        ceiling: usize,
        attempted_total: usize,
    },
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::CeilingReached {
                ceiling,
                attempted_total,
            } => write!(
                f,
                "session ceiling reached: limit {}, attempted {}",
                ceiling, attempted_total
            ),
        }
    }
}

impl std::error::Error for CacheError {}

/// A cached tool entry (lightweight reference to a ToolDefinition).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CachedTool {
    /// Tool name (unique key).
    pub name: String,
    /// Tool description.
    pub description: String,
    /// JSON schema for parameters.
    pub parameters_schema: String,
    /// Precomputed semantic embedding for search/hot-swap.
    #[serde(default)]
    pub embedding: Vec<f32>,
    /// Whether this tool must only be exposed to strict-schema models.
    #[serde(default)]
    pub requires_strict_schema: bool,
    /// Whether streamed tool-argument assembly is safe for this tool.
    #[serde(default)]
    pub streaming_safe: bool,
    /// Whether the tool may execute in parallel with peer tool calls.
    #[serde(default = "default_tool_parallel_safe")]
    pub parallel_safe: bool,
    /// Modalities required by the tool. Empty means text-only.
    #[serde(default)]
    pub modalities: Vec<ToolModality>,
}

const fn default_tool_parallel_safe() -> bool {
    true
}

pub fn normalize_tool_schema(schema: &str) -> Result<String, String> {
    let raw = schema.trim();
    let value = if raw.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|e| format!("tool schema is not valid JSON: {}", e))?
    };
    let normalized = match value {
        serde_json::Value::Object(mut map) => {
            if map.is_empty() {
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": false
                })
            } else if map.get("type").is_some() || map.get("properties").is_some() {
                if !matches!(map.get("type"), Some(serde_json::Value::String(v)) if v == "object") {
                    map.insert("type".into(), serde_json::Value::String("object".into()));
                }
                if !map.contains_key("properties") {
                    map.insert("properties".into(), serde_json::json!({}));
                }
                if !map.contains_key("required") {
                    let mut required_keys = map
                        .get("properties")
                        .and_then(|v| v.as_object())
                        .map(|props| props.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    required_keys.sort();
                    map.insert(
                        "required".into(),
                        serde_json::Value::Array(
                            required_keys
                                .into_iter()
                                .map(serde_json::Value::String)
                                .collect::<Vec<_>>(),
                        ),
                    );
                }
                if !map.contains_key("additionalProperties") {
                    map.insert(
                        "additionalProperties".into(),
                        serde_json::Value::Bool(false),
                    );
                }
                serde_json::Value::Object(map)
            } else {
                let mut required_keys = map.keys().cloned().collect::<Vec<_>>();
                required_keys.sort();
                let required = required_keys
                    .into_iter()
                    .map(serde_json::Value::String)
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "type": "object",
                    "properties": map,
                    "required": required,
                    "additionalProperties": false
                })
            }
        }
        _ => {
            return Err("tool schema must be a JSON object".into());
        }
    };
    jsonschema::Validator::new(&normalized)
        .map_err(|e| format!("tool schema is not a valid JSON schema: {}", e))?;
    serde_json::to_string(&normalized)
        .map_err(|e| format!("serialize normalized schema failed: {}", e))
}

pub fn reduce_tool_schema_for_compat(schema: &str) -> Result<String, String> {
    let normalized = normalize_tool_schema(schema)?;
    let mut value = serde_json::from_str::<serde_json::Value>(&normalized)
        .map_err(|e| format!("parse normalized schema failed: {}", e))?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| String::from("normalized tool schema must be an object"))?;
    map.remove("additionalProperties");
    if let Some(properties) = map.get_mut("properties").and_then(|v| v.as_object_mut()) {
        for property in properties.values_mut() {
            if let Some(property_map) = property.as_object_mut() {
                property_map.remove("description");
                property_map.remove("examples");
            }
        }
    }
    serde_json::to_string(&value).map_err(|e| format!("serialize reduced schema failed: {}", e))
}

fn tool_requires_image_support(tool: &CachedTool) -> bool {
    tool.modalities.contains(&ToolModality::Image)
}

fn tool_requires_audio_support(tool: &CachedTool) -> bool {
    tool.modalities.contains(&ToolModality::Audio)
}

pub fn tool_is_compatible_with_model(
    tool: &CachedTool,
    profile: Option<&ModelCapabilityProfile>,
) -> bool {
    let Some(profile) = profile else {
        return true;
    };
    if matches!(
        profile.tool_schema_mode,
        aria_core::ToolSchemaMode::Unsupported
    ) {
        return false;
    }
    if tool.requires_strict_schema
        && !matches!(
            profile.tool_schema_mode,
            aria_core::ToolSchemaMode::StrictJsonSchema
        )
    {
        return false;
    }
    if tool_requires_image_support(tool)
        && !matches!(
            profile.supports_images,
            aria_core::CapabilitySupport::Supported
        )
    {
        return false;
    }
    if tool_requires_audio_support(tool)
        && !matches!(
            profile.supports_audio,
            aria_core::CapabilitySupport::Supported
        )
    {
        return false;
    }
    normalize_tool_schema(&tool.parameters_schema).is_ok()
}

pub(crate) fn tool_schema_fidelity_bonus(
    tool: &CachedTool,
    profile: Option<&ModelCapabilityProfile>,
) -> f32 {
    let Some(profile) = profile else {
        return 0.0;
    };
    if !tool_is_compatible_with_model(tool, Some(profile)) {
        return f32::NEG_INFINITY;
    }
    match profile.tool_schema_mode {
        aria_core::ToolSchemaMode::Unsupported => f32::NEG_INFINITY,
        aria_core::ToolSchemaMode::StrictJsonSchema => {
            if tool.requires_strict_schema {
                0.1
            } else {
                0.05
            }
        }
        aria_core::ToolSchemaMode::ReducedJsonSchema => {
            let value = match serde_json::from_str::<serde_json::Value>(&tool.parameters_schema) {
                Ok(value) => value,
                Err(_) => return -1.0,
            };
            let mut penalty = 0.0;
            if value
                .as_object()
                .and_then(|map| map.get("additionalProperties"))
                .is_some()
            {
                penalty += 0.05;
            }
            if let Some(properties) = value.get("properties").and_then(|v| v.as_object()) {
                for property in properties.values() {
                    if property
                        .as_object()
                        .map(|map| map.contains_key("description") || map.contains_key("examples"))
                        .unwrap_or(false)
                    {
                        penalty += 0.05;
                    }
                }
            }
            0.2 - penalty
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolVisibilityReason {
    Available,
    ToolSchemasUnsupported,
    StrictSchemaRequired,
    ImageModalityUnsupported,
    AudioModalityUnsupported,
    InvalidSchema(String),
}

impl ToolVisibilityReason {
    pub fn as_message(&self, tool_name: &str) -> String {
        match self {
            Self::Available => format!("tool '{}' is compatible with the active model", tool_name),
            Self::ToolSchemasUnsupported => format!(
                "tool '{}' is hidden because the active model does not support tool schemas",
                tool_name
            ),
            Self::StrictSchemaRequired => format!(
                "tool '{}' is hidden because it requires strict JSON schema support",
                tool_name
            ),
            Self::ImageModalityUnsupported => format!(
                "tool '{}' is hidden because the active model does not support image inputs",
                tool_name
            ),
            Self::AudioModalityUnsupported => format!(
                "tool '{}' is hidden because the active model does not support audio inputs",
                tool_name
            ),
            Self::InvalidSchema(err) => format!(
                "tool '{}' is hidden because its schema is invalid: {}",
                tool_name, err
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolVisibilityDecision {
    pub tool_name: String,
    pub available: bool,
    pub reason: ToolVisibilityReason,
}

pub fn explain_tool_visibility(
    tool: &CachedTool,
    profile: Option<&ModelCapabilityProfile>,
) -> ToolVisibilityDecision {
    let Some(profile) = profile else {
        return ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: normalize_tool_schema(&tool.parameters_schema).is_ok(),
            reason: normalize_tool_schema(&tool.parameters_schema)
                .map(|_| ToolVisibilityReason::Available)
                .unwrap_or_else(ToolVisibilityReason::InvalidSchema),
        };
    };
    if matches!(profile.tool_schema_mode, aria_core::ToolSchemaMode::Unsupported) {
        return ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: false,
            reason: ToolVisibilityReason::ToolSchemasUnsupported,
        };
    }
    if tool.requires_strict_schema
        && !matches!(
            profile.tool_schema_mode,
            aria_core::ToolSchemaMode::StrictJsonSchema
        )
    {
        return ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: false,
            reason: ToolVisibilityReason::StrictSchemaRequired,
        };
    }
    if tool_requires_image_support(tool)
        && !matches!(
            profile.supports_images,
            aria_core::CapabilitySupport::Supported
        )
    {
        return ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: false,
            reason: ToolVisibilityReason::ImageModalityUnsupported,
        };
    }
    if tool_requires_audio_support(tool)
        && !matches!(
            profile.supports_audio,
            aria_core::CapabilitySupport::Supported
        )
    {
        return ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: false,
            reason: ToolVisibilityReason::AudioModalityUnsupported,
        };
    }
    match normalize_tool_schema(&tool.parameters_schema) {
        Ok(_) => ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: true,
            reason: ToolVisibilityReason::Available,
        },
        Err(err) => ToolVisibilityDecision {
            tool_name: tool.name.clone(),
            available: false,
            reason: ToolVisibilityReason::InvalidSchema(err),
        },
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolSearchEntry {
    pub tool: CachedTool,
    pub score: Option<f32>,
    pub visibility: ToolVisibilityDecision,
}

/// LRU-based tool cache with context cap (soft) and session ceiling (hard).
///
/// - `context_cap`: Maximum tools kept in the active context. When exceeded,
///   the least-recently-used tool is evicted.
/// - `session_ceiling`: Absolute maximum unique tools across the session
///   lifetime. Exceeding this returns [`CacheError::CeilingReached`].
pub struct DynamicToolCache {
    context_cap: usize,
    session_ceiling: usize,
    /// Active tools in LRU order (back = most recent).
    active: VecDeque<CachedTool>,
    /// All tools ever seen in this session (for ceiling tracking).
    seen: HashMap<String, ()>,
}

impl DynamicToolCache {
    /// Create a new cache with the given limits.
    pub fn new(context_cap: usize, session_ceiling: usize) -> Self {
        Self {
            context_cap,
            session_ceiling,
            active: VecDeque::with_capacity(context_cap),
            seen: HashMap::new(),
        }
    }

    /// Insert a tool into the cache.
    ///
    /// - If the tool already exists, it's promoted to most-recently-used.
    /// - If `context_cap` is exceeded, the LRU tool is evicted.
    /// - If `session_ceiling` would be exceeded by a new unique tool,
    ///   returns [`CacheError::CeilingReached`].
    pub fn insert(&mut self, tool: CachedTool) -> Result<Option<CachedTool>, CacheError> {
        // If already in cache, promote it
        if let Some(pos) = self.active.iter().position(|t| t.name == tool.name) {
            self.active.remove(pos);
            self.active.push_back(tool);
            return Ok(None);
        }

        // New unique tool — check session ceiling
        if !self.seen.contains_key(&tool.name) {
            if self.seen.len() >= self.session_ceiling {
                return Err(CacheError::CeilingReached {
                    ceiling: self.session_ceiling,
                    attempted_total: self.seen.len() + 1,
                });
            }
            self.seen.insert(tool.name.clone(), ());
        }

        // Evict if at context cap
        let evicted = if self.active.len() >= self.context_cap {
            self.active.pop_front()
        } else {
            None
        };

        self.active.push_back(tool);
        Ok(evicted)
    }

    /// Get a tool by name, promoting it in the LRU order.
    pub fn get(&mut self, name: &str) -> Option<&CachedTool> {
        if let Some(pos) = self.active.iter().position(|t| t.name == name) {
            let tool = self.active.remove(pos).expect("just found");
            self.active.push_back(tool);
            self.active.back()
        } else {
            None
        }
    }

    /// Number of tools currently in the active context.
    pub fn len(&self) -> usize {
        self.active.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Total unique tools seen across the session.
    pub fn total_seen(&self) -> usize {
        self.seen.len()
    }

    /// Snapshot of currently active tools in LRU order.
    pub fn active_tools(&self) -> Vec<CachedTool> {
        self.active.iter().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// ToolManifestStore + search_tool_registry support
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ToolRegistryError {
    EmptyStore,
}

impl std::fmt::Display for ToolRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolRegistryError::EmptyStore => write!(f, "tool registry is empty"),
        }
    }
}

impl std::error::Error for ToolRegistryError {}

/// Registry of all known tools with simple semantic search support.
#[derive(Debug, Clone)]
pub struct ToolManifestStore {
    tools: Vec<CachedTool>,
}

impl Default for ToolManifestStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolManifestStore {
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    pub fn register(&mut self, tool: CachedTool) {
        self.tools.push(tool);
    }

    pub fn register_with_embedding<E: EmbeddingModel>(
        &mut self,
        mut tool: CachedTool,
        embedder: &E,
    ) -> Result<(), String> {
        tool.parameters_schema = normalize_tool_schema(&tool.parameters_schema)?;
        if tool.embedding.is_empty() {
            tool.embedding = embedder.embed(&format!("{} {}", tool.name, tool.description));
        }
        self.register(tool);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    pub fn get_by_name(&self, name: &str) -> Option<CachedTool> {
        self.tools.iter().find(|t| t.name == name).cloned()
    }

    pub fn search<E: EmbeddingModel>(
        &self,
        query: &str,
        embedder: &E,
        top_k: usize,
        profile: Option<&ModelCapabilityProfile>,
    ) -> Result<Vec<(CachedTool, f32)>, ToolRegistryError> {
        if self.tools.is_empty() {
            return Err(ToolRegistryError::EmptyStore);
        }
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let qv = embedder.embed(query);
        let mut ranked: Vec<(CachedTool, f32)> = self
            .tools
            .iter()
            .filter(|t| tool_is_compatible_with_model(t, profile))
            .cloned()
            .map(|t| {
                let tv = if t.embedding.is_empty() {
                    embedder.embed(&format!("{} {}", t.name, t.description))
                } else {
                    t.embedding.clone()
                };
                let score = cosine_similarity(&qv, &tv) + tool_schema_fidelity_bonus(&t, profile);
                (t, score)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(top_k);
        Ok(ranked)
    }

    pub fn search_with_explanations<E: EmbeddingModel>(
        &self,
        query: &str,
        embedder: &E,
        top_k: usize,
        profile: Option<&ModelCapabilityProfile>,
    ) -> Result<Vec<ToolSearchEntry>, ToolRegistryError> {
        if self.tools.is_empty() {
            return Err(ToolRegistryError::EmptyStore);
        }
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let qv = embedder.embed(query);
        let mut ranked: Vec<ToolSearchEntry> = self
            .tools
            .iter()
            .cloned()
            .map(|tool| {
                let visibility = explain_tool_visibility(&tool, profile);
                let score = if visibility.available {
                    let tv = if tool.embedding.is_empty() {
                        embedder.embed(&format!("{} {}", tool.name, tool.description))
                    } else {
                        tool.embedding.clone()
                    };
                    Some(cosine_similarity(&qv, &tv) + tool_schema_fidelity_bonus(&tool, profile))
                } else {
                    None
                };
                ToolSearchEntry {
                    tool,
                    score,
                    visibility,
                }
            })
            .collect();
        ranked.sort_by(|a, b| match (a.score, b.score) {
            (Some(left), Some(right)) => {
                right.partial_cmp(&left).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.tool.name.cmp(&b.tool.name),
        });
        ranked.truncate(top_k);
        Ok(ranked)
    }

    pub fn persist_to_path(&self, path: &Path) -> Result<(), String> {
        let payload = serde_json::to_string_pretty(&self.tools)
            .map_err(|e| format!("serialize tool registry failed: {}", e))?;
        std::fs::write(path, payload).map_err(|e| format!("write tool registry failed: {}", e))
    }

    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        let payload = std::fs::read_to_string(path)
            .map_err(|e| format!("read tool registry failed: {}", e))?;
        let tools: Vec<CachedTool> = serde_json::from_str(&payload)
            .map_err(|e| format!("parse tool registry failed: {}", e))?;
        Ok(Self { tools })
    }

    /// Implements the `search_tool_registry` behavior for one query.
    /// Returns the inserted tool on success.
    pub fn hot_swap_best<E: EmbeddingModel>(
        &self,
        cache: &mut DynamicToolCache,
        query: &str,
        embedder: &E,
        profile: Option<&ModelCapabilityProfile>,
    ) -> Result<Option<CachedTool>, CacheError> {
        let results = self.search(query, embedder, 1, profile);
        match results.ok().and_then(|mut v| v.pop()) {
            Some((tool, score)) => {
                debug!(
                    query = %query,
                    tool = %tool.name,
                    score = score,
                    "ToolManifestStore: hot_swap_best found tool"
                );
                cache.insert(tool.clone())?;
                Ok(Some(tool))
            }
            None => {
                debug!(query = %query, "ToolManifestStore: hot_swap_best no match");
                Ok(None)
            }
        }
    }

    pub fn validate_strict_startup_contract(&self) -> Result<(), String> {
        for tool in &self.tools {
            normalize_tool_schema(&tool.parameters_schema).map_err(|err| {
                format!("tool '{}' failed schema validation: {}", tool.name, err)
            })?;
        }
        Ok(())
    }
}
