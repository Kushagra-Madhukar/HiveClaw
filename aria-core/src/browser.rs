use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserProfileMode {
    Ephemeral,
    ManagedPersistent,
    AttachedExternal,
    ExtensionBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserEngine {
    Chromium,
    Chrome,
    Edge,
    SafariBridge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfile {
    pub profile_id: String,
    pub display_name: String,
    pub mode: BrowserProfileMode,
    pub engine: BrowserEngine,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub persistent: bool,
    #[serde(default)]
    pub managed_by_aria: bool,
    #[serde(default)]
    pub attached_source: Option<String>,
    #[serde(default)]
    pub extension_binding_id: Option<String>,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub auth_enabled: bool,
    #[serde(default)]
    pub write_enabled: bool,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileBindingRecord {
    pub binding_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserSessionStatus {
    Launched,
    Paused,
    Exited,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionRecord {
    pub browser_session_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub engine: BrowserEngine,
    #[serde(default = "default_managed_browser_transport")]
    pub transport: BrowserTransportKind,
    pub status: BrowserSessionStatus,
    #[serde(default)]
    pub pid: Option<u32>,
    pub profile_dir: String,
    #[serde(default)]
    pub start_url: Option<String>,
    #[serde(default)]
    pub launch_command: Vec<String>,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

fn default_managed_browser_transport() -> BrowserTransportKind {
    BrowserTransportKind::ManagedBrowser
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionStateRecord {
    pub state_id: String,
    pub browser_session_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub storage_path: String,
    pub content_sha256_hex: String,
    #[serde(default)]
    pub last_restored_at_us: Option<u64>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserArtifactKind {
    LaunchMetadata,
    Screenshot,
    DomSnapshot,
    ExtractedText,
    Download,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserArtifactRecord {
    pub artifact_id: String,
    pub browser_session_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub kind: BrowserArtifactKind,
    pub mime_type: String,
    pub storage_path: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserActionKind {
    ProfileCreate,
    ProfileBind,
    SessionStart,
    SessionCleanup,
    SessionPause,
    SessionResume,
    SessionInspect,
    SessionStatePersist,
    SessionStateRestore,
    Wait,
    Click,
    Type,
    Select,
    Scroll,
    Screenshot,
    Download,
    Extract,
    Navigate,
    ChallengeDetected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserInteractionKind {
    Navigate,
    Wait,
    Click,
    Type,
    Select,
    Scroll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserActionRequest {
    #[serde(default)]
    pub browser_session_id: String,
    pub action: BrowserInteractionKind,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub selector: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserActionAuditRecord {
    pub audit_id: String,
    pub browser_session_id: Option<String>,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: Option<String>,
    pub action: BrowserActionKind,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserChallengeKind {
    Captcha,
    Mfa,
    BotDefense,
    LoginRequired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserChallengeEvent {
    pub event_id: String,
    pub browser_session_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub challenge: BrowserChallengeKind,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserLoginStateKind {
    LoggedOut,
    ManualPending,
    Authenticated,
    ChallengeRequired,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserLoginStateRecord {
    pub login_state_id: String,
    pub browser_session_id: String,
    pub session_id: Uuid,
    pub agent_id: String,
    pub profile_id: String,
    pub domain: String,
    pub state: BrowserLoginStateKind,
    #[serde(default)]
    pub credential_key_names: Vec<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub last_validated_at_us: Option<u64>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebActionFamily {
    Fetch,
    Crawl,
    Screenshot,
    InteractiveRead,
    InteractiveWrite,
    Login,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainDecisionKind {
    AllowOnce,
    AllowForSession,
    AllowAlways,
    DenyOnce,
    DenyAlways,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainDecisionScope {
    Domain,
    Session,
    Request,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainAccessDecision {
    pub decision_id: String,
    pub domain: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    pub action_family: WebActionFamily,
    pub decision: DomainDecisionKind,
    pub scope: DomainDecisionScope,
    pub created_by_user_id: String,
    pub created_at_us: u64,
    #[serde(default)]
    pub expires_at_us: Option<u64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrawlJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrawlJob {
    pub crawl_id: String,
    pub seed_url: String,
    pub scope: CrawlScope,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    pub max_depth: u16,
    pub max_pages: u32,
    #[serde(default)]
    pub render_js: bool,
    #[serde(default)]
    pub capture_screenshots: bool,
    #[serde(default)]
    pub change_detection: bool,
    pub initiated_by_agent: String,
    pub status: CrawlJobStatus,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchTargetKind {
    Page,
    Site,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchJobStatus {
    Scheduled,
    Running,
    Paused,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchJobRecord {
    pub watch_id: String,
    pub target_url: String,
    pub target_kind: WatchTargetKind,
    pub schedule_str: String,
    pub agent_id: String,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub capture_screenshots: bool,
    #[serde(default)]
    pub change_detection: bool,
    pub status: WatchJobStatus,
    #[serde(default)]
    pub last_checked_at_us: Option<u64>,
    #[serde(default)]
    pub next_check_at_us: Option<u64>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserChallengeFrequency {
    Unknown,
    Rare,
    Occasional,
    Frequent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebsiteMemoryRecord {
    pub record_id: String,
    pub domain: String,
    pub canonical_home_url: String,
    #[serde(default)]
    pub known_paths: Vec<String>,
    #[serde(default)]
    pub known_selectors: Vec<String>,
    #[serde(default)]
    pub known_login_entrypoints: Vec<String>,
    #[serde(default)]
    pub known_search_patterns: Vec<String>,
    #[serde(default)]
    pub last_successful_actions: Vec<String>,
    #[serde(default)]
    pub page_hashes: BTreeMap<String, String>,
    #[serde(default)]
    pub render_required: bool,
    pub challenge_frequency: BrowserChallengeFrequency,
    pub last_seen_at_us: u64,
    pub updated_at_us: u64,
}
