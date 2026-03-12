use super::*;

// ---------------------------------------------------------------------------
// Cron scheduler subsystem
// ---------------------------------------------------------------------------

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ScheduleSpec {
    EverySeconds(u64),
    Cron(cron::Schedule, String, chrono_tz::Tz),
    Once(chrono::DateTime<chrono::Utc>),
    DailyAt {
        hour: u32,
        minute: u32,
        timezone: chrono_tz::Tz,
    },
    WeeklyAt {
        interval_weeks: u32,
        weekday: chrono::Weekday,
        hour: u32,
        minute: u32,
        timezone: chrono_tz::Tz,
    },
}

impl std::fmt::Debug for ScheduleSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleSpec::EverySeconds(s) => write!(f, "EverySeconds({})", s),
            ScheduleSpec::Cron(_, expr, timezone) => write!(f, "Cron({}@{})", expr, timezone),
            ScheduleSpec::Once(dt) => write!(f, "Once({})", dt),
            ScheduleSpec::DailyAt {
                hour,
                minute,
                timezone,
            } => {
                write!(f, "DailyAt({:02}:{:02}@{})", hour, minute, timezone)
            }
            ScheduleSpec::WeeklyAt {
                interval_weeks,
                weekday,
                hour,
                minute,
                timezone,
            } => write!(
                f,
                "WeeklyAt(every={}w,{:?},{:02}:{:02}@{})",
                interval_weeks, weekday, hour, minute, timezone
            ),
        }
    }
}

impl PartialEq for ScheduleSpec {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScheduleSpec::EverySeconds(a), ScheduleSpec::EverySeconds(b)) => a == b,
            (ScheduleSpec::Cron(_, a, atz), ScheduleSpec::Cron(_, b, btz)) => a == b && atz == btz,
            (ScheduleSpec::Once(a), ScheduleSpec::Once(b)) => a == b,
            (
                ScheduleSpec::DailyAt {
                    hour: ah,
                    minute: am,
                    timezone: atz,
                },
                ScheduleSpec::DailyAt {
                    hour: bh,
                    minute: bm,
                    timezone: btz,
                },
            ) => ah == bh && am == bm && atz == btz,
            (
                ScheduleSpec::WeeklyAt {
                    interval_weeks: ai,
                    weekday: aw,
                    hour: ah,
                    minute: am,
                    timezone: atz,
                },
                ScheduleSpec::WeeklyAt {
                    interval_weeks: bi,
                    weekday: bw,
                    hour: bh,
                    minute: bm,
                    timezone: btz,
                },
            ) => ai == bi && aw == bw && ah == bh && am == bm && atz == btz,
            _ => false,
        }
    }
}
impl Eq for ScheduleSpec {}

impl ScheduleSpec {
    pub fn parse(spec: &str) -> Option<Self> {
        let s = spec.trim();
        if let Some(v) = s.strip_prefix("every:") {
            let secs = v.trim_end_matches('s').parse::<u64>().ok()?;
            return (secs > 0).then_some(ScheduleSpec::EverySeconds(secs));
        }

        let (base, timezone) = split_schedule_timezone(s);
        let lower_base = base.to_ascii_lowercase();

        if let Some(hm) = lower_base.strip_prefix("daily@") {
            let (hour, minute) = parse_hh_mm(hm)?;
            return Some(ScheduleSpec::DailyAt {
                hour,
                minute,
                timezone,
            });
        }

        if let Some(rest) = lower_base.strip_prefix("weekly:") {
            let (weekday, hour, minute) = parse_weekday_at(rest)?;
            return Some(ScheduleSpec::WeeklyAt {
                interval_weeks: 1,
                weekday,
                hour,
                minute,
                timezone,
            });
        }
        if let Some(rest) = lower_base.strip_prefix("biweekly:") {
            let (weekday, hour, minute) = parse_weekday_at(rest)?;
            return Some(ScheduleSpec::WeeklyAt {
                interval_weeks: 2,
                weekday,
                hour,
                minute,
                timezone,
            });
        }

        if let Some((cron_tz, expr)) = parse_cron_with_timezone(s) {
            use std::str::FromStr;
            let normalized = normalize_cron_expr(expr);
            if let Ok(cron) = cron::Schedule::from_str(&normalized) {
                return Some(ScheduleSpec::Cron(cron, expr.to_string(), cron_tz));
            }
            return None;
        }

        if let Some(at_text) = s.strip_prefix("at:") {
            if let Some(dt) = parse_once_datetime(at_text.trim()) {
                return Some(ScheduleSpec::Once(dt));
            }
        }
        if let Some(dt) = parse_once_datetime(s) {
            return Some(ScheduleSpec::Once(dt));
        }

        // Handle one-shot delays: "delay:2m", "in:1h", "2m"
        let once_text = s
            .strip_prefix("delay:")
            .or_else(|| s.strip_prefix("in:"))
            .unwrap_or(s);

        if let Some(secs) = parse_duration_to_secs(once_text) {
            // If it's a simple number and was NOT prefixed by delay:/in:,
            // AND the test expects EverySeconds(5) for "*/5 * * * * *",
            // we should be careful.
            // Actually, if it contains '*' or '/', it's a cron.
            if !s.contains('*') && !s.contains('/') {
                let now = chrono::Utc::now();
                return Some(ScheduleSpec::Once(
                    now + chrono::Duration::try_seconds(secs as i64).unwrap(),
                ));
            }
        }

        // Use real cron evaluation.
        // Note: The `cron` crate expects 6 or 7 fields (sec min hour dom month dow [year]).
        // LLMs often give 5 fields (min hour dom month dow).
        use std::str::FromStr;
        let normalized = normalize_cron_expr(s);
        if let Ok(cron) = cron::Schedule::from_str(&normalized) {
            return Some(ScheduleSpec::Cron(cron, s.to_string(), chrono_tz::UTC));
        }

        None
    }

    pub fn is_once(&self) -> bool {
        matches!(self, ScheduleSpec::Once(_))
    }

    pub fn next_fire(&self, now: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
        match self {
            ScheduleSpec::EverySeconds(s) => {
                now + chrono::Duration::try_seconds(*s as i64).unwrap_or(chrono::Duration::days(1))
            }
            ScheduleSpec::Cron(c, _, timezone) => c
                .after(&now.with_timezone(timezone))
                .next()
                .map(|dt: chrono::DateTime<chrono_tz::Tz>| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|| now + chrono::Duration::days(365)),
            ScheduleSpec::Once(dt) => *dt,
            ScheduleSpec::DailyAt {
                hour,
                minute,
                timezone,
            } => next_daily_fire(now, *timezone, *hour, *minute),
            ScheduleSpec::WeeklyAt {
                interval_weeks,
                weekday,
                hour,
                minute,
                timezone,
            } => next_weekly_fire(now, *timezone, *interval_weeks, *weekday, *hour, *minute),
        }
    }
}

fn normalize_cron_expr(s: &str) -> String {
    let parts: Vec<&str> = s.split_whitespace().collect();
    match parts.len() {
        5 => format!("0 {}", s),
        6 | 7 => s.to_string(),
        _ => s.to_string(),
    }
}

fn split_schedule_timezone(spec: &str) -> (&str, chrono_tz::Tz) {
    if let Some((base, tz_text)) = spec.rsplit_once('#') {
        if let Ok(timezone) = tz_text.trim().parse::<chrono_tz::Tz>() {
            return (base.trim(), timezone);
        }
    }
    (spec, chrono_tz::UTC)
}

fn parse_cron_with_timezone(spec: &str) -> Option<(chrono_tz::Tz, &str)> {
    let rest = spec.strip_prefix("cron[")?;
    let (tz_text, expr) = rest.split_once("]:")?;
    let timezone = tz_text.trim().parse::<chrono_tz::Tz>().ok()?;
    Some((timezone, expr.trim()))
}

fn next_daily_fire(
    now: chrono::DateTime<chrono::Utc>,
    timezone: chrono_tz::Tz,
    hour: u32,
    minute: u32,
) -> chrono::DateTime<chrono::Utc> {
    use chrono::TimeZone;
    let now_local = now.with_timezone(&timezone);
    let mut target = now_local.date_naive().and_time(
        chrono::NaiveTime::from_hms_opt(hour, minute, 0).unwrap_or(chrono::NaiveTime::MIN),
    );
    if target <= now_local.naive_local() {
        target += chrono::Duration::days(1);
    }
    timezone
        .from_local_datetime(&target)
        .single()
        .or_else(|| timezone.from_local_datetime(&target).earliest())
        .or_else(|| timezone.from_local_datetime(&target).latest())
        .map(|dt: chrono::DateTime<chrono_tz::Tz>| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| now + chrono::Duration::days(1))
}

fn next_weekly_fire(
    now: chrono::DateTime<chrono::Utc>,
    timezone: chrono_tz::Tz,
    interval_weeks: u32,
    weekday: chrono::Weekday,
    hour: u32,
    minute: u32,
) -> chrono::DateTime<chrono::Utc> {
    use chrono::Datelike;
    use chrono::TimeZone;
    let now_local = now.with_timezone(&timezone);
    let target_time =
        chrono::NaiveTime::from_hms_opt(hour, minute, 0).unwrap_or(chrono::NaiveTime::MIN);
    let today = now_local.date_naive();
    let current_w = today.weekday().num_days_from_monday() as i64;
    let target_w = weekday.num_days_from_monday() as i64;
    let mut days_until = (target_w - current_w + 7) % 7;
    let mut candidate = today.and_time(target_time) + chrono::Duration::days(days_until);
    if candidate <= now_local.naive_local() {
        days_until += 7;
        candidate = today.and_time(target_time) + chrono::Duration::days(days_until);
    }

    let every = interval_weeks.max(1) as i64;
    if every > 1 {
        let anchor_monday = chrono::NaiveDate::from_ymd_opt(1970, 1, 5).unwrap_or(today);
        while candidate
            .date()
            .signed_duration_since(anchor_monday)
            .num_days()
            .div_euclid(7)
            .rem_euclid(every)
            != 0
        {
            candidate += chrono::Duration::days(7);
        }
    }

    timezone
        .from_local_datetime(&candidate)
        .single()
        .or_else(|| timezone.from_local_datetime(&candidate).earliest())
        .or_else(|| timezone.from_local_datetime(&candidate).latest())
        .map(|dt: chrono::DateTime<chrono_tz::Tz>| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|| now + chrono::Duration::days(7))
}

fn parse_duration_to_secs(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    let (digits, unit): (String, String) = s.chars().partition(|c| c.is_ascii_digit());
    let val = digits.parse::<u64>().ok()?;
    match unit.trim() {
        "s" | "" => Some(val),
        "m" | "min" | "mins" => Some(val * 60),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some(val * 3600),
        "d" | "day" | "days" => Some(val * 86400),
        _ => None,
    }
}

fn parse_hh_mm(s: &str) -> Option<(u32, u32)> {
    let mut parts = s.split(':');
    let hour = parts.next()?.trim().parse::<u32>().ok()?;
    let minute = parts.next()?.trim().parse::<u32>().ok()?;
    if parts.next().is_some() || hour > 23 || minute > 59 {
        return None;
    }
    Some((hour, minute))
}

fn parse_weekday(s: &str) -> Option<chrono::Weekday> {
    match s.trim() {
        "mon" | "monday" => Some(chrono::Weekday::Mon),
        "tue" | "tues" | "tuesday" => Some(chrono::Weekday::Tue),
        "wed" | "wednesday" => Some(chrono::Weekday::Wed),
        "thu" | "thurs" | "thursday" => Some(chrono::Weekday::Thu),
        "fri" | "friday" => Some(chrono::Weekday::Fri),
        "sat" | "saturday" => Some(chrono::Weekday::Sat),
        "sun" | "sunday" => Some(chrono::Weekday::Sun),
        _ => None,
    }
}

fn parse_weekday_at(s: &str) -> Option<(chrono::Weekday, u32, u32)> {
    let (day, hm) = s.split_once('@')?;
    let weekday = parse_weekday(day)?;
    let (hour, minute) = parse_hh_mm(hm)?;
    Some((weekday, hour, minute))
}

fn parse_once_datetime(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            ndt,
            chrono::Utc,
        ));
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M") {
        return Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            ndt,
            chrono::Utc,
        ));
    }
    None
}

impl Default for ScheduleSpec {
    fn default() -> Self {
        ScheduleSpec::EverySeconds(60)
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScheduledJobKind {
    Notify,
    #[default]
    Orchestrate,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScheduledJobStatus {
    #[default]
    Scheduled,
    Dispatched,
    Completed,
    Failed,
    ApprovalRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledJobAuditEntry {
    pub timestamp_us: u64,
    pub event: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledPromptJob {
    pub id: String,
    pub agent_id: String,
    #[serde(default)]
    pub creator_agent: Option<String>,
    #[serde(default)]
    pub executor_agent: Option<String>,
    #[serde(default)]
    pub notifier_agent: Option<String>,
    pub prompt: String,
    pub schedule_str: String,
    #[serde(default)]
    pub kind: ScheduledJobKind,
    /// Session this job belongs to, to enable context-aware execution.
    pub session_id: Option<Uuid>,
    pub user_id: Option<String>,
    pub channel: Option<GatewayChannel>,
    #[serde(default)]
    pub status: ScheduledJobStatus,
    #[serde(default)]
    pub last_run_at_us: Option<u64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub audit_log: Vec<ScheduledJobAuditEntry>,
    #[serde(skip)]
    pub schedule: ScheduleSpec,
}

impl ScheduledPromptJob {
    pub fn effective_agent_id(&self) -> &str {
        match self.kind {
            ScheduledJobKind::Notify => self.notifier_agent.as_deref().unwrap_or(&self.agent_id),
            ScheduledJobKind::Orchestrate => {
                self.executor_agent.as_deref().unwrap_or(&self.agent_id)
            }
        }
    }

    pub fn append_audit(
        &mut self,
        event: impl Into<String>,
        detail: Option<String>,
        timestamp_us: u64,
    ) {
        self.audit_log.push(ScheduledJobAuditEntry {
            timestamp_us,
            event: event.into(),
            detail,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduledPromptEvent {
    pub job_id: String,
    pub agent_id: String,
    pub creator_agent: Option<String>,
    pub executor_agent: Option<String>,
    pub notifier_agent: Option<String>,
    pub prompt: String,
    pub kind: ScheduledJobKind,
    pub session_id: Option<Uuid>,
    pub user_id: Option<String>,
    pub channel: Option<GatewayChannel>,
}
