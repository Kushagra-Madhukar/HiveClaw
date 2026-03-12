use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

use aria_core::GatewayChannel;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelHealthEventKind {
    IngressEnqueued,
    IngressDequeued,
    IngressDropped,
    OutboundSent,
    OutboundFailed,
    Retry,
    AuthFailure,
    AdapterStarted,
    AdapterExited,
    AdapterRestarted,
    AdapterPanicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ChannelAdapterState {
    Starting,
    Running,
    Backoff,
    Stopped,
    Panicked,
}

#[derive(Debug, Default)]
struct ChannelHealthCounters {
    ingress_queue_depth: AtomicUsize,
    ingress_enqueued: AtomicU64,
    ingress_dropped: AtomicU64,
    outbound_sent: AtomicU64,
    outbound_failed: AtomicU64,
    retries: AtomicU64,
    auth_failures: AtomicU64,
    adapter_starts: AtomicU64,
    adapter_exits: AtomicU64,
    adapter_restarts: AtomicU64,
    adapter_panics: AtomicU64,
    adapter_state_code: AtomicUsize,
    last_event_at_us: AtomicU64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ChannelHealthSnapshot {
    pub channel: String,
    pub ingress_queue_depth: usize,
    pub ingress_enqueued: u64,
    pub ingress_dropped: u64,
    pub outbound_sent: u64,
    pub outbound_failed: u64,
    pub retries: u64,
    pub auth_failures: u64,
    pub adapter_state: String,
    pub adapter_starts: u64,
    pub adapter_exits: u64,
    pub adapter_restarts: u64,
    pub adapter_panics: u64,
    pub last_event_at_us: u64,
}

static CHANNEL_HEALTH: OnceLock<DashMap<String, Arc<ChannelHealthCounters>>> = OnceLock::new();

fn registry() -> &'static DashMap<String, Arc<ChannelHealthCounters>> {
    CHANNEL_HEALTH.get_or_init(DashMap::new)
}

fn now_us() -> u64 {
    chrono::Utc::now().timestamp_micros() as u64
}

fn channel_key(channel: GatewayChannel) -> String {
    match channel {
        GatewayChannel::Cli => "cli",
        GatewayChannel::Telegram => "telegram",
        GatewayChannel::WhatsApp => "whatsapp",
        GatewayChannel::Discord => "discord",
        GatewayChannel::Slack => "slack",
        GatewayChannel::IMessage => "imessage",
        GatewayChannel::WebSocket => "websocket",
        GatewayChannel::Ros2 => "ros2",
        GatewayChannel::Unknown => "unknown",
    }
    .to_string()
}

fn counters_for(channel: GatewayChannel) -> Arc<ChannelHealthCounters> {
    let key = channel_key(channel);
    registry()
        .entry(key)
        .or_insert_with(|| Arc::new(ChannelHealthCounters::default()))
        .clone()
}

fn adapter_state_name(code: usize) -> &'static str {
    match code {
        1 => "starting",
        2 => "running",
        3 => "backoff",
        4 => "stopped",
        5 => "panicked",
        _ => "unknown",
    }
}

fn set_adapter_state(counters: &ChannelHealthCounters, state: ChannelAdapterState) {
    let code = match state {
        ChannelAdapterState::Starting => 1,
        ChannelAdapterState::Running => 2,
        ChannelAdapterState::Backoff => 3,
        ChannelAdapterState::Stopped => 4,
        ChannelAdapterState::Panicked => 5,
    };
    counters.adapter_state_code.store(code, Ordering::Relaxed);
}

pub(crate) fn record_channel_health_event(channel: GatewayChannel, event: ChannelHealthEventKind) {
    let counters = counters_for(channel);
    match event {
        ChannelHealthEventKind::IngressEnqueued => {
            counters.ingress_enqueued.fetch_add(1, Ordering::Relaxed);
            counters.ingress_queue_depth.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::IngressDequeued => {
            counters
                .ingress_queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                    Some(depth.saturating_sub(1))
                })
                .ok();
        }
        ChannelHealthEventKind::IngressDropped => {
            counters.ingress_dropped.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::OutboundSent => {
            counters.outbound_sent.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::OutboundFailed => {
            counters.outbound_failed.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::Retry => {
            counters.retries.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::AuthFailure => {
            counters.auth_failures.fetch_add(1, Ordering::Relaxed);
        }
        ChannelHealthEventKind::AdapterStarted => {
            counters.adapter_starts.fetch_add(1, Ordering::Relaxed);
            set_adapter_state(&counters, ChannelAdapterState::Running);
        }
        ChannelHealthEventKind::AdapterExited => {
            counters.adapter_exits.fetch_add(1, Ordering::Relaxed);
            set_adapter_state(&counters, ChannelAdapterState::Stopped);
        }
        ChannelHealthEventKind::AdapterRestarted => {
            counters.adapter_restarts.fetch_add(1, Ordering::Relaxed);
            set_adapter_state(&counters, ChannelAdapterState::Backoff);
        }
        ChannelHealthEventKind::AdapterPanicked => {
            counters.adapter_panics.fetch_add(1, Ordering::Relaxed);
            set_adapter_state(&counters, ChannelAdapterState::Panicked);
        }
    }
    counters.last_event_at_us.store(now_us(), Ordering::Relaxed);
}

pub(crate) fn mark_channel_adapter_state(channel: GatewayChannel, state: &str) {
    let counters = counters_for(channel);
    match state {
        "starting" => set_adapter_state(&counters, ChannelAdapterState::Starting),
        "running" => set_adapter_state(&counters, ChannelAdapterState::Running),
        "backoff" => set_adapter_state(&counters, ChannelAdapterState::Backoff),
        "stopped" => set_adapter_state(&counters, ChannelAdapterState::Stopped),
        "panicked" => set_adapter_state(&counters, ChannelAdapterState::Panicked),
        _ => {}
    }
    counters.last_event_at_us.store(now_us(), Ordering::Relaxed);
}

pub(crate) fn snapshot_channel_health() -> Vec<ChannelHealthSnapshot> {
    let mut snapshots = registry()
        .iter()
        .map(|entry| {
            let channel = entry.key().clone();
            let counters = entry.value();
            ChannelHealthSnapshot {
                channel,
                ingress_queue_depth: counters.ingress_queue_depth.load(Ordering::Relaxed),
                ingress_enqueued: counters.ingress_enqueued.load(Ordering::Relaxed),
                ingress_dropped: counters.ingress_dropped.load(Ordering::Relaxed),
                outbound_sent: counters.outbound_sent.load(Ordering::Relaxed),
                outbound_failed: counters.outbound_failed.load(Ordering::Relaxed),
                retries: counters.retries.load(Ordering::Relaxed),
                auth_failures: counters.auth_failures.load(Ordering::Relaxed),
                adapter_state: adapter_state_name(
                    counters.adapter_state_code.load(Ordering::Relaxed),
                )
                .to_string(),
                adapter_starts: counters.adapter_starts.load(Ordering::Relaxed),
                adapter_exits: counters.adapter_exits.load(Ordering::Relaxed),
                adapter_restarts: counters.adapter_restarts.load(Ordering::Relaxed),
                adapter_panics: counters.adapter_panics.load(Ordering::Relaxed),
                last_event_at_us: counters.last_event_at_us.load(Ordering::Relaxed),
            }
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|snapshot| format!("{:?}", snapshot.channel));
    snapshots
}

#[cfg(test)]
pub(crate) fn reset_channel_health_for_tests() {
    registry().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_health_records_queue_and_delivery_counters() {
        reset_channel_health_for_tests();
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::IngressEnqueued);
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::IngressEnqueued);
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::IngressDequeued);
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::OutboundSent);
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::Retry);
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::AuthFailure);
        mark_channel_adapter_state(GatewayChannel::Cli, "running");
        record_channel_health_event(GatewayChannel::Cli, ChannelHealthEventKind::AdapterStarted);

        let snapshots = snapshot_channel_health();
        assert_eq!(snapshots.len(), 1);
        let cli = &snapshots[0];
        assert_eq!(cli.channel, "cli");
        assert_eq!(cli.ingress_queue_depth, 1);
        assert_eq!(cli.ingress_enqueued, 2);
        assert_eq!(cli.ingress_dropped, 0);
        assert_eq!(cli.outbound_sent, 1);
        assert_eq!(cli.outbound_failed, 0);
        assert_eq!(cli.retries, 1);
        assert_eq!(cli.auth_failures, 1);
        assert_eq!(cli.adapter_state, "running");
        assert_eq!(cli.adapter_starts, 1);
        assert!(cli.last_event_at_us > 0);
    }
}
