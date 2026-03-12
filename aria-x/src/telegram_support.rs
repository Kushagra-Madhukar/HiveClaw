use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize};

pub const TELEGRAM_DEDUPE_WINDOW: usize = 4096;
pub const TELEGRAM_UPDATE_QUEUE_CAPACITY: usize = 256;
pub const TELEGRAM_WORKER_COUNT: usize = 4;

#[derive(Default)]
pub struct ChannelMetrics {
    pub queue_depth: AtomicUsize,
    pub enqueued: AtomicU64,
    pub dropped: AtomicU64,
    pub processed: AtomicU64,
    pub deduped: AtomicU64,
}

#[derive(Default)]
pub struct DedupeWindow {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupeWindow {
    pub fn insert(&mut self, key: String) -> bool {
        if self.seen.contains(&key) {
            return false;
        }
        self.order.push_back(key.clone());
        self.seen.insert(key);
        while self.order.len() > TELEGRAM_DEDUPE_WINDOW {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_window_rejects_duplicate_keys() {
        let mut window = DedupeWindow::default();
        assert!(window.insert("u:1".to_string()));
        assert!(!window.insert("u:1".to_string()));
    }
}
