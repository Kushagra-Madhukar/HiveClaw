use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub(crate) struct IngressQueueMetrics {
    pub queue_depth: AtomicUsize,
    pub enqueued: AtomicU64,
    pub dropped: AtomicU64,
}

pub(crate) struct IngressQueueBridge<T> {
    tx: mpsc::Sender<T>,
    metrics: Arc<IngressQueueMetrics>,
}

impl<T> Clone for IngressQueueBridge<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<T> IngressQueueBridge<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(capacity: usize) -> (Self, mpsc::Receiver<T>, Arc<IngressQueueMetrics>) {
        let (tx, rx) = mpsc::channel(capacity);
        let metrics = Arc::new(IngressQueueMetrics::default());
        (
            Self {
                tx,
                metrics: Arc::clone(&metrics),
            },
            rx,
            metrics,
        )
    }

    pub(crate) fn try_enqueue(&self, item: T) -> Result<(), T> {
        match self.tx.try_send(item) {
            Ok(()) => {
                self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
                self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(item))
            | Err(mpsc::error::TrySendError::Closed(item)) => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                Err(item)
            }
        }
    }

    pub(crate) fn mark_dequeued(&self) {
        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(crate) struct PartitionedIngressQueueBridge<T> {
    lanes: Vec<IngressQueueBridge<T>>,
}

impl<T> PartitionedIngressQueueBridge<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(
        partitions: usize,
        capacity_per_partition: usize,
    ) -> (Self, Vec<mpsc::Receiver<T>>, Vec<Arc<IngressQueueMetrics>>) {
        let partitions = partitions.max(1);
        let mut lanes = Vec::with_capacity(partitions);
        let mut receivers = Vec::with_capacity(partitions);
        let mut metrics = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let (lane, rx, lane_metrics) = IngressQueueBridge::new(capacity_per_partition);
            lanes.push(lane);
            receivers.push(rx);
            metrics.push(lane_metrics);
        }
        (Self { lanes }, receivers, metrics)
    }

    pub(crate) fn try_enqueue_by_key<K: Hash>(&self, item: T, key: &K) -> Result<usize, T> {
        let lane_idx = self.partition_for_key(key);
        self.lanes[lane_idx].try_enqueue(item).map(|()| lane_idx)
    }

    pub(crate) fn lane(&self, lane_idx: usize) -> IngressQueueBridge<T> {
        self.lanes[lane_idx].clone()
    }

    fn partition_for_key<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.lanes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aria_core::{AgentRequest, GatewayChannel, MessageContent};

    fn make_request(text: &str, channel: GatewayChannel) -> AgentRequest {
        AgentRequest {
            request_id: *uuid::Uuid::new_v4().as_bytes(),
            session_id: *uuid::Uuid::new_v4().as_bytes(),
            channel,
            user_id: "test-user".into(),
            content: MessageContent::Text(text.into()),
            tool_runtime_policy: None,
            timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
        }
    }

    #[tokio::test]
    async fn ingress_queue_bridge_preserves_fifo_order_for_mixed_channels() {
        let (bridge, mut rx, metrics) = IngressQueueBridge::new(8);
        let first = make_request("one", GatewayChannel::Cli);
        let second = make_request("two", GatewayChannel::Telegram);
        let third = make_request("three", GatewayChannel::Cli);
        bridge.try_enqueue(first).expect("enqueue first");
        bridge.try_enqueue(second).expect("enqueue second");
        bridge.try_enqueue(third).expect("enqueue third");
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 3);

        let mut seen = Vec::new();
        while let Some(req) = rx.recv().await {
            if let MessageContent::Text(text) = req.content {
                seen.push((req.channel, text));
            }
            bridge.mark_dequeued();
            if seen.len() == 3 {
                break;
            }
        }

        assert_eq!(
            seen,
            vec![
                (GatewayChannel::Cli, "one".to_string()),
                (GatewayChannel::Telegram, "two".to_string()),
                (GatewayChannel::Cli, "three".to_string())
            ]
        );
        assert_eq!(metrics.queue_depth.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.enqueued.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn partitioned_ingress_queue_bridge_preserves_same_key_order() {
        let (bridge, mut receivers, _metrics) = PartitionedIngressQueueBridge::new(4, 8);
        let first = make_request("one", GatewayChannel::Cli);
        let second = make_request("two", GatewayChannel::Cli);
        let third = make_request("three", GatewayChannel::Cli);
        let key = first.session_id;
        bridge
            .try_enqueue_by_key(first, &key)
            .expect("enqueue first");
        bridge
            .try_enqueue_by_key(second, &key)
            .expect("enqueue second");
        bridge
            .try_enqueue_by_key(third, &key)
            .expect("enqueue third");

        let lane_idx = {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            (hasher.finish() as usize) % receivers.len()
        };
        let rx = &mut receivers[lane_idx];
        let mut seen = Vec::new();
        while let Some(req) = rx.recv().await {
            if let MessageContent::Text(text) = req.content {
                seen.push(text);
            }
            if seen.len() == 3 {
                break;
            }
        }
        assert_eq!(seen, vec!["one", "two", "three"]);
    }
}
