use std::time::Instant;

use kascov_core::performance::{PerformanceMetrics, Stage};

#[derive(Debug, Default)]
pub struct ReadPoolMetrics {
    checkout: kascov_core::performance::LatencyHistogram,
    query: kascov_core::performance::LatencyHistogram,
}

impl ReadPoolMetrics {
    pub fn record_checkout(&self, duration: std::time::Duration) {
        self.checkout.record(duration);
    }

    pub fn record_query(&self, duration: std::time::Duration) {
        self.query.record(duration);
    }

    pub fn snapshot_json(&self) -> serde_json::Value {
        serde_json::json!({
            "checkout": self.checkout.snapshot(),
            "query": self.query.snapshot(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconcileTrigger {
    Initial,
    Notification { observed_at_ms: u64 },
    Watchdog,
    Disconnected { observed_at_ms: u64 },
}

pub struct ReconcileSchedule {
    wakeups: kascov_core::node::ChainWakeups,
    watchdog: std::time::Duration,
    initial: bool,
}

impl ReconcileSchedule {
    pub fn new(wakeups: kascov_core::node::ChainWakeups, watchdog: std::time::Duration) -> Self {
        Self {
            wakeups,
            watchdog,
            initial: true,
        }
    }

    pub async fn next(&mut self) -> ReconcileTrigger {
        if self.initial {
            self.initial = false;
            return ReconcileTrigger::Initial;
        }
        tokio::select! {
            wakeup = self.wakeups.recv() => wakeup.map(map_wakeup).unwrap_or_else(|| {
                ReconcileTrigger::Disconnected { observed_at_ms: now_ms() }
            }),
            _ = tokio::time::sleep(self.watchdog) => ReconcileTrigger::Watchdog,
        }
    }

    pub fn take_pending(&mut self) -> Option<ReconcileTrigger> {
        self.wakeups.try_recv().map(map_wakeup)
    }

    pub fn accepted_work_pending(&self) -> bool {
        self.initial || self.wakeups.has_pending()
    }
}

fn map_wakeup(wakeup: kascov_core::node::ChainWakeup) -> ReconcileTrigger {
    match wakeup.kind {
        kascov_core::node::ChainWakeupKind::VirtualChainChanged => ReconcileTrigger::Notification {
            observed_at_ms: wakeup.observed_at_ms,
        },
        kascov_core::node::ChainWakeupKind::Disconnected => ReconcileTrigger::Disconnected {
            observed_at_ms: wakeup.observed_at_ms,
        },
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn timed<T>(metrics: &PerformanceMetrics, stage: Stage, operation: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let value = operation();
    metrics.record(stage, started.elapsed());
    value
}

pub fn snapshot_json(metrics: &PerformanceMetrics) -> serde_json::Value {
    let stages = Stage::ALL
        .into_iter()
        .map(|stage| {
            (
                stage.as_str().to_owned(),
                serde_json::to_value(metrics.snapshot(stage)).unwrap(),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(stages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn snapshot_has_only_fixed_stage_names() {
        let metrics = PerformanceMetrics::new();
        metrics.record(Stage::Query, Duration::from_micros(8));
        let snapshot = snapshot_json(&metrics);

        assert_eq!(Stage::ALL.len(), snapshot.as_object().unwrap().len());
        assert_eq!(1, snapshot["query"]["count"]);
        assert_eq!(0, snapshot["serialization"]["count"]);
    }

    #[test]
    fn tuning_snapshot_exposes_the_selected_stage_2_tuple() {
        let snapshot = crate::tuning::TuningProfile::default().health_json();

        assert_eq!(1, snapshot["profile_version"]);
        assert_eq!("selected", snapshot["profile_status"]);
        assert_eq!(16, snapshot["fetch_ahead"]);
        assert_eq!(1_000, snapshot["wal_autocheckpoint_pages"]);
        assert_eq!(4, snapshot["read_pool_connections"]);
        assert_eq!(256, snapshot["replay_page_records"]);
    }

    #[tokio::test(start_paused = true)]
    async fn schedule_reconciles_immediately_on_start() {
        let (_, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut schedule = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        assert_eq!(ReconcileTrigger::Initial, schedule.next().await);
    }

    #[tokio::test(start_paused = true)]
    async fn schedule_wakes_on_a_notification() {
        let (publisher, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut schedule = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        assert_eq!(ReconcileTrigger::Initial, schedule.next().await);
        publisher.publish(kascov_core::node::ChainWakeupKind::VirtualChainChanged, 123);
        assert_eq!(
            ReconcileTrigger::Notification {
                observed_at_ms: 123
            },
            schedule.next().await
        );
    }

    #[tokio::test(start_paused = true)]
    async fn schedule_coalesces_notifications_and_clears_priority() {
        let (publisher, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut schedule = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        schedule.next().await;
        publisher.publish(kascov_core::node::ChainWakeupKind::VirtualChainChanged, 100);
        publisher.publish(kascov_core::node::ChainWakeupKind::VirtualChainChanged, 200);
        assert!(schedule.accepted_work_pending());
        assert_eq!(
            ReconcileTrigger::Notification {
                observed_at_ms: 200
            },
            schedule.next().await
        );
        assert!(!schedule.accepted_work_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_ends_a_session_and_a_new_session_reconciles_immediately() {
        let (publisher, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut schedule = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        schedule.next().await;
        publisher.publish(kascov_core::node::ChainWakeupKind::Disconnected, 456);
        assert_eq!(
            ReconcileTrigger::Disconnected {
                observed_at_ms: 456
            },
            schedule.next().await
        );

        let (_, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut reconnected = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        assert_eq!(ReconcileTrigger::Initial, reconnected.next().await);
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_wakes_a_silent_session() {
        let (publisher, wakeups) = kascov_core::node::chain_wakeup_channel();
        let mut schedule = ReconcileSchedule::new(wakeups, Duration::from_secs(5));
        schedule.next().await;
        let task = tokio::spawn(async move { schedule.next().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(ReconcileTrigger::Watchdog, task.await.unwrap());
        drop(publisher);
    }
}
