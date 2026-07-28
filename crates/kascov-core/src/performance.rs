use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

const BUCKETS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Stage {
    Reconciliation = 0,
    BodyResolution = 1,
    Classification = 2,
    Commit = 3,
    Publication = 4,
    Query = 5,
    Serialization = 6,
    StreamDelivery = 7,
}

impl Stage {
    pub const ALL: [Self; 8] = [
        Self::Reconciliation,
        Self::BodyResolution,
        Self::Classification,
        Self::Commit,
        Self::Publication,
        Self::Query,
        Self::Serialization,
        Self::StreamDelivery,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reconciliation => "reconciliation",
            Self::BodyResolution => "body_resolution",
            Self::Classification => "classification",
            Self::Commit => "commit",
            Self::Publication => "publication",
            Self::Query => "query",
            Self::Serialization => "serialization",
            Self::StreamDelivery => "stream_delivery",
        }
    }
}

#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn increment(&self) {
        self.add(1);
    }

    pub fn add(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct LatencyHistogram {
    count: Counter,
    total_us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            count: Counter::new(),
            total_us: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub fn record(&self, duration: Duration) {
        let micros = u64::try_from(duration.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let bucket = (u64::BITS - (micros - 1).leading_zeros()) as usize;
        self.buckets[bucket.min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(micros, Ordering::Relaxed);
        self.count.increment();
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let count = self.count.snapshot();
        LatencySnapshot {
            count,
            total_us: self.total_us.load(Ordering::Relaxed),
            p50_us: self.percentile(count, 50),
            p95_us: self.percentile(count, 95),
            p99_us: self.percentile(count, 99),
        }
    }

    fn percentile(&self, count: u64, percentile: u64) -> u64 {
        if count == 0 {
            return 0;
        }
        let target = (count * percentile).div_ceil(100);
        let mut seen = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            seen += bucket.load(Ordering::Relaxed);
            if seen >= target {
                return 1u64 << index;
            }
        }
        1u64 << (BUCKETS - 1)
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct LatencySnapshot {
    pub count: u64,
    pub total_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    stages: [LatencyHistogram; Stage::ALL.len()],
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            stages: std::array::from_fn(|_| LatencyHistogram::new()),
        }
    }

    pub fn record(&self, stage: Stage, duration: Duration) {
        self.stages[stage as usize].record(duration);
    }

    pub fn snapshot(&self, stage: Stage) -> LatencySnapshot {
        self.stages[stage as usize].snapshot()
    }

    pub fn timer(&self, stage: Stage) -> LatencyTimer<'_> {
        LatencyTimer {
            metrics: self,
            stage,
            started: Instant::now(),
        }
    }
}

pub struct LatencyTimer<'a> {
    metrics: &'a PerformanceMetrics,
    stage: Stage,
    started: Instant,
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        self.metrics.record(self.stage, self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::{Counter, LatencyHistogram, PerformanceMetrics, Stage};
    use std::time::Duration;

    #[test]
    fn histogram_uses_fixed_logarithmic_buckets() {
        let histogram = LatencyHistogram::new();
        histogram.record(Duration::from_micros(1));
        histogram.record(Duration::from_micros(2));
        histogram.record(Duration::from_micros(3));
        histogram.record(Duration::from_secs(2));

        let snapshot = histogram.snapshot();
        assert_eq!(4, snapshot.count);
        assert_eq!(2, snapshot.p50_us);
        assert_eq!(2_097_152, snapshot.p95_us);
        assert_eq!(snapshot.p95_us, snapshot.p99_us);
    }

    #[test]
    fn stage_percentiles_do_not_allocate_labels() {
        let metrics = PerformanceMetrics::new();
        for micros in 1..=100 {
            metrics.record(Stage::Commit, Duration::from_micros(micros));
        }

        let snapshot = metrics.snapshot(Stage::Commit);
        assert_eq!(100, snapshot.count);
        assert!(snapshot.p50_us >= 32);
        assert!(snapshot.p95_us >= 64);
        assert_eq!("commit", Stage::Commit.as_str());
        assert_eq!(8, Stage::ALL.len());
    }

    #[test]
    fn counter_snapshot_is_monotonic() {
        let counter = Counter::new();
        counter.increment();
        counter.add(4);
        assert_eq!(5, counter.snapshot());
        counter.increment();
        assert_eq!(6, counter.snapshot());
    }
}
