use std::time::Instant;

use kascov_core::performance::{PerformanceMetrics, Stage};

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
}
