#![allow(dead_code)]

#[path = "../src/fixture.rs"]
mod fixture;

use serde_json::json;

fn complete_report() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "sample_source": "deterministic_fixture",
        "hardware": {
            "os": "test-os",
            "architecture": "test-arch",
            "logical_cpus": 4
        },
        "workload": {
            "blocks": 100,
            "events": 400,
            "duration_seconds": 1.0
        },
        "latency": {
            "observations": 100,
            "p50_ms": 1.0,
            "p95_ms": 2.0,
            "p99_ms": 3.0
        },
        "throughput": {
            "blocks_per_second": 100.0,
            "events_per_second": 400.0
        },
        "resources": {
            "rss_bytes": 1024,
            "database_bytes": 2048,
            "wal_bytes": 0
        }
    })
}

#[test]
fn report_requires_each_measurement_group() {
    for field in ["hardware", "latency", "throughput", "resources"] {
        let mut report = complete_report();
        report.as_object_mut().unwrap().remove(field);
        let error = fixture::validate_report(&report).unwrap_err();
        assert!(
            error.to_string().contains(field),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn percentile_claim_requires_one_hundred_observations() {
    let mut report = complete_report();
    report["latency"]["observations"] = json!(99);
    let error = fixture::validate_report(&report).unwrap_err();
    assert!(error.to_string().contains("100 observations"));

    report["latency"]["p50_ms"] = serde_json::Value::Null;
    report["latency"]["p95_ms"] = serde_json::Value::Null;
    report["latency"]["p99_ms"] = serde_json::Value::Null;
    fixture::validate_report(&report).unwrap();
}

#[test]
fn report_separates_fixture_from_live_samples() {
    let report = complete_report();
    fixture::validate_report(&report).unwrap();

    let mut live = report;
    live["sample_source"] = json!("live_node");
    fixture::validate_report(&live).unwrap();

    live["sample_source"] = json!("mixed");
    assert!(fixture::validate_report(&live).is_err());
}
