use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;

const MIN_PERCENTILE_OBSERVATIONS: usize = 100;

#[derive(Clone, Copy)]
struct FixedEvent {
    accepting_block: u64,
    tx_index: u32,
    event_index: u32,
    value: u64,
}

struct AcceptingBlockBatch {
    block: u64,
    events: Vec<FixedEvent>,
}

/// Deterministic source used by the fixture command. It models fixed accepted
/// batches without a node, network, clock-derived input, or persistent state.
struct FakeChainSource {
    next_block: u64,
    blocks: u64,
    events_per_block: u32,
}

impl FakeChainSource {
    fn new(blocks: u64, events_per_block: u32) -> Self {
        Self {
            next_block: 0,
            blocks,
            events_per_block,
        }
    }

    fn next_batch(&mut self) -> Option<AcceptingBlockBatch> {
        if self.next_block >= self.blocks {
            return None;
        }
        let block = self.next_block;
        self.next_block += 1;
        let events = (0..self.events_per_block)
            .map(|event_index| FixedEvent {
                accepting_block: block,
                tx_index: event_index,
                event_index,
                value: block
                    .wrapping_mul(1_000_003)
                    .wrapping_add(u64::from(event_index)),
            })
            .collect();
        Some(AcceptingBlockBatch { block, events })
    }
}

#[derive(Serialize)]
struct Hardware {
    os: &'static str,
    architecture: &'static str,
    logical_cpus: usize,
}

#[derive(Serialize)]
struct Workload {
    blocks: u64,
    events: u64,
    duration_seconds: f64,
    checksum: u64,
}

#[derive(Serialize)]
struct Latency {
    observations: usize,
    p50_ms: Option<f64>,
    p95_ms: Option<f64>,
    p99_ms: Option<f64>,
}

#[derive(Serialize)]
struct Throughput {
    blocks_per_second: f64,
    events_per_second: f64,
}

#[derive(Serialize)]
struct Resources {
    rss_bytes: u64,
    database_bytes: u64,
    wal_bytes: u64,
}

#[derive(Serialize)]
struct FixtureReport {
    schema_version: u32,
    sample_source: &'static str,
    source_identity: &'static str,
    node_identity: Option<&'static str>,
    hardware: Hardware,
    workload: Workload,
    latency: Latency,
    throughput: Throughput,
    resources: Resources,
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let rank = (percentile * sorted.len()).div_ceil(100).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn summarize_latency(mut samples: Vec<f64>) -> Latency {
    let observations = samples.len();
    if observations < MIN_PERCENTILE_OBSERVATIONS {
        return Latency {
            observations,
            p50_ms: None,
            p95_ms: None,
            p99_ms: None,
        };
    }
    samples.sort_by(f64::total_cmp);
    Latency {
        observations,
        p50_ms: Some(percentile(&samples, 50)),
        p95_ms: Some(percentile(&samples, 95)),
        p99_ms: Some(percentile(&samples, 99)),
    }
}

fn resident_bytes() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    output
        .ok()
        .filter(|result| result.status.success())
        .and_then(|result| String::from_utf8(result.stdout).ok())
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_mul(1024)
}

fn fixture_report(blocks: u64, events_per_block: u32) -> Result<FixtureReport> {
    if blocks == 0 {
        bail!("blocks must be greater than zero");
    }
    if events_per_block == 0 {
        bail!("events-per-block must be greater than zero");
    }

    let mut source = FakeChainSource::new(blocks, events_per_block);
    let started = Instant::now();
    let mut samples = Vec::with_capacity(usize::try_from(blocks).unwrap_or(usize::MAX));
    let mut checksum = 0u64;
    let mut observed_blocks = 0u64;
    let mut observed_events = 0u64;

    while let Some(batch) = source.next_batch() {
        let batch_started = Instant::now();
        let mut hasher = DefaultHasher::new();
        batch.block.hash(&mut hasher);
        for event in batch.events {
            event.accepting_block.hash(&mut hasher);
            event.tx_index.hash(&mut hasher);
            event.event_index.hash(&mut hasher);
            event.value.hash(&mut hasher);
            observed_events += 1;
        }
        checksum ^= hasher.finish();
        observed_blocks += 1;
        samples.push(batch_started.elapsed().as_secs_f64() * 1_000.0);
    }

    std::hint::black_box(checksum);
    let duration_seconds = started.elapsed().as_secs_f64().max(f64::EPSILON);
    Ok(FixtureReport {
        schema_version: 2,
        sample_source: "deterministic_fixture",
        source_identity: "kascov-bench:fixed-chain:v1",
        node_identity: None,
        hardware: Hardware {
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        },
        workload: Workload {
            blocks: observed_blocks,
            events: observed_events,
            duration_seconds,
            checksum,
        },
        latency: summarize_latency(samples),
        throughput: Throughput {
            blocks_per_second: observed_blocks as f64 / duration_seconds,
            events_per_second: observed_events as f64 / duration_seconds,
        },
        resources: Resources {
            rss_bytes: resident_bytes(),
            database_bytes: 0,
            wal_bytes: 0,
        },
    })
}

pub fn write_fixture_report(blocks: u64, events_per_block: u32, output: &Path) -> Result<()> {
    let report = fixture_report(blocks, events_per_block)?;
    let value = serde_json::to_value(&report)?;
    validate_report(&value)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create report directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&value)?;
    fs::write(output, bytes).with_context(|| format!("write report {}", output.display()))
}

pub fn validate_report(report: &Value) -> Result<()> {
    let object = report.as_object().context("report must be a JSON object")?;
    for field in ["hardware", "workload", "latency", "throughput", "resources"] {
        if !object.get(field).is_some_and(Value::is_object) {
            bail!("report requires object field {field}");
        }
    }
    match object.get("sample_source").and_then(Value::as_str) {
        Some("deterministic_fixture" | "live_node") => {}
        _ => bail!("sample_source must be deterministic_fixture or live_node"),
    }
    if !object
        .get("source_identity")
        .and_then(Value::as_str)
        .is_some_and(|identity| !identity.is_empty())
    {
        bail!("source_identity is required");
    }
    if object.get("sample_source").and_then(Value::as_str) == Some("live_node")
        && !object
            .get("node_identity")
            .and_then(Value::as_str)
            .is_some_and(|identity| !identity.is_empty())
    {
        bail!("live_node samples require node_identity");
    }
    let latency = object["latency"].as_object().unwrap();
    let observations = latency
        .get("observations")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let claims_percentiles = ["p50_ms", "p95_ms", "p99_ms"]
        .iter()
        .any(|field| latency.get(*field).is_some_and(|value| !value.is_null()));
    if observations < MIN_PERCENTILE_OBSERVATIONS as u64 && claims_percentiles {
        bail!("percentile claims require at least 100 observations");
    }
    Ok(())
}
