use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kascov_core::store::{
    AcceptedBlockBatch, AcceptedTransaction, EventKind, NewEvent, NewUtxo, Store,
};
use kascov_core::store_delivery::DeliveryFilter;
use kascov_core::{
    pending_event_id, ApplicationDecoder, ApplicationOutput, ApplicationPreprocess, BlockHash,
    CovenantBinding, CovenantId, DecodeFailure, DeliveryKind, Network, Outpoint, Output,
    StreamCursor, Transaction, TxId,
};

const NETWORK: Network = Network::Testnet(10);

fn batch(index: u8, application: &str) -> AcceptedBlockBatch {
    let covenant_id = CovenantId([index; 32]);
    let txid = TxId([index.saturating_add(32); 32]);
    let output = Output {
        value: 10 + u64::from(index),
        spk_version: 0,
        spk_script: vec![0x51],
        covenant: Some(CovenantBinding {
            covenant_id,
            authorizing_input: 0,
        }),
    };
    let transaction = Transaction {
        txid,
        version: 1,
        inputs: vec![],
        outputs: vec![output.clone()],
        payload: b"ARGI-scenario".to_vec(),
    };
    let mut batch = AcceptedBlockBatch::empty(BlockHash([index; 32]));
    batch.accepting_daa = u64::from(index) * 100;
    batch.accepting_time_ms = u64::from(index) * 1_000;
    batch.accepting_blue_score = u64::from(index) * 100;
    batch.events.push(NewEvent {
        covenant_id,
        kind: EventKind::Genesis,
        txid,
        tx_index: 0,
        event_index: 0,
        payload: Some(transaction.payload.clone()),
        lane_namespace: None,
    });
    batch.created_utxos.push(NewUtxo {
        outpoint: Outpoint { txid, index: 0 },
        covenant_id,
        value: output.value,
        spk_version: output.spk_version,
        spk_script: output.spk_script.clone(),
    });
    batch.transactions.push(AcceptedTransaction {
        txid,
        transaction,
        application: ApplicationPreprocess {
            raw_envelope: Some(b"ARGI-scenario".to_vec()),
            application_payload: Some(vec![index]),
            outputs: vec![ApplicationOutput {
                output_index: 0,
                covenant_id,
                application_id: application.into(),
                artifact_id: [0xa0 + index; 32],
                actor_path: format!("Match.Player{index}"),
                state_json: format!(r#"{{"turn":{index}}}"#),
            }],
            failures: vec![],
        },
    });
    batch
}

#[test]
fn pending_acceptance_reorg_restart_replay_and_backup_restore_are_consistent() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("source.db");
    let backup = directory.path().join("backup.db");
    let mut store = Store::open(&database, NETWORK).unwrap();

    let first = batch(1, "duel");
    let second = batch(2, "other");
    let first_pending = pending_event_id(first.events[0].txid, first.events[0].covenant_id, 0);
    let dropped_pending = pending_event_id(TxId([0xee; 32]), CovenantId([0xef; 32]), 0);
    let first_commit = store.apply_accepted_block(&first).unwrap();
    let second_commit = store.apply_accepted_block(&second).unwrap();
    assert_eq!(
        Some(first_pending.as_str()),
        first_commit.deliveries[0].pending_id.as_deref()
    );
    assert_ne!(first_pending, dropped_pending);
    assert!(store
        .delivery_page(None, 10)
        .unwrap()
        .iter()
        .all(|record| record.pending_id.as_deref() != Some(dropped_pending.as_str())));

    let removed = store
        .rollback_removed_blocks(&[second.accepting_block])
        .unwrap();
    assert_eq!(1, removed.deliveries.len());
    assert_eq!(DeliveryKind::Removed, removed.deliveries[0].kind);
    assert_eq!(
        Some(second_commit.deliveries[0].cursor),
        removed.deliveries[0].source_cursor
    );

    let before_restart = store.delivery_page(None, 10).unwrap();
    let high_water = store.delivery_high_water().unwrap();
    store.backup_to(&backup).unwrap();
    drop(store);

    let restarted = Store::open_reader(&database, NETWORK).unwrap();
    assert_eq!(high_water, restarted.delivery_high_water().unwrap());
    assert_eq!(before_restart, restarted.delivery_page(None, 10).unwrap());
    assert_eq!(
        Some(first_commit.deliveries[0].applications[0].clone()),
        restarted
            .current_application_output("duel", "Match.Player1", &CovenantId([1; 32]))
            .unwrap()
    );
    let filtered = restarted
        .delivery_page_filtered(
            Some(StreamCursor {
                epoch: high_water.epoch,
                seq: 0,
            }),
            10,
            &DeliveryFilter {
                application_id: Some("duel".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(vec![first_commit.deliveries[0].clone()], filtered);
    drop(restarted);

    let restored = Store::open_reader(&backup, NETWORK).unwrap();
    assert_eq!(high_water, restored.delivery_high_water().unwrap());
    assert_eq!(before_restart, restored.delivery_page(None, 10).unwrap());
    assert_eq!(
        Some(first_commit.deliveries[0].applications[0].clone()),
        restored
            .current_application_output("duel", "Match.Player1", &CovenantId([1; 32]))
            .unwrap()
    );
}

#[test]
fn crash_worker() {
    let Some(mode) = std::env::var_os("KASCOV_SCENARIO_CRASH") else {
        return;
    };
    let database = std::path::PathBuf::from(std::env::var_os("KASCOV_SCENARIO_DB").unwrap());
    let marker = std::path::PathBuf::from(std::env::var_os("KASCOV_SCENARIO_PUBLISH").unwrap());
    let mut store = Store::open(&database, NETWORK).unwrap();
    if mode == "before_commit" {
        std::process::abort();
    }
    store.apply_accepted_block(&batch(3, "duel")).unwrap();
    if mode == "after_commit" {
        std::process::abort();
    }
    std::fs::write(marker, b"published").unwrap();
}

fn crash_at(mode: &str, database: &std::path::Path, marker: &std::path::Path) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_worker", "--nocapture"])
        .env("KASCOV_SCENARIO_CRASH", mode)
        .env("KASCOV_SCENARIO_DB", database)
        .env("KASCOV_SCENARIO_PUBLISH", marker)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "crash injection did not terminate the worker"
    );
}

#[test]
fn crash_boundaries_publish_only_after_commit_and_restart_replays_missed_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let before = directory.path().join("before.db");
    let before_marker = directory.path().join("before.published");
    crash_at("before_commit", &before, &before_marker);
    let before_store = Store::open_reader(&before, NETWORK).unwrap();
    assert!(before_store.delivery_page(None, 10).unwrap().is_empty());
    assert!(!before_marker.exists());
    drop(before_store);

    let after = directory.path().join("after.db");
    let after_marker = directory.path().join("after.published");
    crash_at("after_commit", &after, &after_marker);
    let after_store = Store::open_reader(&after, NETWORK).unwrap();
    let replay = after_store.delivery_page(None, 10).unwrap();
    assert_eq!(1, replay.len());
    assert_eq!(DeliveryKind::Accepted, replay[0].kind);
    assert!(!after_marker.exists());
}

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(directory: &std::path::Path, max_streams: usize) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let child = Command::new(env!("CARGO_BIN_EXE_kascov"))
        .args([
            "--rpc",
            "ws://127.0.0.1:1",
            "serve",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--networks",
            "testnet-10",
            "--db-dir",
            directory.to_str().unwrap(),
            "--max-streams",
            &max_streams.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = Server { child, port };
    for _ in 0..120 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("kascov scenario server did not listen");
}

fn open_request(port: u16, path: &str) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    stream.flush().unwrap();
    stream
}

fn read_until(stream: &mut TcpStream, needle: &str) -> String {
    let mut response = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                response.extend_from_slice(&buffer[..read]);
                if String::from_utf8_lossy(&response).contains(needle) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("scenario response read failed: {error}"),
        }
    }
    String::from_utf8(response).unwrap()
}

#[test]
fn filtered_replay_emits_checkpoint_and_slow_streams_hit_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("testnet-10.db");
    let mut store = Store::open(&database, NETWORK).unwrap();
    store.apply_accepted_block(&batch(4, "duel")).unwrap();
    store.apply_accepted_block(&batch(5, "duel")).unwrap();
    let current = store.delivery_high_water().unwrap();
    drop(store);

    let server = start_server(directory.path(), 2);
    let start = StreamCursor {
        epoch: current.epoch,
        seq: 0,
    };
    let mut filtered = open_request(
        server.port,
        &format!(
            "/data/testnet-10/stream?after={start}&covenant={}",
            "ff".repeat(32)
        ),
    );
    let response = read_until(&mut filtered, "event: checkpoint");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("event: checkpoint"), "{response}");
    assert!(response.contains(&format!("id: {current}")), "{response}");
    drop(server);

    let server = start_server(directory.path(), 1);
    let mut slow = open_request(server.port, "/data/testnet-10/stream");
    let ready = read_until(&mut slow, "event: ready");
    assert!(ready.starts_with("HTTP/1.1 200"), "{ready}");
    let mut rejected = open_request(server.port, "/data/testnet-10/stream");
    let overload = read_until(&mut rejected, "stream capacity exhausted");
    assert!(overload.starts_with("HTTP/1.1 503"), "{overload}");
    assert!(
        overload.to_ascii_lowercase().contains("retry-after: 1"),
        "{overload}"
    );
}

struct RepairDecoder(ApplicationOutput);

impl ApplicationDecoder for RepairDecoder {
    fn preprocess(&self, transaction: &Transaction) -> ApplicationPreprocess {
        ApplicationPreprocess {
            raw_envelope: Some(transaction.payload.clone()),
            application_payload: Some(vec![]),
            outputs: vec![self.0.clone()],
            failures: vec![],
        }
    }
}

#[test]
fn offline_repair_appends_one_idempotent_projection_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("repair.db");
    let mut store = Store::open(&database, NETWORK).unwrap();
    let mut batch = batch(6, "duel");
    let expected = batch.transactions[0].application.outputs.remove(0);
    batch.transactions[0]
        .application
        .failures
        .push(DecodeFailure {
            output_index: Some(0),
            application_id: Some("duel".into()),
            artifact_id: Some(expected.artifact_id),
            code: "application_not_approved".into(),
            detail: "scenario approval lag".into(),
        });
    store.apply_accepted_block(&batch).unwrap();

    let repaired = store
        .repair_application_failures(&RepairDecoder(expected.clone()), 10)
        .unwrap();
    assert_eq!(1, repaired.outputs_repaired);
    assert_eq!(1, repaired.deliveries_appended);
    let deliveries = store.delivery_page(None, 10).unwrap();
    assert_eq!(DeliveryKind::ProjectionRepaired, deliveries[1].kind);
    assert_eq!(Some(deliveries[0].cursor), deliveries[1].source_cursor);
    assert_eq!(
        Some(expected.clone()),
        store
            .current_application_output("duel", "Match.Player6", &CovenantId([6; 32]))
            .unwrap()
    );
    let repeated = store
        .repair_application_failures(&RepairDecoder(expected), 10)
        .unwrap();
    assert_eq!(0, repeated.deliveries_appended);
}
