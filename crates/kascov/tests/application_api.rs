use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kascov_core::store::{AcceptedBlockBatch, AcceptedTransaction, EventKind, NewEvent, NewUtxo, Store};
use kascov_core::{
    ApplicationOutput, ApplicationPreprocess, BlockHash, CovenantBinding, CovenantId,
    DecodeFailure, Network, Outpoint, Output, Transaction, TxId,
};

struct Server {
    child: Child,
    directory: std::path::PathBuf,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn start_server() -> Server {
    let directory = std::env::temp_dir().join(format!(
        "kascov-application-api-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("testnet-10.db");
    let mut store = Store::open(&database, Network::Testnet(10)).unwrap();
    let txid = TxId([2; 32]);
    let covenants = [CovenantId([1; 32]), CovenantId([4; 32])];
    let outputs: Vec<_> = covenants
        .iter()
        .enumerate()
        .map(|(index, covenant_id)| Output {
            value: 7 + index as u64,
            spk_version: 0,
            spk_script: vec![0x51 + index as u8],
            covenant: Some(CovenantBinding {
                covenant_id: *covenant_id,
                authorizing_input: 0,
            }),
        })
        .collect();
    let application_outputs: Vec<_> = covenants
        .iter()
        .enumerate()
        .map(|(index, covenant_id)| ApplicationOutput {
            output_index: index as u32,
            covenant_id: *covenant_id,
            application_id: "duel".into(),
            artifact_id: [3; 32],
            actor_path: "Match".into(),
            state_json: format!("{{\"turn\":{index}}}"),
        })
        .collect();
    let transaction = Transaction {
        txid,
        version: 1,
        inputs: vec![],
        outputs: outputs.clone(),
        payload: b"ARGI-fixture".to_vec(),
    };
    let mut batch = AcceptedBlockBatch::empty(BlockHash([5; 32]));
    batch.accepting_daa = 100;
    batch.accepting_time_ms = 1_000;
    batch.accepting_blue_score = 100;
    for (index, covenant_id) in covenants.iter().enumerate() {
        batch.events.push(NewEvent {
            covenant_id: *covenant_id,
            kind: EventKind::Genesis,
            txid,
            tx_index: 0,
            event_index: index as u32,
            payload: Some(transaction.payload.clone()),
            lane_namespace: None,
        });
        batch.created_utxos.push(NewUtxo {
            outpoint: Outpoint {
                txid,
                index: index as u32,
            },
            covenant_id: *covenant_id,
            value: outputs[index].value,
            spk_version: outputs[index].spk_version,
            spk_script: outputs[index].spk_script.clone(),
        });
    }
    batch.transactions.push(AcceptedTransaction {
        txid,
        transaction,
        application: ApplicationPreprocess {
            raw_envelope: Some(b"ARGI-fixture".to_vec()),
            application_payload: Some(vec![]),
            outputs: application_outputs,
            failures: vec![DecodeFailure {
                output_index: Some(1),
                application_id: Some("duel".into()),
                artifact_id: Some([3; 32]),
                code: "fixture_warning".into(),
                detail: "bounded fixture".into(),
            }],
        },
    });
    store.apply_accepted_block(&batch).unwrap();
    store.set_tip(110, 1_100).unwrap();
    drop(store);

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
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = Server {
        child,
        directory,
        port,
    };
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("kascov test server did not listen");
}

fn get_json(port: u16, path: &str) -> serde_json::Value {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let body = response.split_once("\r\n\r\n").unwrap().1;
    serde_json::from_str(body).unwrap()
}

fn assert_freshness(response: &serde_json::Value) {
    for field in [
        "network",
        "application",
        "stream_epoch",
        "stream_cursor",
        "processed_daa",
        "tip_daa",
        "projection_cursor",
        "projection_lag",
        "completeness",
        "freshness",
        "data",
    ] {
        assert!(response.get(field).is_some(), "missing {field}: {response}");
    }
    assert_eq!(response["processed_daa"], 100);
    assert_eq!(response["tip_daa"], 110);
}

#[test]
fn serves_current_covenant_actor_history_pending_and_failure_pages() {
    let server = start_server();
    let first = get_json(server.port, "/data/testnet-10/apps/duel/state?limit=1");
    assert_freshness(&first);
    assert_eq!(1, first["data"]["states"].as_array().unwrap().len());
    assert_eq!(first["data"]["has_more"], true);
    let after = first["data"]["next_after_id"].as_u64().unwrap();
    let second = get_json(
        server.port,
        &format!("/data/testnet-10/apps/duel/state?limit=1&after_id={after}"),
    );
    assert_eq!(1, second["data"]["states"].as_array().unwrap().len());

    let covenant = get_json(
        server.port,
        &format!(
            "/data/testnet-10/apps/duel/covenant/{}",
            CovenantId([1; 32])
        ),
    );
    assert_freshness(&covenant);
    assert_eq!(1, covenant["data"]["states"].as_array().unwrap().len());

    let actor = get_json(server.port, "/data/testnet-10/apps/duel/actor/Match");
    assert_freshness(&actor);
    assert_eq!(2, actor["data"]["states"].as_array().unwrap().len());

    let history = get_json(server.port, "/data/testnet-10/apps/duel/history?limit=10");
    assert_freshness(&history);
    assert_eq!(2, history["data"]["history"].as_array().unwrap().len());

    let pending = get_json(server.port, "/data/testnet-10/apps/duel/pending");
    assert_freshness(&pending);
    assert_eq!(0, pending["data"]["pending"].as_array().unwrap().len());

    let failures = get_json(server.port, "/data/testnet-10/apps/duel/failures?limit=1");
    assert_freshness(&failures);
    assert_eq!(1, failures["data"]["failures"].as_array().unwrap().len());

    let transaction = get_json(
        server.port,
        &format!("/data/testnet-10/apps/duel/tx/{}", TxId([2; 32])),
    );
    assert_freshness(&transaction);
    assert_eq!(2, transaction["data"]["outputs"].as_array().unwrap().len());

    let outpoint = get_json(
        server.port,
        &format!("/data/testnet-10/apps/duel/outpoint/{}/0", TxId([2; 32])),
    );
    assert_freshness(&outpoint);
    assert_eq!(outpoint["data"]["output"]["actor_path"], "Match");
}
