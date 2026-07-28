use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
use kascov_core::{BlockHash, CovenantId, Network, StreamCursor, TxId};

struct Server {
    child: Child,
    directory: std::path::PathBuf,
    port: u16,
    current: StreamCursor,
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
        "kascov-eventsource-replay-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("testnet-10.db");
    let mut store = Store::open(&database, Network::Testnet(10)).unwrap();
    let mut batch = AcceptedBlockBatch::empty(BlockHash([3; 32]));
    batch.accepting_daa = 100;
    batch.accepting_blue_score = 100;
    batch.events = (0..3)
        .map(|event_index| NewEvent {
            covenant_id: CovenantId([1; 32]),
            kind: EventKind::Transition,
            txid: TxId([2; 32]),
            tx_index: 0,
            event_index,
            payload: None,
            lane_namespace: None,
        })
        .collect();
    store.apply_accepted_block(&batch).unwrap();
    let current = store.delivery_high_water().unwrap();
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
        current,
    };
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return server;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("kascov test server did not listen");
}

fn stream_until(port: u16, path: &str, until: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    stream.flush().unwrap();
    let mut response = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                response.extend_from_slice(&buffer[..read]);
                if String::from_utf8_lossy(&response).contains(until) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break
            }
            Err(error) => panic!("SSE read failed: {error}"),
        }
    }
    String::from_utf8(response).unwrap()
}

#[test]
fn replays_durable_ids_and_filtered_checkpoints() {
    let server = start_server();
    let start = StreamCursor {
        epoch: server.current.epoch,
        seq: 0,
    };
    let response = stream_until(
        server.port,
        &format!("/data/testnet-10/stream?after={start}"),
        &format!("id: {}", server.current),
    );
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let ids: Vec<_> = response
        .lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .collect();
    assert_eq!(
        vec![
            format!("{}:1", server.current.epoch),
            format!("{}:2", server.current.epoch),
            format!("{}:3", server.current.epoch),
        ],
        ids
    );
    assert!(response.find("event: ready") < response.find("id: "));
    assert_eq!(3, response.matches("event: accepted").count());

    let response = stream_until(
        server.port,
        &format!(
            "/data/testnet-10/stream?after={start}&covenant={}",
            "ff".repeat(32)
        ),
        "event: checkpoint",
    );
    assert!(response.contains(&format!("id: {}", server.current)), "{response}");
    assert!(response.contains("event: checkpoint"), "{response}");
    assert!(!response.contains("event: accepted"), "{response}");
}
