use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
use kascov_core::{BlockHash, CovenantId, Network, StreamCursor, StreamEpoch, TxId};

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
        "kascov-eventsource-cursor-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let database = directory.join("testnet-10.db");
    let mut store = Store::open(&database, Network::Testnet(10)).unwrap();
    let mut batch = AcceptedBlockBatch::empty(BlockHash([3; 32]));
    batch.accepting_daa = 100;
    batch.accepting_blue_score = 100;
    batch.events.push(NewEvent {
        covenant_id: CovenantId([1; 32]),
        kind: EventKind::Genesis,
        txid: TxId([2; 32]),
        tx_index: 0,
        event_index: 0,
        payload: None,
        lane_namespace: None,
    });
    let current = store.apply_accepted_block(&batch).unwrap().deliveries[0].cursor;
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

fn request(port: u16, method: &str, path: &str, headers: &[(&str, &str)], until: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n"
    )
    .unwrap();
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").unwrap();
    }
    write!(stream, "\r\n").unwrap();
    stream.flush().unwrap();

    let mut response = Vec::new();
    let mut buffer = [0; 4096];
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
            Err(error) => panic!("HTTP read failed: {error}"),
        }
    }
    String::from_utf8(response).unwrap()
}

fn assert_no_event_id(response: &str) {
    assert!(!response.lines().any(|line| line.starts_with("id:")), "{response}");
}

#[test]
fn eventsource_cursor_precedence_reset_and_transport_contract() {
    let server = start_server();
    let epoch = server.current.epoch;
    let start = StreamCursor { epoch, seq: 0 };
    let ahead = StreamCursor {
        epoch,
        seq: server.current.seq + 1,
    };

    let response = request(
        server.port,
        "GET",
        &format!("/data/testnet-10/stream?after={ahead}"),
        &[("Last-Event-ID", &start.to_string())],
        "event: ready",
    );
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains(&format!("\"after\":\"{start}\"")), "{response}");
    assert!(response.contains("retry: 1000"), "{response}");
    assert!(response.to_ascii_lowercase().contains("cache-control: no-store"));
    assert_no_event_id(&response);

    let response = request(
        server.port,
        "GET",
        &format!("/data/testnet-10/stream?after={start}"),
        &[],
        "event: ready",
    );
    assert!(response.contains(&format!("\"after\":\"{start}\"")), "{response}");

    let response = request(
        server.port,
        "GET",
        "/data/testnet-10/stream",
        &[],
        "event: ready",
    );
    assert!(
        response.contains(&format!("\"after\":\"{}\"", server.current)),
        "{response}"
    );

    for path in [
        "/data/testnet-10/stream?after=bad".to_owned(),
        format!("/data/testnet-10/stream?after={start}"),
    ] {
        let headers = if path.ends_with("after=bad") {
            vec![]
        } else {
            vec![("Last-Event-ID", "bad")]
        };
        let response = request(server.port, "GET", &path, &headers, "after must");
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    for (cursor, reason) in [
        (
            StreamCursor {
                epoch: StreamEpoch([0xff; 16]),
                seq: 0,
            },
            "foreign_epoch",
        ),
        (ahead, "ahead"),
    ] {
        let response = request(
            server.port,
            "GET",
            &format!("/data/testnet-10/stream?after={cursor}"),
            &[],
            "event: reset",
        );
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains(&format!("\"reason\":\"{reason}\"")), "{response}");
        assert!(response.contains("retry: 1000"), "{response}");
        assert_no_event_id(&response);
    }

    let response = request(
        server.port,
        "OPTIONS",
        "/data/testnet-10/stream",
        &[
            ("Origin", "https://argent.example"),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "last-event-id"),
        ],
        "\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response
            .to_ascii_lowercase()
            .lines()
            .any(|line| line.starts_with("access-control-allow-headers:")
                && line.contains("last-event-id")),
        "{response}"
    );
}
