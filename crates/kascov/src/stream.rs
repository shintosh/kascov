use super::*;

/// Cap on concurrent SSE subscribers per network — extras are rejected with
/// 503 and stay on the polling path.
const MAX_STREAM_SUBSCRIBERS: usize = 512;
/// Committed records held per network before slow clients recover from the log.
const DELIVERY_BUFFER: usize = 1024;
/// Best-effort pending frames stay isolated from durable accepted delivery.
const PENDING_BUFFER: usize = 256;

/// One network's post-commit accepted-record fan-out.
pub(super) struct DeliveryHub {
    pub(super) tx: tokio::sync::broadcast::Sender<std::sync::Arc<kascov_core::DeliveryRecord>>,
    subscribers: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl DeliveryHub {
    pub(super) fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(DELIVERY_BUFFER);
        Self {
            tx,
            subscribers: Default::default(),
        }
    }
}

/// One network's process-local pending fan-out. Pending frames never carry a
/// durable cursor or consume accepted delivery capacity.
pub(super) struct PendingHub {
    pub(super) tx: tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
}

impl PendingHub {
    pub(super) fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(PENDING_BUFFER);
        Self { tx }
    }
}

/// Frees a subscriber slot when its SSE stream is dropped (client gone,
/// keep-alive write failed, or the connection timed out).
struct SubscriberSlot(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl Drop for SubscriberSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/* --------------------------------------------------------------- stream */

/// Parses the optional `?covenant=` SSE filter. `Ok(None)` when absent; the
/// substring needle to probe fan-out messages with when it's a well-formed
/// 64-hex id; `Err` on anything else (a typo'd filter must fail loudly, not
/// silently stream the whole firehose).
fn covenant_filter(param: Option<&str>) -> std::result::Result<Option<CovenantId>, ()> {
    let Some(raw) = param else { return Ok(None) };
    let id = raw.trim().to_ascii_lowercase();
    if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(());
    }
    id.parse().map(Some).map_err(|_| ())
}

fn delivery_matches(record: &kascov_core::DeliveryRecord, covenant: Option<CovenantId>) -> bool {
    covenant.is_none_or(|id| record.covenant_id == id)
}

fn pending_matches(msg: &str, covenant: Option<CovenantId>) -> bool {
    covenant.is_none_or(|id| msg.contains(&format!("\"covenant_id\":\"{id}\"")))
}

fn pending_sse_event(msg: &str) -> axum::response::sse::Event {
    axum::response::sse::Event::default().data(msg)
}

/// Push covenant events over SSE the moment the follower indexes them.
/// Hints only — no replay, no backlog, lagged subscribers skip ahead;
/// consumers confirm state through the polled feeds.
pub(super) async fn stream_handler(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<ServeState>>,
    axum::extract::Path(net_name): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::http::{header, HeaderName, HeaderValue, StatusCode};
    use axum::response::sse::{Event, KeepAlive, Sse};
    use axum::response::IntoResponse;
    use std::sync::atomic::Ordering;

    let network = match resolve_network(&state, &net_name) {
        Ok(n) => n,
        Err(resp) => return resp,
    };
    // Optional ?covenant=<64 hex>: narrow the fan-out to one coin's events.
    let Ok(covenant) = covenant_filter(params.get("covenant").map(String::as_str)) else {
        return (
            StatusCode::BAD_REQUEST,
            "bad covenant filter (want 64 hex chars)",
        )
            .into_response();
    };
    let Some((_, delivery_hub)) = state.deliveries.iter().find(|(n, _)| *n == network) else {
        return (StatusCode::NOT_FOUND, "unknown network").into_response();
    };
    let Some((_, pending_hub)) = state.pending_hubs.iter().find(|(n, _)| *n == network) else {
        return (StatusCode::NOT_FOUND, "unknown network").into_response();
    };
    let performance = state
        .performance
        .iter()
        .find(|(candidate, _)| *candidate == network)
        .map(|(_, metrics)| metrics.clone())
        .expect("every configured network has performance metrics");
    // Reserve a subscriber slot; back out over the cap.
    if delivery_hub.subscribers.fetch_add(1, Ordering::AcqRel) >= MAX_STREAM_SUBSCRIBERS {
        delivery_hub.subscribers.fetch_sub(1, Ordering::AcqRel);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "stream full — use the polling feeds",
        )
            .into_response();
    }
    let slot = SubscriberSlot(delivery_hub.subscribers.clone());
    let delivery_rx = delivery_hub.tx.subscribe();
    let pending_rx = pending_hub.tx.subscribe();

    // broadcast::Receiver is not a Stream; unfold avoids a tokio-stream dep.
    // The slot rides in the state so disconnects free it via Drop. Streams
    // also carry a hard lifetime: a client that connects and never reads
    // would otherwise pin a subscriber slot forever (keep-alives sink into
    // TCP buffers without erroring) — after the deadline the stream ends
    // cleanly and well-behaved clients (EventSource) reconnect on their own.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15 * 60);
    let stream = futures::stream::unfold(
        (delivery_rx, pending_rx, slot, covenant, performance),
        move |(mut delivery_rx, mut pending_rx, slot, covenant, performance)| async move {
            loop {
                tokio::select! {
                    delivery = delivery_rx.recv() => match delivery {
                        Ok(record) => {
                            if !delivery_matches(&record, covenant) { continue; }
                            let event = {
                                let _delivery = performance.timer(kascov_core::performance::Stage::StreamDelivery);
                                match Event::default()
                                    .id(record.cursor.to_string())
                                    .event(match record.kind {
                                        kascov_core::DeliveryKind::Accepted => "accepted",
                                        kascov_core::DeliveryKind::Removed => "removed",
                                        kascov_core::DeliveryKind::ProjectionRepaired => "projection_repaired",
                                    })
                                    .json_data(&*record)
                                {
                                    Ok(event) => event,
                                    Err(_) => continue,
                                }
                            };
                            return Some((Ok::<_, std::convert::Infallible>(event), (delivery_rx, pending_rx, slot, covenant, performance)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    },
                    pending = pending_rx.recv() => match pending {
                        Ok(msg) => {
                            if !pending_matches(&msg, covenant) { continue; }
                            let event = {
                                let _delivery = performance.timer(kascov_core::performance::Stage::StreamDelivery);
                                pending_sse_event(&msg)
                            };
                            return Some((Ok::<_, std::convert::Infallible>(event), (delivery_rx, pending_rx, slot, covenant, performance)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    },
                    _ = tokio::time::sleep_until(deadline) => return None,
                }
            }
        },
    );
    // Lead with a comment so headers and first bytes flush at accept time —
    // clients see the connection is live and buffering proxies commit to the
    // stream instead of holding a byteless response open.
    let stream = futures::stream::once(async {
        Ok::<_, std::convert::Infallible>(Event::default().comment("connected"))
    })
    .chain(stream);

    let mut resp = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(25))
                .text("ka"),
        )
        .into_response();
    let headers = resp.headers_mut();
    // no-store beats axum's default no-cache: the CDN must never coalesce a stream
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    // ask proxies not to buffer (nginx-style hint; Firebase may ignore it)
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covenant_filter_parses_and_rejects() {
        assert_eq!(covenant_filter(None), Ok(None));
        let id = "ab".repeat(32);
        assert_eq!(
            covenant_filter(Some(&id)),
            Ok(Some(CovenantId([0xab; 32])))
        );
        // uppercase input normalizes to the lowercase hex the follower emits
        assert_eq!(
            covenant_filter(Some(&"AB".repeat(32))),
            Ok(Some(CovenantId([0xab; 32])))
        );
        assert_eq!(covenant_filter(Some("abcd")), Err(())); // too short
        assert_eq!(covenant_filter(Some(&"zz".repeat(32))), Err(())); // not hex
    }

    #[test]
    fn filters_typed_deliveries_and_pending_json() {
        let id = CovenantId([0xab; 32]);
        let other = CovenantId([0xcd; 32]);
        let pending = serde_json::json!({
            "covenant_id": id,
            "kind": "pending",
        })
        .to_string();
        let delivery = kascov_core::DeliveryRecord {
            cursor: kascov_core::StreamCursor { epoch: kascov_core::StreamEpoch([2; 16]), seq: 1 },
            kind: kascov_core::DeliveryKind::Accepted,
            source_cursor: None,
            covenant_id: id,
            covenant_event_seq: 1,
            txid: TxId([1; 32]),
            accepting_block: BlockHash([3; 32]),
            accepting_daa: 12345,
            tx_index: Some(0),
            event_index: Some(0),
            order_complete: true,
            pending_id: None,
            applications: vec![],
        };

        assert!(delivery_matches(&delivery, Some(id)));
        assert!(!delivery_matches(&delivery, Some(other)));
        assert!(pending_matches(&pending, Some(id)));
        assert!(!pending_matches(&pending, Some(other)));
        assert!(delivery_matches(&delivery, None));
        assert!(pending_matches(&pending, None));
    }

    #[tokio::test]
    async fn pending_frames_never_emit_an_eventsource_id() {
        use axum::response::IntoResponse;
        let stream = futures::stream::once(async {
            Ok::<_, std::convert::Infallible>(pending_sse_event(r#"{"kind":"pending"}"#))
        });
        let response = axum::response::sse::Sse::new(stream).into_response();
        let bytes = axum::body::to_bytes(response.into_body(), 1_024)
            .await
            .unwrap();
        let wire = std::str::from_utf8(&bytes).unwrap();
        assert!(wire.contains("data: {\"kind\":\"pending\"}"));
        assert!(!wire.lines().any(|line| line.starts_with("id:")));
    }
}
