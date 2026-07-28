use super::*;

/// Cap on concurrent SSE subscribers per network — extras are rejected with
/// 503 and stay on the polling path.
const MAX_STREAM_SUBSCRIBERS: usize = 512;
/// Broadcast buffer per network. A receiver that falls behind gets
/// `RecvError::Lagged`, skips ahead, and the client resyncs via the poll.
const STREAM_BUFFER: usize = 256;

/// One network's live event fan-out: the chain follower broadcasts each
/// covenant event as pre-serialized JSON; every open SSE connection holds a
/// receiver. Messages are hints — clients confirm through the polled feeds.
pub(super) struct LiveChannel {
    pub(super) tx: tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
    subscribers: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl LiveChannel {
    pub(super) fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(STREAM_BUFFER);
        Self {
            tx,
            subscribers: Default::default(),
        }
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
fn covenant_filter(param: Option<&str>) -> std::result::Result<Option<String>, ()> {
    let Some(raw) = param else { return Ok(None) };
    let id = raw.trim().to_ascii_lowercase();
    if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(());
    }
    Ok(Some(format!("\"covenant_id\":\"{id}\"")))
}

/// Substring probe, no JSON parse: fan-out messages are compact serde_json
/// strings, so a covenant event embeds `"covenant_id":"<hex>"` verbatim.
/// Non-covenant messages (reorg notices) don't match a filtered stream.
fn sse_event_matches(msg: &str, needle: Option<&str>) -> bool {
    needle.map_or(true, |n| msg.contains(n))
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
    let Ok(needle) = covenant_filter(params.get("covenant").map(String::as_str)) else {
        return (
            StatusCode::BAD_REQUEST,
            "bad covenant filter (want 64 hex chars)",
        )
            .into_response();
    };
    let Some((_, channel)) = state.live.iter().find(|(n, _)| *n == network) else {
        return (StatusCode::NOT_FOUND, "unknown network").into_response();
    };
    let performance = state
        .performance
        .iter()
        .find(|(candidate, _)| *candidate == network)
        .map(|(_, metrics)| metrics.clone())
        .expect("every configured network has performance metrics");
    // Reserve a subscriber slot; back out over the cap.
    if channel.subscribers.fetch_add(1, Ordering::AcqRel) >= MAX_STREAM_SUBSCRIBERS {
        channel.subscribers.fetch_sub(1, Ordering::AcqRel);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "stream full — use the polling feeds",
        )
            .into_response();
    }
    let slot = SubscriberSlot(channel.subscribers.clone());
    let rx = channel.tx.subscribe();

    // broadcast::Receiver is not a Stream; unfold avoids a tokio-stream dep.
    // The slot rides in the state so disconnects free it via Drop. Streams
    // also carry a hard lifetime: a client that connects and never reads
    // would otherwise pin a subscriber slot forever (keep-alives sink into
    // TCP buffers without erroring) — after the deadline the stream ends
    // cleanly and well-behaved clients (EventSource) reconnect on their own.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15 * 60);
    let stream = futures::stream::unfold(
        (rx, slot, needle, performance),
        move |(mut rx, slot, needle, performance)| async move {
            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Ok(msg)) => {
                        // Filtered streams drop non-matching events pre-emit; the
                        // keep-alive layer still shows the client a live socket.
                        if !sse_event_matches(&msg, needle.as_deref()) {
                            continue;
                        }
                        let event = {
                            let _delivery =
                                performance.timer(kascov_core::performance::Stage::StreamDelivery);
                            Event::default().data(&*msg)
                        };
                        return Some((
                            Ok::<_, std::convert::Infallible>(event),
                            (rx, slot, needle, performance),
                        ));
                    }
                    // Fell behind the buffer: skip ahead — clients resync by polling.
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return None,
                    // Lifetime reached — recycle the slot.
                    Err(_) => return None,
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
            Ok(Some(format!("\"covenant_id\":\"{id}\"")))
        );
        // uppercase input normalizes to the lowercase hex the follower emits
        assert_eq!(
            covenant_filter(Some(&"AB".repeat(32))),
            Ok(Some(format!("\"covenant_id\":\"{id}\"")))
        );
        assert_eq!(covenant_filter(Some("abcd")), Err(())); // too short
        assert_eq!(covenant_filter(Some(&"zz".repeat(32))), Err(())); // not hex
    }

    /// The filter must match exactly the JSON the follower's fan-out builds
    /// (same serde_json compact encoding, same field name).
    #[test]
    fn sse_filter_matches_fanout_shape() {
        let id = kascov_core::CovenantId([0xab; 32]);
        let other = kascov_core::CovenantId([0xcd; 32]);
        let msg = serde_json::json!({
            "covenant_id": id,
            "kind": "genesis",
            "txid": kascov_core::TxId([1; 32]),
            "accepting_daa": 12345,
        })
        .to_string();
        let reorg = serde_json::json!({ "kind": "reorg", "rolled_back": 2 }).to_string();

        let needle = covenant_filter(Some(&id.to_string())).unwrap();
        let wrong = covenant_filter(Some(&other.to_string())).unwrap();
        assert!(sse_event_matches(&msg, needle.as_deref()));
        assert!(!sse_event_matches(&msg, wrong.as_deref()));
        // reorg notices don't match a filtered stream
        assert!(!sse_event_matches(&reorg, needle.as_deref()));
        // unfiltered streams pass everything through
        assert!(sse_event_matches(&msg, None));
        assert!(sse_event_matches(&reorg, None));
    }
}
