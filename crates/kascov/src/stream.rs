use super::*;

pub(super) const DEFAULT_MAX_STREAMS: usize = 512;
pub(super) const DEFAULT_MAX_REPLAYS: usize = 32;
#[cfg(test)]
pub(super) const DEFAULT_REPLAY_PAGE_SIZE: u64 = crate::tuning::DEFAULT_REPLAY_PAGE;
pub(super) const DEFAULT_EVENT_PAGE_SIZE: u64 = 1_000;
pub(super) const MAX_STREAM_REQUEST_BYTES: usize = 4 * 1024;
pub(super) const MAX_STREAM_EVENT_BYTES: usize = 1024 * 1024;
pub(super) const MAX_JSON_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct CapacityLimits {
    pub max_streams: usize,
    pub max_replays: usize,
    pub replay_page_size: u64,
    pub event_page_size: u64,
}

impl CapacityLimits {
    pub fn validate(self) -> anyhow::Result<Self> {
        if self.max_streams == 0
            || self.max_replays == 0
            || !crate::tuning::REPLAY_PAGE_CANDIDATES.contains(&self.replay_page_size)
            || self.event_page_size == 0
        {
            anyhow::bail!("stream capacity values are invalid or replay-page-size is not a fixed candidate");
        }
        Ok(self)
    }
}
/// Committed records held per network before slow clients recover from the log.
const DELIVERY_BUFFER: usize = 1024;
/// Best-effort pending frames stay isolated from durable accepted delivery.
const PENDING_BUFFER: usize = 256;

/// One network's post-commit accepted-record fan-out.
pub(super) struct DeliveryHub {
    pub(super) tx: tokio::sync::broadcast::Sender<std::sync::Arc<kascov_core::DeliveryRecord>>,
    subscribers: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    max_subscribers: usize,
    replays: std::sync::Arc<tokio::sync::Semaphore>,
    max_replays: usize,
}

impl DeliveryHub {
    pub(super) fn new(limits: CapacityLimits) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(DELIVERY_BUFFER);
        Self {
            tx,
            subscribers: Default::default(),
            max_subscribers: limits.max_streams,
            replays: std::sync::Arc::new(tokio::sync::Semaphore::new(limits.max_replays)),
            max_replays: limits.max_replays,
        }
    }

    fn try_subscribe(&self) -> Option<SubscriberSlot> {
        use std::sync::atomic::Ordering;
        let previous = self.subscribers.fetch_add(1, Ordering::AcqRel);
        if previous >= self.max_subscribers {
            self.subscribers.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(SubscriberSlot(self.subscribers.clone()))
    }

    fn try_replay(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.replays.clone().try_acquire_owned().ok()
    }

    pub(super) fn capacity_json(&self, limits: CapacityLimits) -> serde_json::Value {
        serde_json::json!({
            "max_streams": self.max_subscribers,
            "active_streams": self.subscribers.load(std::sync::atomic::Ordering::Relaxed),
            "max_historical_replays": self.max_replays,
            "active_historical_replays": self.max_replays.saturating_sub(self.replays.available_permits()),
            "replay_page_records": limits.replay_page_size,
            "event_page_records": limits.event_page_size,
            "max_request_bytes": MAX_STREAM_REQUEST_BYTES,
            "max_stream_event_bytes": MAX_STREAM_EVENT_BYTES,
            "max_json_response_bytes": MAX_JSON_RESPONSE_BYTES,
        })
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

pub(super) fn delivery_filter(
    params: &std::collections::HashMap<String, String>,
) -> std::result::Result<kascov_core::store_delivery::DeliveryFilter, &'static str> {
    let covenant_id = params
        .get("covenant")
        .map(|raw| raw.trim().to_ascii_lowercase().parse())
        .transpose()
        .map_err(|_| "covenant must be 64 lowercase or uppercase hex characters")?;
    let application_id = bounded_filter(params.get("application"), 128, "application")?;
    let actor_path = bounded_filter(params.get("actor"), 256, "actor")?;
    let artifact_id = params
        .get("artifact")
        .map(|raw| {
            let mut id = [0; 32];
            hex::decode_to_slice(raw.trim(), &mut id)
                .map(|_| id)
                .map_err(|_| "artifact must be 64 hex characters")
        })
        .transpose()?;
    Ok(kascov_core::store_delivery::DeliveryFilter {
        covenant_id,
        application_id,
        artifact_id,
        actor_path,
    })
}

fn bounded_filter(
    value: Option<&String>,
    max: usize,
    name: &'static str,
) -> std::result::Result<Option<String>, &'static str> {
    let Some(value) = value else { return Ok(None) };
    let value = value.trim();
    if value.is_empty() || value.len() > max {
        return Err(match name {
            "application" => "application must be 1..=128 bytes",
            _ => "actor must be 1..=256 bytes",
        });
    }
    Ok(Some(value.to_owned()))
}

fn stream_request_bytes(
    params: &std::collections::HashMap<String, String>,
    headers: &axum::http::HeaderMap,
) -> usize {
    let query_bytes = params
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(2))
        .sum::<usize>();
    query_bytes.saturating_add(
        headers
            .get("last-event-id")
            .map_or(0, |value| value.as_bytes().len()),
    )
}

fn delivery_matches(
    record: &kascov_core::DeliveryRecord,
    filter: &kascov_core::store_delivery::DeliveryFilter,
) -> bool {
    if filter
        .covenant_id
        .is_some_and(|id| record.covenant_id != id)
    {
        return false;
    }
    if filter.application_id.is_none()
        && filter.artifact_id.is_none()
        && filter.actor_path.is_none()
    {
        return true;
    }
    record.applications.iter().any(|application| {
        filter
            .application_id
            .as_ref()
            .is_none_or(|id| application.application_id == *id)
            && filter
                .artifact_id
                .is_none_or(|id| application.artifact_id == id)
            && filter
                .actor_path
                .as_ref()
                .is_none_or(|path| application.actor_path == *path)
    })
}

fn pending_matches(
    msg: &str,
    filter: &kascov_core::store_delivery::DeliveryFilter,
) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(msg) else { return false };
    if filter.covenant_id.is_some_and(|id| {
        value.get("covenant_id").and_then(serde_json::Value::as_str)
            != Some(id.to_string().as_str())
    }) {
        return false;
    }
    if filter.application_id.is_none()
        && filter.artifact_id.is_none()
        && filter.actor_path.is_none()
    {
        return true;
    }
    value
        .pointer("/application/outputs")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|outputs| {
            outputs.iter().any(|output| {
                filter.application_id.as_ref().is_none_or(|id| {
                    output.get("application_id").and_then(serde_json::Value::as_str)
                        == Some(id.as_str())
                }) && filter.artifact_id.is_none_or(|id| {
                    output.get("artifact_id").and_then(serde_json::Value::as_array).is_some_and(
                        |bytes| {
                            bytes.len() == id.len()
                                && bytes.iter().zip(id).all(|(value, expected)| {
                                    value.as_u64() == Some(u64::from(expected))
                                })
                        },
                    )
                }) && filter.actor_path.as_ref().is_none_or(|path| {
                    output.get("actor_path").and_then(serde_json::Value::as_str)
                        == Some(path.as_str())
                })
            })
        })
}

fn pending_sse_event(msg: &str) -> Option<axum::response::sse::Event> {
    (msg.len() <= MAX_STREAM_EVENT_BYTES)
        .then(|| axum::response::sse::Event::default().data(msg))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamStart {
    Ready(kascov_core::StreamCursor),
    Reset {
        reason: &'static str,
        current: kascov_core::StreamCursor,
    },
}

fn select_stream_start(
    headers: &axum::http::HeaderMap,
    params: &std::collections::HashMap<String, String>,
    info: kascov_core::store_delivery::DeliveryStreamInfo,
) -> std::result::Result<StreamStart, &'static str> {
    use kascov_core::store_delivery::DeliveryCursorPosition;

    let raw = match headers.get("last-event-id") {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| "Last-Event-ID must be an opaque <epoch>:<sequence> cursor")?,
        ),
        None => params.get("after").map(String::as_str),
    };
    let cursor = raw
        .map(str::trim)
        .map(str::parse)
        .transpose()
        .map_err(|_| "Last-Event-ID or after must be an opaque <epoch>:<sequence> cursor")?
        .unwrap_or(info.current);
    Ok(match info.classify(cursor) {
        DeliveryCursorPosition::Valid
            if info
                .earliest
                .is_some_and(|earliest| cursor.seq.saturating_add(1) < earliest.seq) =>
        {
            StreamStart::Reset {
                reason: "history_unavailable",
                current: info.current,
            }
        }
        DeliveryCursorPosition::Valid => StreamStart::Ready(cursor),
        DeliveryCursorPosition::ForeignEpoch => StreamStart::Reset {
            reason: "foreign_epoch",
            current: info.current,
        },
        DeliveryCursorPosition::Ahead => StreamStart::Reset {
            reason: "ahead",
            current: info.current,
        },
    })
}

fn apply_stream_headers(response: &mut axum::response::Response) {
    use axum::http::{header, HeaderName, HeaderValue};

    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("last-event-id"),
    );
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("identity"));
}

enum StreamFrame {
    Delivery(std::sync::Arc<kascov_core::DeliveryRecord>),
    Checkpoint(kascov_core::StreamCursor),
    Pending(std::sync::Arc<str>),
}

struct ReplayState {
    cursor: kascov_core::StreamCursor,
    through: kascov_core::StreamCursor,
    last_emitted: kascov_core::StreamCursor,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct DurableStreamState {
    delivery_rx: tokio::sync::broadcast::Receiver<std::sync::Arc<kascov_core::DeliveryRecord>>,
    pending_rx: tokio::sync::broadcast::Receiver<std::sync::Arc<str>>,
    _slot: SubscriberSlot,
    filter: kascov_core::store_delivery::DeliveryFilter,
    performance: std::sync::Arc<kascov_core::performance::PerformanceMetrics>,
    read_pool: crate::read_pool::ReadPool,
    network: Network,
    replay_page_size: u64,
    replay: Option<ReplayState>,
    discard_through: kascov_core::StreamCursor,
    queued: VecDeque<StreamFrame>,
    live_checkpoint: Option<kascov_core::StreamCursor>,
    live_checkpoint_count: u16,
    live_checkpoint_at: tokio::time::Instant,
    deadline: tokio::time::Instant,
}

async fn replay_page(
    read_pool: crate::read_pool::ReadPool,
    after: kascov_core::StreamCursor,
    through: kascov_core::StreamCursor,
    page_size: u64,
) -> kascov_core::Result<Vec<kascov_core::DeliveryRecord>> {
    tokio::task::spawn_blocking(move || {
        read_pool.query(|store| {
            let mut page = store.delivery_page(Some(after), page_size)?;
            page.retain(|record| record.cursor.seq <= through.seq);
            Ok(page)
        })
    })
    .await
    .map_err(|error| kascov_core::Error::Invalid {
        what: "delivery replay task",
        value: error.to_string(),
    })?
    .map_err(|error| kascov_core::Error::Invalid {
        what: "delivery replay read pool",
        value: error.to_string(),
    })
}

async fn next_stream_frame(
    mut state: DurableStreamState,
) -> Option<(StreamFrame, DurableStreamState)> {
    loop {
        if let Some(frame) = state.queued.pop_front() {
            return Some((frame, state));
        }
        if let Some(replay) = state.replay.as_mut() {
            if replay.cursor.seq < replay.through.seq {
                let page = match replay_page(
                    state.read_pool.clone(),
                    replay.cursor,
                    replay.through,
                    state.replay_page_size,
                )
                .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        tracing::warn!("{}: delivery replay failed: {error}", state.network);
                        return None;
                    }
                };
                let Some(scanned_to) = page.last().map(|record| record.cursor) else {
                    tracing::warn!(
                        "{}: delivery replay stopped before high-water {}",
                        state.network,
                        replay.through
                    );
                    return None;
                };
                for record in page {
                    if delivery_matches(&record, &state.filter) {
                        replay.last_emitted = record.cursor;
                        state
                            .queued
                            .push_back(StreamFrame::Delivery(std::sync::Arc::new(record)));
                    }
                }
                replay.cursor = scanned_to;
                if state.filter != Default::default()
                    && scanned_to.seq > replay.last_emitted.seq
                {
                    replay.last_emitted = scanned_to;
                    state.queued.push_back(StreamFrame::Checkpoint(scanned_to));
                }
                continue;
            }
            state.replay = None;
        }

        tokio::select! {
            delivery = state.delivery_rx.recv() => match delivery {
                Ok(record) => {
                    if record.cursor.epoch == state.discard_through.epoch
                        && record.cursor.seq <= state.discard_through.seq
                    {
                        continue;
                    }
                    if delivery_matches(&record, &state.filter) {
                        state.live_checkpoint = None;
                        state.live_checkpoint_count = 0;
                        return Some((StreamFrame::Delivery(record), state));
                    }
                    if state.filter != Default::default() {
                        if state.live_checkpoint.is_none() {
                            state.live_checkpoint_at = tokio::time::Instant::now()
                                + std::time::Duration::from_secs(1);
                        }
                        state.live_checkpoint = Some(record.cursor);
                        state.live_checkpoint_count = state.live_checkpoint_count.saturating_add(1);
                        if state.live_checkpoint_count >= state.replay_page_size as u16 {
                            state.live_checkpoint_count = 0;
                            return Some((
                                StreamFrame::Checkpoint(state.live_checkpoint.take().unwrap()),
                                state,
                            ));
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return None,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            },
            pending = state.pending_rx.recv() => match pending {
                Ok(message) => {
                    if pending_matches(&message, &state.filter) {
                        return Some((StreamFrame::Pending(message), state));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            },
            _ = tokio::time::sleep_until(state.live_checkpoint_at), if state.live_checkpoint.is_some() => {
                state.live_checkpoint_count = 0;
                return Some((
                    StreamFrame::Checkpoint(state.live_checkpoint.take().unwrap()),
                    state,
                ));
            },
            _ = tokio::time::sleep_until(state.deadline) => return None,
        }
    }
}

fn stream_frame_event(
    frame: StreamFrame,
) -> std::result::Result<Option<axum::response::sse::Event>, &'static str> {
    use axum::response::sse::Event;

    match frame {
        StreamFrame::Delivery(record) => {
            let bytes = serde_json::to_vec(&*record).map_err(|_| "delivery JSON encoding failed")?;
            if bytes.len() > MAX_STREAM_EVENT_BYTES {
                return Err("delivery exceeds the stream event byte limit");
            }
            Ok(Event::default()
                .id(record.cursor.to_string())
                .event(match record.kind {
                    kascov_core::DeliveryKind::Accepted => "accepted",
                    kascov_core::DeliveryKind::Removed => "removed",
                    kascov_core::DeliveryKind::ProjectionRepaired => "projection_repaired",
                })
                .json_data(&*record)
                .ok())
        }
        StreamFrame::Checkpoint(cursor) => Ok(Event::default()
            .id(cursor.to_string())
            .event("checkpoint")
            .json_data(serde_json::json!({
                "kind": "checkpoint",
                "cursor": cursor,
            }))
            .ok()),
        StreamFrame::Pending(message) => Ok(pending_sse_event(&message)),
    }
}

fn capacity_response(
    status: axum::http::StatusCode,
    message: &'static str,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    (
        status,
        [(axum::http::header::RETRY_AFTER, "1")],
        message,
    )
        .into_response()
}

/// Replay durable delivery records, then hand off without gaps to the
/// post-commit hub. Pending frames remain best-effort and carry no cursor.
pub(super) async fn stream_handler(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<ServeState>>,
    axum::extract::Path(net_name): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::sse::{Event, KeepAlive, Sse};
    use axum::response::IntoResponse;

    if stream_request_bytes(&params, &headers) > MAX_STREAM_REQUEST_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "stream request exceeds 4096 bytes")
            .into_response();
    }

    let network = match resolve_network(&state, &net_name) {
        Ok(n) => n,
        Err(resp) => return resp,
    };
    let filter = match delivery_filter(&params) {
        Ok(filter) => filter,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
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
    let Some(slot) = delivery_hub.try_subscribe() else {
        return capacity_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "stream capacity exhausted; retry or use the polling feeds",
        );
    };
    // Subscribe before reading high-water. Every commit after the snapshot is
    // then either in the replay range or queued here for the live handoff.
    let delivery_rx = delivery_hub.tx.subscribe();
    let pending_rx = pending_hub.tx.subscribe();
    let read_pool = super::read_pool_for(&state, network);
    let info_pool = read_pool.clone();
    let info = match tokio::task::spawn_blocking(move || {
        info_pool.query(|store| Ok(store.delivery_stream_info()?))
    })
    .await
    {
        Ok(Ok(info)) => info,
        Ok(Err(error)) => {
            tracing::error!("{network}: stream cursor discovery failed: {error}");
            return super::read_unavailable("stream unavailable");
        }
        Err(error) => {
            tracing::error!("{network}: stream cursor task failed: {error}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    let start = match select_stream_start(&headers, &params, info) {
        Ok(start) => start,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15 * 60);
    if let StreamStart::Reset { reason, current } = start {
        let reset = Event::default()
            .retry(std::time::Duration::from_secs(1))
            .event("reset")
            .json_data(serde_json::json!({
                "reason": reason,
                "current_epoch": current.epoch,
                "current": current,
                "snapshot": format!("/data/{network}.json"),
            }))
            .expect("reset JSON is serializable");
        let tail = futures::stream::unfold(slot, move |slot| async move {
            tokio::time::sleep_until(deadline).await;
            drop(slot);
            None::<(
                std::result::Result<Event, std::convert::Infallible>,
                SubscriberSlot,
            )>
        });
        let stream = futures::stream::once(async {
            Ok::<_, std::convert::Infallible>(reset)
        })
        .chain(tail);
        let mut response = Sse::new(stream)
            .keep_alive(
                KeepAlive::new()
                    .interval(std::time::Duration::from_secs(25))
                    .text("ka"),
            )
            .into_response();
        apply_stream_headers(&mut response);
        return response;
    }
    let StreamStart::Ready(after) = start else { unreachable!() };

    let replay = if after.seq < info.current.seq {
        let Some(permit) = delivery_hub.try_replay() else {
            return capacity_response(
                StatusCode::TOO_MANY_REQUESTS,
                "historical replay capacity exhausted; retry",
            );
        };
        Some(ReplayState {
            cursor: after,
            through: info.current,
            last_emitted: after,
            _permit: permit,
        })
    } else {
        None
    };
    let stream_state = DurableStreamState {
        delivery_rx,
        pending_rx,
        _slot: slot,
        filter,
        performance,
        read_pool,
        network,
        replay_page_size: state.capacity.replay_page_size,
        replay,
        discard_through: info.current,
        queued: VecDeque::new(),
        live_checkpoint: None,
        live_checkpoint_count: 0,
        live_checkpoint_at: deadline,
        deadline,
    };
    // The slot rides in state so disconnects free it through Drop. A lagged
    // durable receiver ends the connection; EventSource reconnects and reads
    // the missing range from the log instead of skipping it.
    let stream = futures::stream::unfold(stream_state, |mut state| async move {
        loop {
            let (frame, next_state) = next_stream_frame(state).await?;
            state = next_state;
            let event = {
                let _delivery = state
                    .performance
                    .timer(kascov_core::performance::Stage::StreamDelivery);
                stream_frame_event(frame)
            };
            match event {
                Ok(Some(event)) => {
                    return Some((Ok::<_, std::convert::Infallible>(event), state));
                }
                Ok(None) => {}
                Err(message) => {
                    tracing::warn!("{}: closing stream: {message}", state.network);
                    return None;
                }
            }
        }
    });
    // Lead with an ID-less ready frame. It flushes headers and reports the
    // exact cursor selected by Last-Event-ID, the query, or current high-water.
    let ready = Event::default()
        .retry(std::time::Duration::from_secs(1))
        .event("ready")
        .json_data(serde_json::json!({
            "after": after,
            "current": info.current,
        }))
        .expect("ready JSON is serializable");
    let stream = futures::stream::once(async move {
        Ok::<_, std::convert::Infallible>(ready)
    })
    .chain(stream);

    let mut resp = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(25))
                .text("ka"),
        )
        .into_response();
    apply_stream_headers(&mut resp);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_limits() -> CapacityLimits {
        CapacityLimits {
            max_streams: DEFAULT_MAX_STREAMS,
            max_replays: DEFAULT_MAX_REPLAYS,
            replay_page_size: DEFAULT_REPLAY_PAGE_SIZE,
            event_page_size: DEFAULT_EVENT_PAGE_SIZE,
        }
    }

    #[test]
    fn capacity_limits_reject_zero_and_oversized_replay_pages() {
        assert!(test_limits().validate().is_ok());
        assert!(CapacityLimits { max_streams: 0, ..test_limits() }
            .validate()
            .is_err());
        assert!(CapacityLimits { max_replays: 0, ..test_limits() }
            .validate()
            .is_err());
        assert!(CapacityLimits { replay_page_size: u64::from(u16::MAX) + 1, ..test_limits() }
            .validate()
            .is_err());
        assert!(CapacityLimits { replay_page_size: 300, ..test_limits() }
            .validate()
            .is_err());
        assert!(CapacityLimits { event_page_size: 0, ..test_limits() }
            .validate()
            .is_err());
    }

    #[test]
    fn subscriber_slots_are_bounded_and_released() {
        let hub = DeliveryHub::new(CapacityLimits {
            max_streams: 1,
            ..test_limits()
        });
        let slot = hub.try_subscribe().unwrap();
        assert!(hub.try_subscribe().is_none());
        drop(slot);
        assert!(hub.try_subscribe().is_some());
    }

    #[test]
    fn capacity_health_and_overload_response_expose_contract() {
        let limits = test_limits();
        let hub = DeliveryHub::new(limits);
        let capacity = hub.capacity_json(limits);
        assert_eq!(Some(512), capacity["max_streams"].as_u64());
        assert_eq!(Some(32), capacity["max_historical_replays"].as_u64());
        assert_eq!(Some(512), capacity["replay_page_records"].as_u64());
        assert_eq!(Some(1_000), capacity["event_page_records"].as_u64());

        let response = capacity_response(axum::http::StatusCode::TOO_MANY_REQUESTS, "busy");
        assert_eq!(axum::http::StatusCode::TOO_MANY_REQUESTS, response.status());
        assert_eq!("1", response.headers()[axum::http::header::RETRY_AFTER]);
    }

    #[test]
    fn historical_replays_are_bounded_and_released() {
        let hub = DeliveryHub::new(CapacityLimits {
            max_replays: 1,
            ..test_limits()
        });
        let permit = hub.try_replay().unwrap();
        assert!(hub.try_replay().is_none());
        drop(permit);
        assert!(hub.try_replay().is_some());
    }

    #[test]
    fn stream_request_and_event_bytes_are_bounded() {
        let mut params = std::collections::HashMap::new();
        let headers = axum::http::HeaderMap::new();
        assert!(stream_request_bytes(&params, &headers) < MAX_STREAM_REQUEST_BYTES);
        params.insert("actor".into(), "x".repeat(MAX_STREAM_REQUEST_BYTES));
        assert!(stream_request_bytes(&params, &headers) > MAX_STREAM_REQUEST_BYTES);
        assert!(pending_sse_event(&"x".repeat(MAX_STREAM_EVENT_BYTES)).is_some());
        assert!(pending_sse_event(&"x".repeat(MAX_STREAM_EVENT_BYTES + 1)).is_none());
    }

    #[test]
    fn delivery_filters_parse_and_reject() {
        let mut params = std::collections::HashMap::new();
        assert_eq!(delivery_filter(&params).unwrap(), Default::default());
        let id = "ab".repeat(32);
        params.insert("covenant".into(), id);
        params.insert("application".into(), "duel".into());
        params.insert("artifact".into(), "cd".repeat(32));
        params.insert("actor".into(), "Match.Player".into());
        assert_eq!(
            delivery_filter(&params).unwrap(),
            kascov_core::store_delivery::DeliveryFilter {
                covenant_id: Some(CovenantId([0xab; 32])),
                application_id: Some("duel".into()),
                artifact_id: Some([0xcd; 32]),
                actor_path: Some("Match.Player".into()),
            }
        );
        params.insert("covenant".into(), "abcd".into());
        assert!(delivery_filter(&params).is_err());
        params.insert("covenant".into(), "ab".repeat(32));
        params.insert("artifact".into(), "zz".repeat(32));
        assert!(delivery_filter(&params).is_err());
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

        let filter = |covenant_id| kascov_core::store_delivery::DeliveryFilter {
            covenant_id,
            ..Default::default()
        };
        assert!(delivery_matches(&delivery, &filter(Some(id))));
        assert!(!delivery_matches(&delivery, &filter(Some(other))));
        assert!(pending_matches(&pending, &filter(Some(id))));
        assert!(!pending_matches(&pending, &filter(Some(other))));
        assert!(delivery_matches(&delivery, &filter(None)));
        assert!(pending_matches(&pending, &filter(None)));
    }

    #[tokio::test]
    async fn pending_frames_never_emit_an_eventsource_id() {
        use axum::response::IntoResponse;
        let stream = futures::stream::once(async {
            Ok::<_, std::convert::Infallible>(
                pending_sse_event(r#"{"kind":"pending"}"#).unwrap(),
            )
        });
        let response = axum::response::sse::Sse::new(stream).into_response();
        let bytes = axum::body::to_bytes(response.into_body(), 1_024)
            .await
            .unwrap();
        let wire = std::str::from_utf8(&bytes).unwrap();
        assert!(wire.contains("data: {\"kind\":\"pending\"}"));
        assert!(!wire.lines().any(|line| line.starts_with("id:")));
    }

    fn accepted_batch(block: u8, events: u32) -> kascov_core::store::AcceptedBlockBatch {
        use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent};

        let mut batch = AcceptedBlockBatch::empty(BlockHash([block; 32]));
        batch.accepting_daa = u64::from(block) * 100;
        batch.accepting_blue_score = u64::from(block) * 100;
        batch.events = (0..events)
            .map(|event_index| NewEvent {
                covenant_id: CovenantId([block; 32]),
                kind: EventKind::Transition,
                txid: TxId([block.saturating_add(10); 32]),
                tx_index: 0,
                event_index,
                payload: None,
                lane_namespace: None,
            })
            .collect();
        batch
    }

    fn replay_state(
        path: std::path::PathBuf,
        after: kascov_core::StreamCursor,
        through: kascov_core::StreamCursor,
        delivery_rx: tokio::sync::broadcast::Receiver<std::sync::Arc<kascov_core::DeliveryRecord>>,
        pending_rx: tokio::sync::broadcast::Receiver<std::sync::Arc<str>>,
        filter: kascov_core::store_delivery::DeliveryFilter,
    ) -> DurableStreamState {
        DurableStreamState {
            delivery_rx,
            pending_rx,
            _slot: SubscriberSlot(std::sync::Arc::new(
                std::sync::atomic::AtomicUsize::new(1),
            )),
            filter,
            performance: std::sync::Arc::new(
                kascov_core::performance::PerformanceMetrics::new(),
            ),
            read_pool: crate::read_pool::ReadPool::new(&path, Network::Testnet(10)),
            network: Network::Testnet(10),
            replay_page_size: DEFAULT_REPLAY_PAGE_SIZE,
            replay: Some(ReplayState {
                cursor: after,
                through,
                last_emitted: after,
                _permit: std::sync::Arc::new(tokio::sync::Semaphore::new(1))
                    .try_acquire_owned()
                    .unwrap(),
            }),
            discard_through: through,
            queued: VecDeque::new(),
            live_checkpoint: None,
            live_checkpoint_count: 0,
            live_checkpoint_at: tokio::time::Instant::now()
                + std::time::Duration::from_secs(5),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn commits_during_replay_and_at_handoff_are_delivered_once() {
        let path = std::env::temp_dir().join(format!(
            "kascov-stream-race-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        store.apply_accepted_block(&accepted_batch(1, 513)).unwrap();
        let through = store.delivery_high_water().unwrap();
        let after = kascov_core::StreamCursor {
            epoch: through.epoch,
            seq: 0,
        };
        let (delivery_tx, delivery_rx) = tokio::sync::broadcast::channel(16);
        let (pending_tx, pending_rx) = tokio::sync::broadcast::channel(16);
        let mut state = replay_state(
            path.clone(),
            after,
            through,
            delivery_rx,
            pending_rx,
            Default::default(),
        );

        let (first, next) = next_stream_frame(state).await.unwrap();
        state = next;
        assert!(matches!(first, StreamFrame::Delivery(ref record) if record.cursor.seq == 1));

        let committed = store.apply_accepted_block(&accepted_batch(2, 1)).unwrap();
        delivery_tx
            .send(std::sync::Arc::new(committed.deliveries[0].clone()))
            .unwrap();
        let mut sequences = vec![1];
        while sequences.len() < 514 {
            let (frame, next) = next_stream_frame(state).await.unwrap();
            state = next;
            if let StreamFrame::Delivery(record) = frame {
                sequences.push(record.cursor.seq);
            }
        }
        assert_eq!((1..=514).collect::<Vec<_>>(), sequences);
        drop(pending_tx);
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn filtered_replay_checkpoints_each_scanned_page() {
        let path = std::env::temp_dir().join(format!(
            "kascov-stream-checkpoint-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        store.apply_accepted_block(&accepted_batch(1, 600)).unwrap();
        let through = store.delivery_high_water().unwrap();
        let after = kascov_core::StreamCursor {
            epoch: through.epoch,
            seq: 0,
        };
        let (_delivery_tx, delivery_rx) = tokio::sync::broadcast::channel(16);
        let (_pending_tx, pending_rx) = tokio::sync::broadcast::channel(16);
        let mut state = replay_state(
            path.clone(),
            after,
            through,
            delivery_rx,
            pending_rx,
            kascov_core::store_delivery::DeliveryFilter {
                covenant_id: Some(CovenantId([9; 32])),
                ..Default::default()
            },
        );

        for expected in [512, 600] {
            let (frame, next) = next_stream_frame(state).await.unwrap();
            state = next;
            assert!(matches!(frame, StreamFrame::Checkpoint(cursor) if cursor.seq == expected));
        }
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_lagged_durable_subscriber_closes_instead_of_skipping() {
        let path = std::env::temp_dir().join(format!(
            "kascov-stream-lag-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();
        let current = store.delivery_high_water().unwrap();
        let (delivery_tx, delivery_rx) = tokio::sync::broadcast::channel(2);
        let (_pending_tx, pending_rx) = tokio::sync::broadcast::channel(2);
        for seq in 1..=3 {
            let record = kascov_core::DeliveryRecord {
                cursor: kascov_core::StreamCursor {
                    epoch: current.epoch,
                    seq,
                },
                kind: kascov_core::DeliveryKind::Accepted,
                source_cursor: None,
                covenant_id: CovenantId([1; 32]),
                covenant_event_seq: seq,
                txid: TxId([2; 32]),
                accepting_block: BlockHash([3; 32]),
                accepting_daa: seq,
                tx_index: Some(0),
                event_index: Some(0),
                order_complete: true,
                pending_id: None,
                applications: vec![],
            };
            delivery_tx.send(std::sync::Arc::new(record)).unwrap();
        }
        let mut state = replay_state(
            path.clone(),
            current,
            current,
            delivery_rx,
            pending_rx,
            Default::default(),
        );
        state.replay = None;
        assert!(next_stream_frame(state).await.is_none());
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(start_paused = true)]
    async fn filtered_live_stream_checkpoints_unmatched_progress() {
        let path = std::env::temp_dir().join(format!(
            "kascov-stream-live-checkpoint-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();
        let current = store.delivery_high_water().unwrap();
        let (delivery_tx, delivery_rx) = tokio::sync::broadcast::channel(2);
        let (_pending_tx, pending_rx) = tokio::sync::broadcast::channel(2);
        delivery_tx
            .send(std::sync::Arc::new(kascov_core::DeliveryRecord {
                cursor: kascov_core::StreamCursor {
                    epoch: current.epoch,
                    seq: 1,
                },
                kind: kascov_core::DeliveryKind::Accepted,
                source_cursor: None,
                covenant_id: CovenantId([1; 32]),
                covenant_event_seq: 1,
                txid: TxId([2; 32]),
                accepting_block: BlockHash([3; 32]),
                accepting_daa: 1,
                tx_index: Some(0),
                event_index: Some(0),
                order_complete: true,
                pending_id: None,
                applications: vec![],
            }))
            .unwrap();
        let mut state = replay_state(
            path.clone(),
            current,
            current,
            delivery_rx,
            pending_rx,
            kascov_core::store_delivery::DeliveryFilter {
                covenant_id: Some(CovenantId([9; 32])),
                ..Default::default()
            },
        );
        state.replay = None;
        let (frame, _) = next_stream_frame(state).await.unwrap();
        assert!(matches!(frame, StreamFrame::Checkpoint(cursor) if cursor.seq == 1));
        drop(store);
        let _ = std::fs::remove_file(path);
    }
}
