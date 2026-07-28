use super::*;

/* --------------------------------------------- live pending (mempool) feed */

/// A millisecond tunable read from the environment, mirroring KASCOV_RPC_*:
/// a plain integer, falling back to `default` on absent or garbage input.
fn env_ms(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// The most a network's pending set may hold. A safety cap only — a healthy
/// covenant mempool is tiny; past this we stop tracking new entries until the
/// pool drains. The confirmed pipeline is never affected.
const MAX_PENDING: usize = 512;
/// A 250ms poller that has not succeeded for this long is stale even if the
/// task is hung before it can explicitly transition to reconnecting.
const PENDING_HEALTH_STALE_MS: u64 = 5_000;

/// One covenant event touched by a pending transaction. Kept sorted by
/// covenant id inside [`PendingEntry`] so both the legacy scalar fields and
/// the additive `events` array have a stable meaning across processes.
#[derive(Clone, Copy)]
struct PendingEvent {
    covenant_id: CovenantId,
    kind: kascov_core::store::EventKind,
    ordinal: u32,
}

/// One pending transaction we're tracking between "seen in mempool" and
/// "resolved" (confirmed or dropped).
struct PendingEntry {
    events: Vec<PendingEvent>,
    inputs: Vec<kascov_core::Outpoint>,
    application: kascov_core::ApplicationPreprocess,
    first_seen: std::time::Instant,
    first_seen_ms: u64,
    /// Set the first poll a tracked txid is gone from the pool; the drop-grace
    /// timer runs from here. A mined tx leaves the pool before the follower has
    /// indexed its events, so we hold briefly before declaring it dropped.
    leaving_since: Option<std::time::Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingFeedStatus {
    Starting,
    Live,
    Reconnecting,
    Disabled,
}

impl PendingFeedStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Live => "live",
            Self::Reconnecting => "reconnecting",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingInsert {
    Added,
    AlreadyTracked,
    Overflow,
}

impl PendingInsert {
    fn tracked(self) -> bool {
        !matches!(self, Self::Overflow)
    }
}

/// A network's authoritative pending feed: tx rows, insertion order, and
/// poller liveness metadata share one lock so `/pending` can never combine
/// rows from one revision with health from another.
pub(super) struct PendingFeed {
    entries: std::collections::HashMap<TxId, PendingEntry>,
    order: VecDeque<TxId>,
    status: PendingFeedStatus,
    last_poll_ms: Option<u64>,
    revision: u64,
}

impl PendingFeed {
    pub(super) fn new() -> Self {
        Self {
            entries: Default::default(),
            order: Default::default(),
            status: PendingFeedStatus::Starting,
            last_poll_ms: None,
            revision: 0,
        }
    }

    fn set_status(&mut self, status: PendingFeedStatus) {
        if self.status != status {
            self.status = status;
            self.revision = self.revision.wrapping_add(1);
        }
    }

    fn mark_live_at(&mut self, at_ms: u64) {
        self.last_poll_ms = Some(at_ms);
        self.set_status(PendingFeedStatus::Live);
    }

    fn mark_reconnecting(&mut self) {
        self.set_status(PendingFeedStatus::Reconnecting);
    }

    fn mark_disabled(&mut self) {
        self.set_status(PendingFeedStatus::Disabled);
        if !self.entries.is_empty() {
            self.entries.clear();
            self.order.clear();
            self.revision = self.revision.wrapping_add(1);
        }
    }

    fn status_at(&self, at_ms: u64) -> &'static str {
        if self.status == PendingFeedStatus::Live
            && self
                .last_poll_ms
                .is_some_and(|last| at_ms.saturating_sub(last) > PENDING_HEALTH_STALE_MS)
        {
            "stale"
        } else {
            self.status.as_str()
        }
    }

    #[cfg(test)]
    fn insert_at_ms(
        &mut self,
        txid: TxId,
        covenant_id: CovenantId,
        kind: kascov_core::store::EventKind,
        at_ms: u64,
    ) -> PendingInsert {
        self.insert_at(
            txid,
            covenant_id,
            kind,
            0,
            at_ms,
            std::time::Instant::now(),
            vec![],
            kascov_core::ApplicationPreprocess::default(),
        )
    }

    fn insert_at(
        &mut self,
        txid: TxId,
        covenant_id: CovenantId,
        kind: kascov_core::store::EventKind,
        ordinal: u32,
        at_ms: u64,
        at: std::time::Instant,
        inputs: Vec<kascov_core::Outpoint>,
        application: kascov_core::ApplicationPreprocess,
    ) -> PendingInsert {
        if let Some(entry) = self.entries.get_mut(&txid) {
            if let Some(existing) = entry
                .events
                .iter_mut()
                .find(|event| event.covenant_id == covenant_id)
            {
                if existing.kind == kind {
                    return PendingInsert::AlreadyTracked;
                }
                existing.kind = kind;
            } else {
                entry.events.push(PendingEvent {
                    covenant_id,
                    kind,
                    ordinal,
                });
            }
            entry.events.sort_by_key(|event| event.covenant_id.0);
            self.revision = self.revision.wrapping_add(1);
            return PendingInsert::Added;
        }
        if self.entries.len() >= MAX_PENDING {
            return PendingInsert::Overflow;
        }
        self.entries.insert(
            txid,
            PendingEntry {
                events: vec![PendingEvent {
                    covenant_id,
                    kind,
                    ordinal,
                }],
                inputs,
                application,
                first_seen: at,
                first_seen_ms: at_ms,
                leaving_since: None,
            },
        );
        self.order.push_back(txid);
        self.revision = self.revision.wrapping_add(1);
        PendingInsert::Added
    }

    fn remove(&mut self, txid: &TxId) -> Option<PendingEntry> {
        let removed = self.entries.remove(txid);
        if removed.is_some() {
            self.order.retain(|t| t != txid);
            self.revision = self.revision.wrapping_add(1);
        }
        removed
    }

    fn snapshot_json_at(&self, generated_at_ms: u64) -> serde_json::Value {
        let rows: Vec<_> = self
            .order
            .iter()
            .filter_map(|txid| self.entries.get(txid).map(|entry| (txid, entry)))
            .filter_map(|(txid, entry)| {
                let primary = entry.events.first()?;
                Some(serde_json::json!({
                    // Backward-compatible scalar preview: the stable first
                    // event. New clients should render the full array.
                    "covenant_id": primary.covenant_id,
                    "tx_kind": primary.kind.as_str(),
                    "txid": txid,
                    "age_ms": generated_at_ms.saturating_sub(entry.first_seen_ms),
                    "events": pending_events_json(txid, entry),
                    "application": pending_application_json(entry),
                }))
            })
            .collect();
        serde_json::json!({
            "status": self.status_at(generated_at_ms),
            "last_poll_ms": self.last_poll_ms,
            "generated_at_ms": generated_at_ms,
            "revision": self.revision,
            "pending": rows,
        })
    }

    pub(super) fn health_json_at(&self, generated_at_ms: u64) -> serde_json::Value {
        serde_json::json!({
            "status": self.status_at(generated_at_ms),
            "last_poll_ms": self.last_poll_ms,
            "revision": self.revision,
            "pending": self.entries.len(),
        })
    }
}

/// Atomically admit every covenant event for one mempool transaction. The
/// capacity gate is checked once at tx granularity: callers can safely decide
/// whether an SSE hint will have an authoritative row that can later resolve.
fn track_pending_transaction(
    feed: &mut PendingFeed,
    tx: &kascov_core::Transaction,
    events: Vec<kascov_core::sync::PendingTxEvent>,
    application: kascov_core::ApplicationPreprocess,
) -> PendingInsert {
    track_pending_transaction_at(
        feed,
        tx,
        events,
        application,
        now_ms(),
        std::time::Instant::now(),
    )
}

fn track_pending_transaction_at(
    feed: &mut PendingFeed,
    tx: &kascov_core::Transaction,
    mut events: Vec<kascov_core::sync::PendingTxEvent>,
    application: kascov_core::ApplicationPreprocess,
    at_ms: u64,
    at: std::time::Instant,
) -> PendingInsert {
    if !feed.entries.contains_key(&tx.txid) && feed.entries.len() >= MAX_PENDING {
        return PendingInsert::Overflow;
    }
    events.sort_by_key(|event| event.covenant_id.0);
    let inputs: Vec<_> = tx
        .inputs
        .iter()
        .map(|input| input.previous_outpoint)
        .collect();
    let mut result = PendingInsert::AlreadyTracked;
    for (ordinal, event) in events.into_iter().enumerate() {
        if feed.insert_at(
            tx.txid,
            event.covenant_id,
            event.kind,
            ordinal as u32,
            at_ms,
            at,
            inputs.clone(),
            application.clone(),
        ) == PendingInsert::Added
        {
            result = PendingInsert::Added;
        }
    }
    result
}

#[cfg(test)]
fn track_pending_transaction_at_ms(
    feed: &mut PendingFeed,
    txid: TxId,
    events: Vec<kascov_core::sync::PendingTxEvent>,
    at_ms: u64,
) -> PendingInsert {
    let tx = kascov_core::Transaction {
        txid,
        version: 1,
        inputs: vec![],
        outputs: vec![],
        payload: vec![],
    };
    track_pending_transaction_at(
        feed,
        &tx,
        events,
        kascov_core::ApplicationPreprocess::default(),
        at_ms,
        std::time::Instant::now(),
    )
}

/// Only successfully classified/admitted txids enter the next poll's seen set.
/// Failed classifications and capacity overflows remain "new", so a transient
/// error cannot hide a tx for the rest of its mempool lifetime.
fn pending_ids_to_remember(mut current: HashSet<TxId>, retry: &HashSet<TxId>) -> HashSet<TxId> {
    current.retain(|txid| !retry.contains(txid));
    current
}

fn pending_events_json(txid: &TxId, entry: &PendingEntry) -> Vec<serde_json::Value> {
    entry
        .events
        .iter()
        .map(|event| {
            serde_json::json!({
                "covenant_id": event.covenant_id,
                "tx_kind": event.kind.as_str(),
                "pending_id": kascov_core::pending_event_id(
                    *txid,
                    event.covenant_id,
                    event.ordinal,
                ),
            })
        })
        .collect()
}

fn pending_application_json(entry: &PendingEntry) -> serde_json::Value {
    let status = if entry.application.raw_envelope.is_none() {
        "absent"
    } else if entry.application.failures.is_empty() {
        "valid"
    } else {
        "invalid"
    };
    serde_json::json!({
        "status": status,
        "outputs": entry.application.outputs,
        "failures": entry.application.failures,
    })
}

/// Deterministic pending hints. Existing consumers still receive one message
/// per covenant; every message additionally carries the complete tx-level
/// event set. Reverse order makes a legacy txid-keyed map finish on the same
/// stable primary event exposed by the snapshot's scalar fields.
fn pending_sse_jsons(feed: &PendingFeed, txid: &TxId) -> Vec<serde_json::Value> {
    let Some(entry) = feed.entries.get(txid) else {
        return vec![];
    };
    let events = pending_events_json(txid, entry);
    entry
        .events
        .iter()
        .rev()
        .map(|event| {
            serde_json::json!({
                "kind": "pending",
                "pending_id": kascov_core::pending_event_id(
                    *txid,
                    event.covenant_id,
                    event.ordinal,
                ),
                "covenant_id": event.covenant_id,
                "tx_kind": event.kind.as_str(),
                "txid": txid,
                "events": events.clone(),
                "application": pending_application_json(entry),
                "revision": feed.revision,
            })
        })
        .collect()
}

fn pending_resolved_sse_json(
    txid: &TxId,
    entry: &PendingEntry,
    resolution: &'static str,
    replaced_by: Option<TxId>,
    revision: u64,
) -> Option<serde_json::Value> {
    let primary = entry.events.first()?;
    let mut value = serde_json::json!({
        "kind": "pending_resolved",
        "covenant_id": primary.covenant_id,
        "txid": txid,
        "resolution": resolution,
        "events": pending_events_json(txid, entry),
        "application": pending_application_json(entry),
        "revision": revision,
    });
    if let Some(replaced_by) = replaced_by {
        value["replaced_by"] = serde_json::json!(replaced_by);
    }
    Some(value)
}

fn replacement_for(
    txid: TxId,
    entry: &PendingEntry,
    current_spenders: &std::collections::HashMap<kascov_core::Outpoint, TxId>,
) -> Option<TxId> {
    entry
        .inputs
        .iter()
        .filter_map(|outpoint| current_spenders.get(outpoint))
        .copied()
        .find(|replacement| replacement != &txid)
}

pub(super) async fn resolve_accepted_pending(
    pending: &std::sync::Arc<tokio::sync::Mutex<PendingFeed>>,
    live_tx: &tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
    accepted_txids: &HashSet<TxId>,
) -> usize {
    let mut resolved = Vec::new();
    {
        let mut feed = pending.lock().await;
        for txid in accepted_txids {
            if let Some(entry) = feed.remove(txid) {
                resolved.push((*txid, entry, feed.revision));
            }
        }
    }
    let count = resolved.len();
    if live_tx.receiver_count() > 0 {
        for (txid, entry, revision) in resolved {
            if let Some(message) =
                pending_resolved_sse_json(&txid, &entry, "accepted", None, revision)
            {
                let _ = live_tx.send(message.to_string().into());
            }
        }
    }
    count
}

/// A node without mempool RPC answers get_mempool_entries as an unsupported
/// method — a permanent condition (disable the feed), distinct from a
/// transient transport drop (reconnect). The wRPC layer surfaces both as error
/// strings, so match on the method-level signals; anything else is treated as
/// transient (retrying costs only a log line, and get_mempool_entries is a
/// standard method, so this branch is a defensive guard).
fn mempool_unsupported(err: &kascov_core::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("method not found")
        || msg.contains("not implemented")
        || msg.contains("unimplemented")
        || msg.contains("unsupported")
        || msg.contains("not supported")
        || msg.contains("no such method")
}

/// Poll a network's mempool forever, diff it against the last poll, and fan
/// pending covenant events out on the shared broadcast channel. Reconnects on
/// transient failure; disables itself (returns) if the node has no mempool RPC.
pub(super) async fn poll_mempool_forever(
    network: Network,
    rpc: Option<String>,
    db: std::path::PathBuf,
    live_tx: tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
    pending: std::sync::Arc<tokio::sync::Mutex<PendingFeed>>,
    decoder: std::sync::Arc<dyn kascov_core::ApplicationDecoder>,
) {
    // Kill-switch: KASCOV_MEMPOOL=off disables the feed for every network.
    if std::env::var("KASCOV_MEMPOOL")
        .map(|v| v.trim().eq_ignore_ascii_case("off"))
        .unwrap_or(false)
    {
        pending.lock().await.mark_disabled();
        tracing::info!("{network}: pending mempool feed disabled (KASCOV_MEMPOOL=off)");
        return;
    }
    // Same per-network node override the follower honors, so the poller reads
    // the very node that will confirm these txs.
    let env_key = format!(
        "KASCOV_RPC_{}",
        network.to_string().to_uppercase().replace('-', "_")
    );
    let rpc = match std::env::var(&env_key) {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => rpc,
    };
    // Keep the pending lane responsive without accepting unbounded operator
    // input. The accepted follower remains independent of this poller.
    let poll =
        std::time::Duration::from_millis(env_ms("KASCOV_MEMPOOL_POLL_MS", 100).clamp(25, 1_000));
    let grace = std::time::Duration::from_millis(env_ms("KASCOV_MEMPOOL_DROP_GRACE_MS", 8000));
    let max_age = std::time::Duration::from_millis(env_ms("KASCOV_MEMPOOL_MAX_AGE_MS", 600_000));

    let mut prev_ids: HashSet<TxId> = HashSet::new();
    loop {
        // Our OWN Store connection — a concurrent WAL reader in this loop,
        // never the follower's &mut. Open failure is transient: retry.
        let store = match Store::open_read_only(&db, network) {
            Ok(store) => store,
            Err(err) => {
                pending.lock().await.mark_reconnecting();
                tracing::warn!(
                    "{network}: pending poller cannot open store ({err}), retrying in 30s"
                );
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        };
        let node = match NodeHandle::connect(network, rpc.as_deref()).await {
            Ok(node) => node,
            Err(err) => {
                pending.lock().await.mark_reconnecting();
                tracing::warn!("{network}: pending poller connect failed ({err}), retrying in 10s");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };
        tracing::info!("{network}: pending mempool feed live");
        loop {
            let txs = match node.mempool_txs().await {
                Ok(txs) => txs,
                Err(err) => {
                    if mempool_unsupported(&err) {
                        pending.lock().await.mark_disabled();
                        tracing::warn!(
                            "{network}: node has no get_mempool_entries ({err}) — pending feed disabled"
                        );
                        return;
                    }
                    pending.lock().await.mark_reconnecting();
                    tracing::warn!("{network}: pending poll failed ({err}), reconnecting");
                    break;
                }
            };
            let cur_ids: HashSet<TxId> = txs.iter().map(|tx| tx.txid).collect();
            let current_spenders: std::collections::HashMap<kascov_core::Outpoint, TxId> = txs
                .iter()
                .flat_map(|tx| {
                    tx.inputs
                        .iter()
                        .map(move |input| (input.previous_outpoint, tx.txid))
                })
                .collect();
            let now = std::time::Instant::now();
            pending.lock().await.mark_live_at(now_ms());
            let mut retry_ids = HashSet::new();

            // NEW txids only: classify each and surface any covenant events.
            // Diffing keeps the work bound to churn, not pool size.
            for tx in &txs {
                if prev_ids.contains(&tx.txid) {
                    continue;
                }
                let events = match kascov_core::sync::classify_pending(&store, tx) {
                    Ok(events) => events,
                    Err(err) => {
                        retry_ids.insert(tx.txid);
                        tracing::debug!(
                            "{network}: pending classify failed for {}: {err}",
                            tx.txid
                        );
                        continue;
                    }
                };
                if events.is_empty() {
                    continue;
                }
                let application = decoder.preprocess(tx);
                let (admission, messages) = {
                    let mut feed = pending.lock().await;
                    let admission = track_pending_transaction(&mut feed, tx, events, application);
                    let messages = if admission.tracked() {
                        pending_sse_jsons(&feed, &tx.txid)
                            .into_iter()
                            .map(|value| value.to_string())
                            .collect()
                    } else {
                        vec![]
                    };
                    (admission, messages)
                };
                if admission == PendingInsert::Overflow {
                    // Do not publish a hint the authoritative snapshot cannot
                    // track and resolve. Leaving it out of prev_ids retries it
                    // as soon as another row frees capacity.
                    retry_ids.insert(tx.txid);
                    tracing::debug!(
                        "{network}: pending set full; retrying {} next poll",
                        tx.txid
                    );
                    continue;
                }
                if live_tx.receiver_count() > 0 {
                    for msg in messages {
                        let _ = live_tx.send(msg.into());
                    }
                }
            }

            // Resolve tracked txids that LEFT the pool, and age out stale ones.
            // Collect broadcasts under the lock; send after releasing it.
            let mut resolved: Vec<(TxId, PendingEntry, &'static str, Option<TxId>, u64)> = vec![];
            {
                let mut set = pending.lock().await;
                let tracked: Vec<TxId> = set.order.iter().copied().collect();
                for txid in tracked {
                    let gone = !cur_ids.contains(&txid);
                    let (first_seen, leaving_since) = match set.entries.get(&txid) {
                        Some(e) => (e.first_seen, e.leaving_since),
                        None => continue,
                    };
                    // Age-out: a stuck entry the pool keeps re-serving (or a
                    // resolution we somehow missed) is dropped after max_age.
                    if now.duration_since(first_seen) >= max_age {
                        if let Some(entry) = set.remove(&txid) {
                            resolved.push((txid, entry, "dropped", None, set.revision));
                        }
                        continue;
                    }
                    if !gone {
                        // Still in the pool: clear any leaving timer (a tx that
                        // re-entered on a reorg simply keeps waiting).
                        if let Some(e) = set.entries.get_mut(&txid) {
                            e.leaving_since = None;
                        }
                        continue;
                    }
                    // Gone from the pool: did the follower index its events?
                    let confirmed = store
                        .events_by_txid(&txid)
                        .map(|r| !r.is_empty())
                        .unwrap_or(false);
                    if confirmed {
                        if let Some(entry) = set.remove(&txid) {
                            resolved.push((txid, entry, "accepted", None, set.revision));
                        }
                        continue;
                    }
                    let replaced_by = set
                        .entries
                        .get(&txid)
                        .and_then(|entry| replacement_for(txid, entry, &current_spenders));
                    if let Some(replaced_by) = replaced_by {
                        if let Some(entry) = set.remove(&txid) {
                            resolved.push((
                                txid,
                                entry,
                                "replaced",
                                Some(replaced_by),
                                set.revision,
                            ));
                        }
                        continue;
                    }
                    // Not yet indexed: hold for the grace window (mined-but-
                    // follower-behind race) before calling it dropped.
                    let since = match leaving_since {
                        Some(t) => t,
                        None => {
                            if let Some(e) = set.entries.get_mut(&txid) {
                                e.leaving_since = Some(now);
                            }
                            now
                        }
                    };
                    if now.duration_since(since) >= grace {
                        if let Some(entry) = set.remove(&txid) {
                            resolved.push((txid, entry, "dropped", None, set.revision));
                        }
                    }
                }
            }
            for (txid, entry, resolution, replaced_by, revision) in resolved {
                if live_tx.receiver_count() > 0 {
                    if let Some(msg) =
                        pending_resolved_sse_json(&txid, &entry, resolution, replaced_by, revision)
                    {
                        let _ = live_tx.send(msg.to_string().into());
                    }
                }
            }

            prev_ids = pending_ids_to_remember(cur_ids, &retry_ids);
            tokio::time::sleep(poll).await;
        }
        // Reconnect: reset the diff so the fresh session re-surfaces the pool.
        prev_ids.clear();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Snapshot of a network's live pending covenant txs and poller health
/// (in-memory, lock-guarded). `no-store` — memory-derived and changing every
/// poll, so it must never be cached. Legacy `pending` rows and their scalar
/// event fields remain; health/revision and per-tx `events` are additive.
pub(super) async fn pending_handler(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<ServeState>>,
    axum::extract::Path(net_name): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let network = match resolve_network(&state, &net_name) {
        Ok(n) => n,
        Err(resp) => return resp,
    };
    let Some((_, set)) = state.pending.iter().find(|(n, _)| *n == network) else {
        return (StatusCode::NOT_FOUND, "pending feed unavailable").into_response();
    };
    let body = set.lock().await.snapshot_json_at(now_ms());
    json_resp(body)
}

#[cfg(test)]
mod pending_feed_tests {
    use super::*;
    use kascov_core::store::EventKind;
    use kascov_core::sync::PendingTxEvent;

    #[test]
    fn pending_snapshot_is_deterministic_and_backward_compatible() {
        let mut feed = PendingFeed::new();
        feed.mark_live_at(1_000);
        let txid = TxId([0xaa; 32]);

        // Insert out of order: the wire shape must not depend on HashMap
        // iteration or classifier event order.
        assert_eq!(
            feed.insert_at_ms(txid, CovenantId([0x22; 32]), EventKind::Burn, 1_000),
            PendingInsert::Added
        );
        assert_eq!(
            feed.insert_at_ms(txid, CovenantId([0x11; 32]), EventKind::Transition, 1_000,),
            PendingInsert::Added
        );

        let body = feed.snapshot_json_at(1_250);
        assert_eq!(body["status"], "live");
        assert_eq!(body["last_poll_ms"], 1_000);
        assert_eq!(body["generated_at_ms"], 1_250);
        assert_eq!(body["revision"], 3);

        let row = &body["pending"][0];
        assert_eq!(row["txid"], txid.to_string());
        assert_eq!(row["age_ms"], 250);
        // Legacy clients keep reading these scalar fields.
        assert_eq!(row["covenant_id"], CovenantId([0x11; 32]).to_string());
        assert_eq!(row["tx_kind"], "transition");
        // New clients get every touched covenant in stable id order.
        assert_eq!(
            row["events"][0]["covenant_id"],
            CovenantId([0x11; 32]).to_string()
        );
        assert_eq!(row["events"][0]["tx_kind"], "transition");
        assert_eq!(
            row["events"][1]["covenant_id"],
            CovenantId([0x22; 32]).to_string()
        );
        assert_eq!(row["events"][1]["tx_kind"], "burn");
    }

    #[test]
    fn pending_capacity_rejects_only_new_transactions() {
        let mut feed = PendingFeed::new();
        let txid = |n: usize| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
            TxId(bytes)
        };
        for n in 0..MAX_PENDING {
            assert_eq!(
                track_pending_transaction_at_ms(
                    &mut feed,
                    txid(n),
                    vec![PendingTxEvent {
                        covenant_id: CovenantId([(n % 255) as u8; 32]),
                        kind: EventKind::Transition,
                    }],
                    1_000,
                ),
                PendingInsert::Added
            );
        }
        let full_revision = feed.revision;

        // An already-accepted tx may still reveal another touched covenant.
        assert_eq!(
            track_pending_transaction_at_ms(
                &mut feed,
                txid(0),
                vec![PendingTxEvent {
                    covenant_id: CovenantId([0xfe; 32]),
                    kind: EventKind::Burn,
                }],
                1_001,
            ),
            PendingInsert::Added
        );
        assert_eq!(feed.entries[&txid(0)].events.len(), 2);

        // A genuinely new tx is rejected and must not mutate the revision.
        let before_overflow = feed.revision;
        assert_eq!(
            track_pending_transaction_at_ms(
                &mut feed,
                txid(MAX_PENDING),
                vec![PendingTxEvent {
                    covenant_id: CovenantId([0xff; 32]),
                    kind: EventKind::Genesis,
                }],
                1_002,
            ),
            PendingInsert::Overflow
        );
        assert_eq!(feed.revision, before_overflow);
        assert!(feed.revision > full_revision);
    }

    #[test]
    fn pending_health_distinguishes_live_reconnecting_and_disabled() {
        let mut feed = PendingFeed::new();
        assert_eq!(feed.health_json_at(1_000)["status"], "starting");
        assert_eq!(
            feed.health_json_at(1_000)["last_poll_ms"],
            serde_json::Value::Null
        );

        feed.mark_live_at(2_000);
        assert_eq!(
            feed.insert_at_ms(
                TxId([0x44; 32]),
                CovenantId([0x55; 32]),
                EventKind::Genesis,
                2_000,
            ),
            PendingInsert::Added
        );
        assert_eq!(feed.health_json_at(2_100)["status"], "live");
        assert_eq!(feed.health_json_at(2_100)["last_poll_ms"], 2_000);
        assert_eq!(feed.health_json_at(2_100)["pending"], 1);
        assert_eq!(
            feed.health_json_at(2_000 + PENDING_HEALTH_STALE_MS + 1)["status"],
            "stale"
        );

        feed.mark_reconnecting();
        let reconnecting = feed.health_json_at(9_000);
        assert_eq!(reconnecting["status"], "reconnecting");
        // The last successful poll stays visible while reconnecting.
        assert_eq!(reconnecting["last_poll_ms"], 2_000);

        feed.mark_disabled();
        assert_eq!(feed.health_json_at(9_000)["status"], "disabled");
        assert_eq!(feed.health_json_at(9_000)["pending"], 0);
    }

    #[test]
    fn failed_or_overflowed_transactions_are_retried_next_poll() {
        let good = TxId([0x10; 32]);
        let classify_failed = TxId([0x20; 32]);
        let overflowed = TxId([0x30; 32]);
        let current = HashSet::from([good, classify_failed, overflowed]);
        let retry = HashSet::from([classify_failed, overflowed]);

        let remembered = pending_ids_to_remember(current, &retry);
        assert!(remembered.contains(&good));
        assert!(!remembered.contains(&classify_failed));
        assert!(!remembered.contains(&overflowed));
    }

    #[test]
    fn pending_sse_keeps_legacy_fields_and_adds_all_events() {
        let mut feed = PendingFeed::new();
        let txid = TxId([0xab; 32]);
        let events = vec![
            PendingTxEvent {
                covenant_id: CovenantId([0x90; 32]),
                kind: EventKind::Burn,
            },
            PendingTxEvent {
                covenant_id: CovenantId([0x10; 32]),
                kind: EventKind::Transition,
            },
        ];
        assert_eq!(
            track_pending_transaction_at_ms(&mut feed, txid, events, 5_000),
            PendingInsert::Added
        );

        let messages = pending_sse_jsons(&feed, &txid);
        assert_eq!(
            messages.len(),
            2,
            "legacy consumers still receive one hint per covenant"
        );
        // Reverse stable order means an old txid-keyed client finishes on the
        // same primary event the snapshot's scalar fields expose.
        assert_eq!(
            messages[0]["covenant_id"],
            CovenantId([0x90; 32]).to_string()
        );
        assert_eq!(
            messages[1]["covenant_id"],
            CovenantId([0x10; 32]).to_string()
        );
        for msg in messages {
            assert_eq!(msg["kind"], "pending");
            assert_eq!(msg["txid"], txid.to_string());
            assert_eq!(msg["events"].as_array().unwrap().len(), 2);
            assert_eq!(
                msg["events"][1]["covenant_id"],
                CovenantId([0x90; 32]).to_string()
            );
            assert_eq!(msg["revision"], feed.revision);
        }
    }

    fn transaction(txid: TxId, previous: kascov_core::Outpoint) -> kascov_core::Transaction {
        kascov_core::Transaction {
            txid,
            version: 1,
            inputs: vec![kascov_core::Input {
                previous_outpoint: previous,
                signature_script: vec![],
                compute_budget: 0,
            }],
            outputs: vec![],
            payload: b"ARGI".to_vec(),
        }
    }

    #[test]
    fn new_pending_argent_failure_is_bounded_and_identified() {
        let txid = TxId([0x71; 32]);
        let covenant_id = CovenantId([0x72; 32]);
        let tx = transaction(
            txid,
            kascov_core::Outpoint {
                txid: TxId([0x70; 32]),
                index: 1,
            },
        );
        let application = kascov_core::ApplicationPreprocess {
            raw_envelope: Some(b"ARGI".to_vec()),
            failures: vec![kascov_core::DecodeFailure {
                output_index: None,
                application_id: Some("duel".to_string()),
                artifact_id: None,
                code: "invalid_envelope".to_string(),
                detail: "bad envelope".to_string(),
            }],
            ..Default::default()
        };
        let mut feed = PendingFeed::new();
        assert_eq!(
            track_pending_transaction_at(
                &mut feed,
                &tx,
                vec![PendingTxEvent {
                    covenant_id,
                    kind: EventKind::Transition,
                }],
                application,
                1_000,
                std::time::Instant::now(),
            ),
            PendingInsert::Added
        );

        let message = pending_sse_jsons(&feed, &txid).remove(0);
        assert_eq!(message["kind"], "pending");
        assert_eq!(message["application"]["status"], "invalid");
        assert_eq!(
            message["application"]["failures"][0]["code"],
            "invalid_envelope"
        );
        assert_eq!(
            message["pending_id"],
            kascov_core::pending_event_id(txid, covenant_id, 0)
        );
        assert!(
            message.get("id").is_none(),
            "pending frames have no SSE id field"
        );
    }

    #[test]
    fn replacement_links_the_conflicting_transaction() {
        let old = TxId([0x81; 32]);
        let replacement = TxId([0x82; 32]);
        let previous = kascov_core::Outpoint {
            txid: TxId([0x80; 32]),
            index: 2,
        };
        let tx = transaction(old, previous);
        let mut feed = PendingFeed::new();
        track_pending_transaction_at(
            &mut feed,
            &tx,
            vec![PendingTxEvent {
                covenant_id: CovenantId([0x83; 32]),
                kind: EventKind::Transition,
            }],
            Default::default(),
            1_000,
            std::time::Instant::now(),
        );
        let spenders = std::collections::HashMap::from([(previous, replacement)]);
        let entry = feed.entries.get(&old).unwrap();
        assert_eq!(Some(replacement), replacement_for(old, entry, &spenders));
        let frame =
            pending_resolved_sse_json(&old, entry, "replaced", Some(replacement), 2).unwrap();
        assert_eq!(frame["resolution"], "replaced");
        assert_eq!(frame["replaced_by"], replacement.to_string());
    }

    #[tokio::test]
    async fn accepted_transaction_resolves_pending_immediately() {
        let txid = TxId([0x91; 32]);
        let mut feed = PendingFeed::new();
        track_pending_transaction_at_ms(
            &mut feed,
            txid,
            vec![PendingTxEvent {
                covenant_id: CovenantId([0x92; 32]),
                kind: EventKind::Genesis,
            }],
            1_000,
        );
        let pending = std::sync::Arc::new(tokio::sync::Mutex::new(feed));
        let (live_tx, mut live_rx) = tokio::sync::broadcast::channel(4);

        assert_eq!(
            1,
            resolve_accepted_pending(&pending, &live_tx, &HashSet::from([txid])).await
        );
        let frame: serde_json::Value =
            serde_json::from_str(&live_rx.recv().await.unwrap()).unwrap();
        assert_eq!(frame["kind"], "pending_resolved");
        assert_eq!(frame["resolution"], "accepted");
        assert_eq!(pending.lock().await.entries.len(), 0);
    }

    #[test]
    fn dropped_resolution_is_explicit() {
        let txid = TxId([0xa1; 32]);
        let mut feed = PendingFeed::new();
        track_pending_transaction_at_ms(
            &mut feed,
            txid,
            vec![PendingTxEvent {
                covenant_id: CovenantId([0xa2; 32]),
                kind: EventKind::Burn,
            }],
            1_000,
        );
        let entry = feed.entries.get(&txid).unwrap();
        let frame = pending_resolved_sse_json(&txid, entry, "dropped", None, 2).unwrap();
        assert_eq!(frame["resolution"], "dropped");
        assert!(frame.get("replaced_by").is_none());
    }
}
