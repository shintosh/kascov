use super::*;

const WRITER_MUTATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

type WriterOperation = Box<dyn FnOnce(&mut Store) + Send + 'static>;

pub(super) struct WriterMutation(WriterOperation);

impl WriterMutation {
    fn apply(self, store: &mut Store) {
        (self.0)(store);
    }
}

#[derive(Clone)]
pub(super) struct WriterHandle {
    tx: tokio::sync::mpsc::Sender<WriterMutation>,
}

impl WriterHandle {
    pub(super) async fn mutate<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T> + Send + 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.tx
            .try_send(WriterMutation(Box::new(move |store| {
                let _ = result_tx.send(operation(store));
            })))
            .map_err(|err| anyhow::anyhow!("canonical writer unavailable: {err}"))?;
        tokio::time::timeout(WRITER_MUTATION_TIMEOUT, result_rx)
            .await
            .map_err(|_| anyhow::anyhow!("canonical writer mutation timed out"))?
            .map_err(|_| anyhow::anyhow!("canonical writer stopped before mutation completed"))?
    }
}

pub(super) fn writer_channel(
    capacity: usize,
) -> (WriterHandle, tokio::sync::mpsc::Receiver<WriterMutation>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (WriterHandle { tx }, rx)
}

async fn await_with_mutations<F>(
    future: F,
    mutations: &mut tokio::sync::mpsc::Receiver<WriterMutation>,
    store: &mut Store,
) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        tokio::select! {
            output = &mut future => return output,
            mutation = mutations.recv() => match mutation {
                Some(mutation) => mutation.apply(store),
                None => return future.await,
            },
        }
    }
}

/// Per-network follower liveness, shared with /healthz. Epoch ms; both fields
/// initialized to boot time so a fresh instance gets the same 10-minute grace
/// as a healthy one.
pub(super) struct SyncHealth {
    /// Most recent virtual-chain notification observed from the node.
    pub(super) last_node_notification_ms: std::sync::atomic::AtomicI64,
    /// Most recent reconciliation start, regardless of its trigger.
    pub(super) last_reconciliation_start_ms: std::sync::atomic::AtomicI64,
    /// Delay from the latest notification to its reconciliation start.
    pub(super) notification_to_reconciliation_ms: std::sync::atomic::AtomicU64,
    /// The last successful sync pass.
    pub(super) last_sync_ok_ms: std::sync::atomic::AtomicI64,
    /// The last pass that MOVED processed_daa. Tracked separately because a
    /// stranded cursor can make passes "succeed" without doing anything
    /// (some nodes answer it with an empty walk) — liveness alone would keep
    /// reporting ok while the index falls behind forever.
    pub(super) last_progress_ms: std::sync::atomic::AtomicI64,
    /// Highest durable accepted cursor, including the database stream epoch.
    delivery_high_water: std::sync::RwLock<Option<kascov_core::StreamCursor>>,
}

impl SyncHealth {
    pub(super) fn new(boot_ms: u64) -> Self {
        Self {
            last_node_notification_ms: std::sync::atomic::AtomicI64::new(0),
            last_reconciliation_start_ms: std::sync::atomic::AtomicI64::new(boot_ms as i64),
            notification_to_reconciliation_ms: std::sync::atomic::AtomicU64::new(0),
            last_sync_ok_ms: std::sync::atomic::AtomicI64::new(boot_ms as i64),
            last_progress_ms: std::sync::atomic::AtomicI64::new(boot_ms as i64),
            delivery_high_water: std::sync::RwLock::new(None),
        }
    }

    pub(super) fn record_delivery_cursor(&self, cursor: kascov_core::StreamCursor) {
        let mut current = self.delivery_high_water.write().expect("sync health poisoned");
        if current.is_none_or(|value| value.epoch != cursor.epoch || value.seq < cursor.seq) {
            *current = Some(cursor);
        }
    }

    pub(super) fn delivery_cursor(&self) -> Option<kascov_core::StreamCursor> {
        *self.delivery_high_water.read().expect("sync health poisoned")
    }
}

#[cfg(test)]
mod writer_mutation_tests {
    use super::*;

    #[tokio::test]
    async fn queued_mutation_uses_the_existing_writer_lease() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("writer.db");
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let (writer, mut mutations) = writer_channel(1);

        let requested = tokio::spawn(async move {
            writer
                .mutate(|store| {
                    store.put_verified_source("hash", "00", "source", "", None, 1)?;
                    Ok(store.get_verified_source("hash")?.is_some())
                })
                .await
        });
        let mutation = mutations.recv().await.unwrap();
        mutation.apply(&mut store);

        assert!(requested.await.unwrap().unwrap());
        assert!(Store::open(&path, Network::Testnet(10)).is_err());
    }
}

#[cfg(test)]
mod sync_health_tests {
    use super::*;

    #[test]
    fn delivery_cursor_restores_and_keeps_the_epoch() {
        let health = SyncHealth::new(1);
        let restored: kascov_core::StreamCursor =
            "00112233445566778899aabbccddeeff:41".parse().unwrap();
        let published = restored.checked_next().unwrap();

        health.record_delivery_cursor(restored);
        health.record_delivery_cursor(published);

        assert_eq!(Some(published), health.delivery_cursor());
    }
}

/// After repeated sync failures — or passes that succeed without advancing —
/// try to un-wedge the cursor. Preferred: re-anchor onto the newest walkable
/// block of our own indexed history (a STRANDED cursor: the block still
/// exists, but the node refuses to walk the virtual chain from it — a branch
/// abandoned by a deep reorg, or past walk retention). Last resort, only when
/// nothing we indexed is walkable AND the cursor block is gone entirely (the
/// true testnet-reset signature): restart at the current sink — indexed
/// history stays, and the gap is real (the old chain is gone), not an artifact.
pub(super) async fn recover_wedged_cursor(
    node: &NodeHandle,
    store: &mut Store,
    network: Network,
) -> bool {
    use kascov_core::sync::ReAnchor;
    match kascov_core::sync::re_anchor(node, store).await {
        Ok(ReAnchor::NotWedged) => false, // the failures are something else
        Ok(ReAnchor::Anchored(anchor)) => {
            let anchor_daa = store.processed_daa().ok().flatten().unwrap_or(0);
            let gap = match node.dag_info().await {
                Ok(dag) => {
                    format!(
                        "{} DAA to re-sync",
                        dag.virtual_daa_score.saturating_sub(anchor_daa)
                    )
                }
                Err(_) => "gap unknown".into(),
            };
            tracing::warn!(
                "{network}: cursor was stranded — re-anchored at {anchor} (DAA {anchor_daa}, {gap})"
            );
            true
        }
        Ok(ReAnchor::NothingWalkable) => {
            let Ok(Some(cursor)) = store.cursor() else {
                return false;
            };
            let Ok(dag) = node.dag_info().await else {
                return false;
            };
            // Two signatures forfeit the gap: the cursor block is GONE (the
            // classic testnet reset), or its header survives while the node —
            // provably able to walk from its own sink — can walk from neither
            // the cursor nor ANY sampled block of our history (deep-reorg
            // strand past retention; observed in the Jul-2026 TN10 incident).
            // A node that fails the sink control is sick, not authoritative.
            if node.block_with_txs(cursor).await.is_ok()
                && node.virtual_chain_from(dag.sink).await.is_err()
            {
                return false;
            }
            tracing::error!(
                "{network}: cursor {cursor} is wedged beyond re-anchoring (nothing indexed is walkable) — restarting from sink {}; the skipped gap is real and unrecoverable from this node",
                dag.sink
            );
            store.reset_cursor(dag.sink).is_ok()
        }
        Err(err) => {
            tracing::warn!("{network}: re-anchor attempt failed ({err})");
            false
        }
    }
}

/// A pending tx_index backfill re-fetches every retained block over RPC —
/// heavy enough to starve a booting instance. Hold it back this long after
/// boot; a completed backfill (the steady state) skips the wait entirely.
const TX_BACKFILL_BOOT_DELAY: std::time::Duration = std::time::Duration::from_secs(120);

/// Limit optional token work so accepted-chain reconciliation keeps priority.
const OPTIONAL_PROJECTION_CHUNK: u64 = 32;

/// Cursor reconciliation safety net when the node notification path is quiet.
const RECONCILIATION_WATCHDOG: std::time::Duration = std::time::Duration::from_secs(5);

/// Follow a network's virtual chain forever, reconnecting on any failure.
pub(super) async fn follow_forever(
    network: Network,
    rpc: Option<String>,
    db: std::path::PathBuf,
    delivery_tx: tokio::sync::broadcast::Sender<std::sync::Arc<kascov_core::DeliveryRecord>>,
    hook_tx: tokio::sync::mpsc::Sender<HookEvent>,
    pending_tx: tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
    pending: std::sync::Arc<tokio::sync::Mutex<crate::pending::PendingFeed>>,
    decoder: std::sync::Arc<dyn kascov_core::ApplicationDecoder>,
    health: std::sync::Arc<SyncHealth>,
    performance: std::sync::Arc<kascov_core::performance::PerformanceMetrics>,
    mut writer_mutations: tokio::sync::mpsc::Receiver<WriterMutation>,
) {
    use kascov_core::sync::SyncUpdate;
    // Per-network node override: KASCOV_RPC_TESTNET_10 / KASCOV_RPC_MAINNET.
    // The global --rpc can't express "TN10 on our node, mainnet on the
    // resolver", and connect() verifies the node's network, so a URL pasted
    // under the wrong variable fails loudly instead of cross-feeding.
    let env_key = format!(
        "KASCOV_RPC_{}",
        network.to_string().to_uppercase().replace('-', "_")
    );
    let rpc = match std::env::var(&env_key) {
        Ok(url) if !url.trim().is_empty() => {
            tracing::info!("{network}: following via {env_key}={url}");
            Some(url)
        }
        _ => rpc,
    };
    // This task is spawned once per network at boot, so "task start" = boot.
    let boot = tokio::time::Instant::now();
    // Lives across reconnects: every sync failure breaks to a fresh session,
    // so a per-session counter would reset before ever reaching the
    // testnet-reset recovery threshold below.
    let mut consecutive_errors = 0u32;
    // Also across reconnects: catches the wedge consecutive_errors can't see
    // (passes that "succeed" without moving the cursor).
    let mut progress = kascov_core::sync::ProgressWatch::default();
    loop {
        let mut store = match kascov_core::store::Store::open(&db, network) {
            Ok(store) => store,
            Err(err) => {
                tracing::error!("{network}: cannot open store: {err}");
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        };
        match store.delivery_high_water() {
            Ok(cursor) => health.record_delivery_cursor(cursor),
            Err(err) => tracing::warn!("{network}: cannot restore delivery cursor: {err}"),
        }
        // One-shot per database + derivation version: build the KCC20 token
        // accounting tables from history. Sited here, NOT in Store::open, so
        // the serve path never pays it (WAL readers keep serving while it runs) — and
        // BEFORE the node connect, because it needs no node and must not
        // wait out an outage. The meta gate makes reruns O(1); a failure
        // retries next session.
        // Re-read stored reveals with the current state-block locator BEFORE
        // deriving: discovery enumerates candidates from the stored template
        // column, so a token whose build the old pinned skeletons missed stays
        // invisible until its rows are re-stamped. Same siting rule as the
        // derivation below — off Store::open, off the serve path.
        match store.restamp_kcc20_if_stale() {
            Ok(0) => {}
            Ok(n) => tracing::info!("{network}: KCC20 re-stamp complete — {n} reveals restamped"),
            Err(err) => {
                tracing::warn!("{network}: KCC20 re-stamp failed ({err}) — will retry next session")
            }
        }
        match store.derive_tokens_if_stale() {
            Ok(0) => {}
            Ok(n) => {
                tracing::info!("{network}: token derivation pass complete — {n} tokens derived")
            }
            Err(err) => tracing::warn!(
                "{network}: token derivation failed ({err}) — will retry next session"
            ),
        }
        let node = match await_with_mutations(
            NodeHandle::connect(network, rpc.as_deref()),
            &mut writer_mutations,
            &mut store,
        )
        .await
        {
            Ok(node) => node,
            Err(err) => {
                tracing::warn!("{network}: connect failed ({err}), retrying in 10s");
                await_with_mutations(
                    tokio::time::sleep(std::time::Duration::from_secs(10)),
                    &mut writer_mutations,
                    &mut store,
                )
                .await;
                continue;
            }
        };
        // One-shot per database: stamp tx_index onto pre-capture event rows
        // still inside node retention. Best-effort — a failed walk resumes
        // next session and never blocks following the chain.
        // Boot-storm guard: when the one-shot still has work, hold it (and
        // this network's first follow) until the instance has been serving
        // for a while — requests come first, heavy background work second.
        if !store.tx_index_backfill_done().unwrap_or(true) {
            let since_boot = boot.elapsed();
            if since_boot < TX_BACKFILL_BOOT_DELAY {
                tokio::time::sleep(TX_BACKFILL_BOOT_DELAY - since_boot).await;
            }
        }
        match kascov_core::sync::backfill_tx_index(&node, &mut store).await {
            Ok(0) => {}
            Ok(n) => tracing::info!("{network}: tx_index backfill stamped {n} event rows"),
            Err(err) => tracing::warn!(
                "{network}: tx_index backfill interrupted ({err}) — will resume next session"
            ),
        }
        tracing::info!("{network}: following the chain");
        let mut schedule =
            performance::ReconcileSchedule::new(node.wakeups(), RECONCILIATION_WATCHDOG);
        'connected: loop {
            let mut trigger =
                await_with_mutations(schedule.next(), &mut writer_mutations, &mut store).await;
            if let performance::ReconcileTrigger::Disconnected { .. } = trigger {
                break;
            }
            loop {
                let reconcile_started = now_ms();
                health.last_reconciliation_start_ms.store(
                    reconcile_started as i64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                if let performance::ReconcileTrigger::Notification { observed_at_ms } = trigger {
                    health
                        .last_node_notification_ms
                        .store(observed_at_ms as i64, std::sync::atomic::Ordering::Relaxed);
                    health.notification_to_reconciliation_ms.store(
                        reconcile_started.saturating_sub(observed_at_ms),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }

                let publication = performance.clone();
                let mut accepted_txids = std::collections::HashSet::new();
                let mut publish = |deliveries: Vec<kascov_core::DeliveryRecord>, webhook: bool| {
                    let _publication =
                        publication.timer(kascov_core::performance::Stage::Publication);
                    for record in deliveries {
                        tracing::info!(
                            "{network}: committed covenant {} at {}",
                            record.covenant_id,
                            record.cursor
                        );
                        health.record_delivery_cursor(record.cursor);
                        let record = std::sync::Arc::new(record);
                        if delivery_tx.receiver_count() > 0 {
                            let _ = delivery_tx.send(record.clone());
                        }
                        if webhook {
                            accepted_txids.insert(record.txid);
                            let _ = hook_tx.try_send(HookEvent { delivery: record });
                        }
                    }
                };
                let result = kascov_core::sync::sync_once_measured_with_decoder(
                    &node,
                    &mut store,
                    None,
                    &performance,
                    decoder.as_ref(),
                    |update| match update {
                        SyncUpdate::Committed(batch) => publish(batch.deliveries, true),
                        SyncUpdate::Removed(batch) => publish(batch.deliveries, false),
                        SyncUpdate::Reorg { rolled_back } => tracing::info!(
                            "{network}: reorg — rolled back {rolled_back} chain blocks"
                        ),
                        SyncUpdate::Progress(_) => {}
                    },
                )
                .await;
                if !accepted_txids.is_empty() {
                    crate::pending::resolve_accepted_pending(
                        &pending,
                        &pending_tx,
                        &accepted_txids,
                    )
                    .await;
                }
                match result {
                    Ok(_) => {
                        consecutive_errors = 0;
                        let now = now_ms() as i64;
                        health
                            .last_sync_ok_ms
                            .store(now, std::sync::atomic::Ordering::Relaxed);
                        let verdict = progress.observe(
                            store.processed_daa().ok().flatten(),
                            store.tip().ok().flatten().map(|(daa, _)| daa),
                        );
                        if verdict.advanced {
                            health
                                .last_progress_ms
                                .store(now, std::sync::atomic::Ordering::Relaxed);
                        }
                        if verdict.demand_recovery {
                            tracing::warn!(
                                "{network}: {} passes succeeded without moving the cursor while > {} DAA behind the tip — attempting recovery",
                                kascov_core::sync::WEDGE_PASSES,
                                kascov_core::sync::WEDGE_LAG_DAA
                            );
                            if recover_wedged_cursor(&node, &mut store, network).await {
                                trigger = performance::ReconcileTrigger::Watchdog;
                                continue;
                            }
                        }
                    }
                    Err(err) => {
                        consecutive_errors += 1;
                        tracing::warn!(
                            "{network}: sync interrupted ({err}), attempt {consecutive_errors}"
                        );
                        if consecutive_errors >= 3
                            && recover_wedged_cursor(&node, &mut store, network).await
                        {
                            consecutive_errors = 0;
                            trigger = performance::ReconcileTrigger::Watchdog;
                            continue;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        break 'connected;
                    }
                }

                if let Some(pending) = schedule.take_pending() {
                    if let performance::ReconcileTrigger::Disconnected { .. } = pending {
                        break 'connected;
                    }
                    trigger = pending;
                    continue;
                }
                let caught_up = match (store.cursor(), node.dag_info().await) {
                    (Ok(Some(cursor)), Ok(dag)) => cursor == dag.sink,
                    (_, Err(err)) => {
                        tracing::debug!("{network}: could not confirm the latest node tip ({err})");
                        true
                    }
                    _ => false,
                };
                if !caught_up {
                    trigger = performance::ReconcileTrigger::Watchdog;
                    continue;
                }
                break;
            }

            let accepted_pending = schedule.accepted_work_pending();
            match store.drain_optional_projection_chunk(accepted_pending, OPTIONAL_PROJECTION_CHUNK)
            {
                Ok(drain) if drain.processed > 0 => tracing::debug!(
                    "{network}: projected {} delivery records; {} remain",
                    drain.processed,
                    drain.status.queued
                ),
                Ok(_) => {}
                Err(err) => tracing::debug!(
                    "{network}: optional token projection deferred after error ({err})"
                ),
            }
        }
    }
}
