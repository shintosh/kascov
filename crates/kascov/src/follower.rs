use super::*;

/// Per-network follower liveness, shared with /healthz. Epoch ms; both fields
/// initialized to boot time so a fresh instance gets the same 10-minute grace
/// as a healthy one.
pub(super) struct SyncHealth {
    /// The last successful sync pass.
    pub(super) last_sync_ok_ms: std::sync::atomic::AtomicI64,
    /// The last pass that MOVED processed_daa. Tracked separately because a
    /// stranded cursor can make passes "succeed" without doing anything
    /// (some nodes answer it with an empty walk) — liveness alone would keep
    /// reporting ok while the index falls behind forever.
    pub(super) last_progress_ms: std::sync::atomic::AtomicI64,
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

/// Follow a network's virtual chain forever, reconnecting on any failure.
pub(super) async fn follow_forever(
    network: Network,
    rpc: Option<String>,
    db: std::path::PathBuf,
    live_tx: tokio::sync::broadcast::Sender<std::sync::Arc<str>>,
    hook_tx: tokio::sync::mpsc::Sender<HookEvent>,
    health: std::sync::Arc<SyncHealth>,
    performance: std::sync::Arc<kascov_core::performance::PerformanceMetrics>,
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
        // One-shot per database + derivation version: build the KCC20 token
        // accounting tables from history (then apply() keeps them current
        // incrementally). Sited here, NOT in Store::open, so the serve path
        // never pays it (WAL readers keep serving while it runs) — and
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
        let node = match NodeHandle::connect(network, rpc.as_deref()).await {
            Ok(node) => node,
            Err(err) => {
                tracing::warn!("{network}: connect failed ({err}), retrying in 10s");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
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
        loop {
            let publication = performance.clone();
            let result = kascov_core::sync::sync_once_measured(
                &node,
                &mut store,
                None,
                &performance,
                |update| match update {
                    SyncUpdate::Event {
                        covenant_id,
                        kind,
                        txid,
                        accepting_daa,
                        tx_index,
                    } => {
                        let _publication =
                            publication.timer(kascov_core::performance::Stage::Publication);
                        tracing::info!("{network}: {} covenant {covenant_id}", kind.as_str());
                        // Fan out to any open SSE streams; serialization is skipped
                        // entirely when nobody is listening, and send() failing
                        // (zero receivers) is fine.
                        if live_tx.receiver_count() > 0 {
                            let msg = serde_json::json!({
                                "covenant_id": covenant_id,
                                "kind": kind.as_str(),
                                "txid": txid,
                                "accepting_daa": accepting_daa,
                                "tx_index": tx_index,
                            })
                            .to_string();
                            let _ = live_tx.send(msg.into());
                        }
                        // Webhook queue: try_send so a slow/stalled delivery task
                        // can never block the indexer — under backpressure (e.g.
                        // the initial full sync) extra events are dropped, which
                        // is fine: webhooks are hints, not a durable feed.
                        let _ = hook_tx.try_send(HookEvent {
                            covenant_id,
                            kind: kind.as_str(),
                            txid,
                            accepting_daa,
                            tx_index,
                        });
                    }
                    SyncUpdate::Reorg { rolled_back } => {
                        let _publication =
                            publication.timer(kascov_core::performance::Stage::Publication);
                        tracing::info!("{network}: reorg — rolled back {rolled_back} chain blocks");
                        // Same fire-and-forget fan-out as events; the "kind":"reorg"
                        // tag lets subscribers distinguish it from covenant activity.
                        if live_tx.receiver_count() > 0 {
                            let msg = serde_json::json!({
                                "kind": "reorg",
                                "rolled_back": rolled_back,
                            })
                            .to_string();
                            let _ = live_tx.send(msg.into());
                        }
                    }
                    SyncUpdate::Progress(_) => {}
                },
            )
            .await;
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
                            continue;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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
                        continue;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    break;
                }
            }
        }
    }
}
