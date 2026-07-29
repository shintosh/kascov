use std::path::Path;

use anyhow::{bail, Result};
use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
use kascov_core::{BlockHash, CovenantId, Network, StreamCursor, TxId};

pub fn seed_delivery_store(
    database: &Path,
    network: Network,
    records: u64,
) -> Result<StreamCursor> {
    if records == 0 || records > u64::from(u32::MAX) {
        bail!("records must be between 1 and {}", u32::MAX);
    }
    let mut store = Store::open(database, network)?;
    let current = store.delivery_high_water()?;
    if current.seq == records {
        return Ok(current);
    }
    if current.seq != 0 {
        bail!(
            "delivery fixture has {} records; expected an empty store or exactly {records}",
            current.seq
        );
    }

    let mut batch = AcceptedBlockBatch::empty(BlockHash([0xb0; 32]));
    batch.accepting_daa = 1;
    batch.accepting_time_ms = 1;
    batch.accepting_blue_score = 1;
    batch.events = (0..records as u32)
        .map(|event_index| NewEvent {
            covenant_id: CovenantId([0xc0; 32]),
            kind: EventKind::Transition,
            txid: TxId([0xd0; 32]),
            tx_index: 0,
            event_index,
            payload: None,
            lane_namespace: None,
        })
        .collect();
    let committed = store.apply_accepted_block(&batch)?;
    if committed.deliveries.len() as u64 != records {
        bail!(
            "seed committed {} delivery records; expected {records}",
            committed.deliveries.len()
        );
    }
    Ok(store.delivery_high_water()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_creates_exact_replay_history_and_is_idempotent() {
        let database =
            std::env::temp_dir().join(format!("kascov-bench-seed-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&database);

        let first = seed_delivery_store(&database, Network::Testnet(10), 1_024).unwrap();
        let second = seed_delivery_store(&database, Network::Testnet(10), 1_024).unwrap();

        assert_eq!(1_024, first.seq);
        assert_eq!(first, second);
        let store = Store::open_reader(&database, Network::Testnet(10)).unwrap();
        assert_eq!(
            1_024,
            store.delivery_replay_page(None, 1_024).unwrap().len()
        );
    }
}
