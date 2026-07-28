use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
use kascov_core::{pending_event_id, BlockHash, CovenantId, Network, TxId};

#[test]
fn accepted_delivery_carries_the_same_pending_identity() {
    let path =
        std::env::temp_dir().join(format!("kascov-pending-argent-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
    let txid = TxId([0x31; 32]);
    let covenant_id = CovenantId([0x32; 32]);
    let mut batch = AcceptedBlockBatch::empty(BlockHash([0x33; 32]));
    batch.accepting_daa = 100;
    batch.accepting_blue_score = 100;
    batch.events.push(NewEvent {
        covenant_id,
        kind: EventKind::Transition,
        txid,
        tx_index: 2,
        event_index: 3,
        payload: Some(b"ARGI".to_vec()),
        lane_namespace: None,
    });

    let committed = store.apply_accepted_block(&batch).unwrap();
    let expected = pending_event_id(txid, covenant_id, 3);
    assert_eq!(
        committed.deliveries[0].pending_id.as_deref(),
        Some(expected.as_str())
    );
    let replayed = store.delivery_page(None, 10).unwrap();
    assert_eq!(committed.deliveries[0].pending_id, replayed[0].pending_id);
}
