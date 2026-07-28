use std::path::Path;

use kascov_core::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
use kascov_core::{BlockHash, CovenantId, DeliveryKind, Network, TxId};
use rusqlite::Connection;

const NETWORK: Network = Network::Testnet(10);

fn batch(block: u8, daa: u64, blue_score: u64, txid: u8, covenant: u8) -> AcceptedBlockBatch {
    let mut batch = AcceptedBlockBatch::empty(BlockHash([block; 32]));
    batch.accepting_daa = daa;
    batch.accepting_blue_score = blue_score;
    batch.events.push(NewEvent {
        covenant_id: CovenantId([covenant; 32]),
        kind: EventKind::Genesis,
        txid: TxId([txid; 32]),
        tx_index: u32::from(txid),
        event_index: 0,
        payload: None,
        lane_namespace: None,
    });
    batch
}

fn legacy_fixture(path: &Path, missing_order: bool) -> Vec<(Vec<u8>, u64, Vec<u8>, u64)> {
    let _ = std::fs::remove_file(path);
    let mut store = Store::open(path, NETWORK).unwrap();
    store.apply_accepted_block(&batch(3, 300, 30, 3, 3)).unwrap();
    store.apply_accepted_block(&batch(1, 100, 10, 1, 1)).unwrap();
    store.apply_accepted_block(&batch(2, 200, 20, 2, 2)).unwrap();
    drop(store);

    let connection = Connection::open(path).unwrap();
    let before = event_facts(&connection);
    connection
        .execute_batch(
            "DROP INDEX IF EXISTS ev_delivery_stream_seq;
             DROP TABLE delivery_applications;
             DROP TABLE delivery_log;
             DROP TABLE canonical_batches;
             UPDATE covenant_events SET delivery_stream_seq = NULL;
             DELETE FROM meta WHERE key IN (
                'stream_epoch', 'next_stream_seq', 'delivery_backfill_complete',
                'delivery_history_start_daa', 'delivery_history_order_complete',
                'optional_projection_cursor'
             );",
        )
        .unwrap();
    if missing_order {
        connection
            .execute(
                "UPDATE covenant_events
                 SET event_index = NULL, tx_index = NULL
                 WHERE accepting_daa = 200",
                [],
            )
            .unwrap();
    }
    before
}

fn event_facts(connection: &Connection) -> Vec<(Vec<u8>, u64, Vec<u8>, u64)> {
    let mut statement = connection
        .prepare(
            "SELECT covenant_id, seq, txid, accepting_daa
             FROM covenant_events ORDER BY covenant_id, seq",
        )
        .unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    rows
}

#[test]
fn delivery_backfill_resumes_preserves_facts_and_is_idempotent() {
    let path = std::env::temp_dir().join(format!(
        "kascov-delivery-migration-resume-{}.db",
        std::process::id()
    ));
    let before = legacy_fixture(&path, true);

    assert!(Store::open(&path, NETWORK).is_err(), "the live writer must refuse incomplete history");
    let mut migration = Store::open_for_delivery_migration(&path, NETWORK).unwrap();
    assert!(Store::open_for_delivery_migration(&path, NETWORK).is_err(), "the migration owns the writer lease");

    let first = migration.backfill_delivery_batch(1).unwrap();
    assert_eq!(1, first.migrated);
    assert_eq!(2, first.remaining);
    assert!(!first.complete);
    assert_eq!(100, migration.delivery_page(None, 10).unwrap()[0].accepting_daa);
    drop(migration);

    assert!(Store::open(&path, NETWORK).is_err(), "an interrupted migration stays fail-closed");
    let mut resumed = Store::open_for_delivery_migration(&path, NETWORK).unwrap();
    assert!(!resumed.backfill_delivery_batch(1).unwrap().complete);
    let finished = resumed.backfill_delivery_batch(1).unwrap();
    assert!(finished.complete);
    assert_eq!(0, finished.remaining);
    assert_eq!(100, finished.history_start_daa);
    assert!(!finished.order_complete);

    let records = resumed.delivery_page(None, 10).unwrap();
    assert_eq!(vec![1, 2, 3], records.iter().map(|record| record.cursor.seq).collect::<Vec<_>>());
    assert_eq!(vec![100, 200, 300], records.iter().map(|record| record.accepting_daa).collect::<Vec<_>>());
    assert!(records.iter().all(|record| record.kind == DeliveryKind::Accepted));
    assert!(records.iter().any(|record| !record.order_complete));
    let again = resumed.backfill_delivery_batch(10).unwrap();
    assert!(again.complete);
    assert_eq!(0, again.migrated);
    drop(resumed);

    let live = Store::open(&path, NETWORK).unwrap();
    assert!(live.delivery_backfill_complete().unwrap());
    assert_eq!(before, event_facts(&Connection::open(&path).unwrap()));
}

#[test]
fn complete_legacy_order_is_reported_when_all_keys_survive() {
    let path = std::env::temp_dir().join(format!(
        "kascov-delivery-migration-order-{}.db",
        std::process::id()
    ));
    legacy_fixture(&path, false);
    assert!(Store::open(&path, NETWORK).is_err());
    let mut migration = Store::open_for_delivery_migration(&path, NETWORK).unwrap();
    let progress = migration.backfill_delivery_batch(100).unwrap();
    assert!(progress.complete);
    assert!(progress.order_complete);
    assert!(migration
        .delivery_page(None, 10)
        .unwrap()
        .iter()
        .all(|record| record.order_complete));
}
