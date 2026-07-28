use std::str::FromStr;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::delivery::{DeliveryRecord, StreamCursor, StreamEpoch};
use crate::store::Store;
use crate::{BlockHash, Error, Result};

const DELIVERY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS delivery_log (
    stream_seq INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    source_stream_seq INTEGER,
    covenant_id BLOB NOT NULL,
    covenant_event_seq INTEGER NOT NULL,
    txid BLOB NOT NULL,
    accepting_block BLOB NOT NULL,
    accepting_daa INTEGER NOT NULL,
    tx_index INTEGER,
    event_index INTEGER,
    order_complete INTEGER NOT NULL,
    pending_id TEXT,
    data_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_by_covenant
    ON delivery_log(covenant_id, stream_seq);
CREATE INDEX IF NOT EXISTS delivery_by_accepting
    ON delivery_log(accepting_block, stream_seq);

CREATE TABLE IF NOT EXISTS delivery_applications (
    stream_seq INTEGER NOT NULL,
    output_index INTEGER NOT NULL,
    application_id TEXT NOT NULL,
    artifact_id BLOB NOT NULL,
    actor_path TEXT NOT NULL,
    PRIMARY KEY (stream_seq, output_index, application_id, actor_path)
);
CREATE INDEX IF NOT EXISTS delivery_by_application
    ON delivery_applications(application_id, stream_seq);
CREATE INDEX IF NOT EXISTS delivery_by_artifact
    ON delivery_applications(artifact_id, stream_seq);
CREATE INDEX IF NOT EXISTS delivery_by_actor
    ON delivery_applications(actor_path, stream_seq);

CREATE TABLE IF NOT EXISTS canonical_batches (
    accepting_block BLOB PRIMARY KEY,
    accepting_daa INTEGER NOT NULL,
    first_stream_seq INTEGER,
    last_stream_seq INTEGER
);
";

pub(crate) fn migrate(conn: &Connection, legacy_schema: bool) -> Result<()> {
    add_column(
        conn,
        "ALTER TABLE covenant_events ADD COLUMN event_index INTEGER",
    )?;
    add_column(
        conn,
        "ALTER TABLE covenant_events ADD COLUMN delivery_stream_seq INTEGER",
    )?;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ev_delivery_stream_seq
         ON covenant_events(delivery_stream_seq)
         WHERE delivery_stream_seq IS NOT NULL",
        [],
    )
    .map_err(db_err)?;
    conn.execute_batch(DELIVERY_SCHEMA).map_err(db_err)?;

    let backfill_complete = if legacy_schema { "0" } else { "1" };
    let order_complete = if legacy_schema { "0" } else { "1" };
    for (key, value) in [
        ("next_stream_seq", "1"),
        ("delivery_backfill_complete", backfill_complete),
        ("delivery_history_start_daa", "0"),
        ("delivery_history_order_complete", order_complete),
        ("optional_projection_cursor", "0"),
    ] {
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(db_err)?;
    }
    Ok(())
}

fn add_column(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(error, Some(detail)))
            if error.code == rusqlite::ErrorCode::Unknown
                && detail.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(error) => Err(db_err(error)),
    }
}

pub(crate) fn canonical_batch_daa(
    tx: &rusqlite::Transaction<'_>,
    accepting_block: &BlockHash,
) -> Result<Option<u64>> {
    tx.query_row(
        "SELECT accepting_daa FROM canonical_batches WHERE accepting_block = ?1",
        [accepting_block.0.as_slice()],
        |row| row.get(0),
    )
    .optional()
    .map_err(db_err)
}

pub(crate) fn transaction_stream_epoch(
    tx: &rusqlite::Transaction<'_>,
) -> Result<StreamEpoch> {
    let raw = meta(tx, "stream_epoch")?.ok_or_else(|| Error::Invalid {
        what: "stream epoch",
        value: "missing".to_owned(),
    })?;
    StreamEpoch::from_str(&raw)
}

pub(crate) fn transaction_next_stream_seq(tx: &rusqlite::Transaction<'_>) -> Result<u64> {
    parse_meta_u64(tx, "next_stream_seq")
}

pub(crate) fn insert_delivery(
    tx: &rusqlite::Transaction<'_>,
    record: &DeliveryRecord,
) -> Result<()> {
    let data_json = serde_json::to_string(record).map_err(|error| Error::Invalid {
        what: "delivery record",
        value: error.to_string(),
    })?;
    let kind = match record.kind {
        crate::DeliveryKind::Accepted => "accepted",
        crate::DeliveryKind::Removed => "removed",
        crate::DeliveryKind::ProjectionRepaired => "projection_repaired",
    };
    tx.execute(
        "INSERT INTO delivery_log (
            stream_seq, kind, source_stream_seq, covenant_id,
            covenant_event_seq, txid, accepting_block, accepting_daa,
            tx_index, event_index, order_complete, pending_id, data_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            record.cursor.seq,
            kind,
            record.source_cursor.map(|cursor| cursor.seq),
            record.covenant_id.0.as_slice(),
            record.covenant_event_seq,
            record.txid.0.as_slice(),
            record.accepting_block.0.as_slice(),
            record.accepting_daa,
            record.tx_index,
            record.event_index,
            record.order_complete,
            record.pending_id,
            data_json,
        ],
    )
    .map_err(db_err)?;
    for application in &record.applications {
        tx.execute(
            "INSERT INTO delivery_applications (
                stream_seq, output_index, application_id, artifact_id, actor_path
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.cursor.seq,
                application.output_index,
                application.application_id,
                application.artifact_id.as_slice(),
                application.actor_path,
            ],
        )
        .map_err(db_err)?;
    }
    Ok(())
}

pub(crate) fn finish_canonical_batch(
    tx: &rusqlite::Transaction<'_>,
    accepting_block: &BlockHash,
    accepting_daa: u64,
    first_stream_seq: Option<u64>,
    last_stream_seq: Option<u64>,
    next_stream_seq: u64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO canonical_batches (
            accepting_block, accepting_daa, first_stream_seq, last_stream_seq
         ) VALUES (?1, ?2, ?3, ?4)",
        params![
            accepting_block.0.as_slice(),
            accepting_daa,
            first_stream_seq,
            last_stream_seq,
        ],
    )
    .map_err(db_err)?;
    tx.execute(
        "UPDATE meta SET value = ?1 WHERE key = 'next_stream_seq'",
        [next_stream_seq.to_string()],
    )
    .map_err(db_err)?;
    Ok(())
}

pub(crate) fn canonical_deliveries_for_block(
    conn: &Connection,
    accepting_block: &BlockHash,
) -> Result<Vec<DeliveryRecord>> {
    let mut statement = conn
        .prepare(
            "SELECT delivery_log.data_json
             FROM canonical_batches
             JOIN delivery_log
               ON delivery_log.stream_seq BETWEEN canonical_batches.first_stream_seq
                                              AND canonical_batches.last_stream_seq
             WHERE canonical_batches.accepting_block = ?1
               AND delivery_log.accepting_block = canonical_batches.accepting_block
               AND delivery_log.kind = 'accepted'
             ORDER BY delivery_log.stream_seq",
        )
        .map_err(db_err)?;
    let deliveries = statement
        .query_map([accepting_block.0.as_slice()], |row| row.get::<_, String>(0))
        .map_err(db_err)?
        .map(|row| {
            let json = row.map_err(db_err)?;
            serde_json::from_str(&json).map_err(|error| Error::Invalid {
                what: "delivery record",
                value: error.to_string(),
            })
        })
        .collect();
    deliveries
}

pub(crate) fn append_removed_deliveries(
    tx: &rusqlite::Transaction<'_>,
    removed: &[BlockHash],
) -> Result<Vec<DeliveryRecord>> {
    let epoch = transaction_stream_epoch(tx)?;
    let mut next_stream_seq = transaction_next_stream_seq(tx)?;
    let mut removals = Vec::new();
    for block in removed {
        let bounds = tx
            .query_row(
                "SELECT first_stream_seq, last_stream_seq
                 FROM canonical_batches WHERE accepting_block = ?1",
                [block.0.as_slice()],
                |row| Ok((row.get::<_, Option<u64>>(0)?, row.get::<_, Option<u64>>(1)?)),
            )
            .optional()
            .map_err(db_err)?;
        let Some((Some(first), Some(last))) = bounds else { continue };
        let sources = {
            let mut statement = tx
                .prepare(
                    "SELECT data_json FROM delivery_log
                     WHERE stream_seq BETWEEN ?1 AND ?2
                       AND accepting_block = ?3 AND kind = 'accepted'
                     ORDER BY stream_seq",
                )
                .map_err(db_err)?;
            let records = statement
                .query_map(params![first, last, block.0.as_slice()], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(db_err)?
                .map(|row| {
                    let json = row.map_err(db_err)?;
                    serde_json::from_str::<DeliveryRecord>(&json).map_err(|error| Error::Invalid {
                        what: "delivery record",
                        value: error.to_string(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            records
        };
        for source in sources {
            let removal = DeliveryRecord {
                cursor: StreamCursor { epoch, seq: next_stream_seq },
                kind: crate::DeliveryKind::Removed,
                source_cursor: Some(source.cursor),
                covenant_id: source.covenant_id,
                covenant_event_seq: source.covenant_event_seq,
                txid: source.txid,
                accepting_block: source.accepting_block,
                accepting_daa: source.accepting_daa,
                tx_index: source.tx_index,
                event_index: source.event_index,
                order_complete: source.order_complete,
                pending_id: source.pending_id,
                applications: source.applications,
            };
            insert_delivery(tx, &removal)?;
            removals.push(removal);
            next_stream_seq = next_stream_seq.checked_add(1).ok_or_else(|| Error::Invalid {
                what: "next stream sequence",
                value: u64::MAX.to_string(),
            })?;
        }
    }
    tx.execute(
        "UPDATE meta SET value = ?1 WHERE key = 'next_stream_seq'",
        [next_stream_seq.to_string()],
    )
    .map_err(db_err)?;
    Ok(removals)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DeliveryMigrationProgress {
    pub migrated: u64,
    pub remaining: u64,
    pub complete: bool,
    pub history_start_daa: u64,
    pub order_complete: bool,
}

impl Store {
    pub fn backfill_delivery_batch(
        &mut self,
        batch_size: u64,
    ) -> Result<DeliveryMigrationProgress> {
        if self.delivery_backfill_complete()? {
            return Ok(DeliveryMigrationProgress {
                migrated: 0,
                remaining: 0,
                complete: true,
                history_start_daa: self.delivery_history_start_daa()?,
                order_complete: self.delivery_history_order_complete()?,
            });
        }
        let tx = self.conn.transaction().map_err(db_err)?;
        let limit = batch_size.clamp(1, 10_000);
        let rows = {
            let mut statement = tx
                .prepare(
                    "SELECT covenant_id, seq, txid, accepting_block,
                            accepting_daa, accepting_blue_score, tx_index,
                            event_index
                     FROM covenant_events
                     WHERE delivery_stream_seq IS NULL
                     ORDER BY accepting_blue_score, accepting_daa, tx_index,
                              covenant_id, seq
                     LIMIT ?1",
                )
                .map_err(db_err)?;
            let rows = statement
                .query_map([limit], |row| {
                    Ok((
                        crate::CovenantId(row.get(0)?),
                        row.get::<_, u64>(1)?,
                        crate::TxId(row.get(2)?),
                        BlockHash(row.get(3)?),
                        row.get::<_, u64>(4)?,
                        row.get::<_, Option<u64>>(5)?,
                        row.get::<_, Option<u32>>(6)?,
                        row.get::<_, Option<u32>>(7)?,
                    ))
                })
                .map_err(db_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)?;
            rows
        };
        let epoch = transaction_stream_epoch(&tx)?;
        let mut next_stream_seq = transaction_next_stream_seq(&tx)?;
        for (covenant_id, covenant_event_seq, txid, accepting_block, accepting_daa, blue_score, tx_index, event_index) in &rows {
            let record = DeliveryRecord {
                cursor: StreamCursor { epoch, seq: next_stream_seq },
                kind: crate::DeliveryKind::Accepted,
                source_cursor: None,
                covenant_id: *covenant_id,
                covenant_event_seq: *covenant_event_seq,
                txid: *txid,
                accepting_block: *accepting_block,
                accepting_daa: *accepting_daa,
                tx_index: *tx_index,
                event_index: *event_index,
                order_complete: blue_score.is_some() && tx_index.is_some() && event_index.is_some(),
                pending_id: None,
                applications: vec![],
            };
            insert_delivery(&tx, &record)?;
            tx.execute(
                "UPDATE covenant_events SET delivery_stream_seq = ?1
                 WHERE covenant_id = ?2 AND seq = ?3
                   AND delivery_stream_seq IS NULL",
                params![next_stream_seq, covenant_id.0.as_slice(), covenant_event_seq],
            )
            .map_err(db_err)?;
            tx.execute(
                "INSERT INTO canonical_batches (
                    accepting_block, accepting_daa, first_stream_seq, last_stream_seq
                 ) VALUES (?1, ?2, ?3, ?3)
                 ON CONFLICT(accepting_block) DO UPDATE SET
                    first_stream_seq = CASE
                        WHEN canonical_batches.first_stream_seq IS NULL
                          OR excluded.first_stream_seq < canonical_batches.first_stream_seq
                        THEN excluded.first_stream_seq
                        ELSE canonical_batches.first_stream_seq
                    END,
                    last_stream_seq = CASE
                        WHEN canonical_batches.last_stream_seq IS NULL
                          OR excluded.last_stream_seq > canonical_batches.last_stream_seq
                        THEN excluded.last_stream_seq
                        ELSE canonical_batches.last_stream_seq
                    END",
                params![accepting_block.0.as_slice(), accepting_daa, next_stream_seq],
            )
            .map_err(db_err)?;
            next_stream_seq = next_stream_seq.checked_add(1).ok_or_else(|| Error::Invalid {
                what: "next stream sequence",
                value: u64::MAX.to_string(),
            })?;
        }
        tx.execute(
            "UPDATE meta SET value = ?1 WHERE key = 'next_stream_seq'",
            [next_stream_seq.to_string()],
        )
        .map_err(db_err)?;
        let remaining = tx
            .query_row(
                "SELECT COUNT(*) FROM covenant_events WHERE delivery_stream_seq IS NULL",
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(db_err)?;
        let history_start_daa = tx
            .query_row("SELECT MIN(accepting_daa) FROM covenant_events", [], |row| {
                row.get::<_, Option<u64>>(0)
            })
            .map_err(db_err)?
            .unwrap_or(0);
        let order_complete = if remaining == 0 {
            tx.query_row(
                "SELECT NOT EXISTS(
                    SELECT 1 FROM covenant_events
                    WHERE accepting_blue_score IS NULL OR tx_index IS NULL
                       OR event_index IS NULL
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(db_err)?
        } else {
            false
        };
        if remaining == 0 {
            for (key, value) in [
                ("delivery_backfill_complete", "1".to_owned()),
                ("delivery_history_start_daa", history_start_daa.to_string()),
                (
                    "delivery_history_order_complete",
                    if order_complete { "1" } else { "0" }.to_owned(),
                ),
            ] {
                tx.execute("UPDATE meta SET value = ?1 WHERE key = ?2", params![value, key])
                    .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(DeliveryMigrationProgress {
            migrated: rows.len() as u64,
            remaining,
            complete: remaining == 0,
            history_start_daa,
            order_complete,
        })
    }

    pub fn stream_epoch(&self) -> Result<StreamEpoch> {
        let raw = meta(&self.conn, "stream_epoch")?.ok_or_else(|| Error::Invalid {
            what: "stream epoch",
            value: "missing".to_owned(),
        })?;
        StreamEpoch::from_str(&raw)
    }

    pub fn delivery_high_water(&self) -> Result<StreamCursor> {
        let epoch = self.stream_epoch()?;
        let next = parse_meta_u64(&self.conn, "next_stream_seq")?;
        let seq = next.checked_sub(1).ok_or_else(|| Error::Invalid {
            what: "next stream sequence",
            value: next.to_string(),
        })?;
        Ok(StreamCursor { epoch, seq })
    }

    pub fn earliest_delivery_cursor(&self) -> Result<Option<StreamCursor>> {
        let epoch = self.stream_epoch()?;
        let seq = self
            .conn
            .query_row("SELECT MIN(stream_seq) FROM delivery_log", [], |row| {
                row.get::<_, Option<u64>>(0)
            })
            .map_err(db_err)?;
        Ok(seq.map(|seq| StreamCursor { epoch, seq }))
    }

    pub fn delivery_page(
        &self,
        after: Option<StreamCursor>,
        limit: u64,
    ) -> Result<Vec<DeliveryRecord>> {
        let epoch = self.stream_epoch()?;
        if let Some(cursor) = after {
            if cursor.epoch != epoch {
                return Err(Error::Invalid {
                    what: "stream cursor epoch",
                    value: cursor.epoch.to_string(),
                });
            }
        }
        let after_seq = after.map_or(0, |cursor| cursor.seq);
        let mut statement = self
            .conn
            .prepare(
                "SELECT data_json FROM delivery_log
                 WHERE stream_seq > ?1 ORDER BY stream_seq LIMIT ?2",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map(params![after_seq, limit.clamp(1, 1000)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(db_err)?
            .map(|row| {
                let json = row.map_err(db_err)?;
                serde_json::from_str(&json).map_err(|error| Error::Invalid {
                    what: "delivery record",
                    value: error.to_string(),
                })
            })
            .collect();
        rows
    }

    pub fn delivery_backfill_complete(&self) -> Result<bool> {
        Ok(meta(&self.conn, "delivery_backfill_complete")?.as_deref() == Some("1"))
    }

    pub fn delivery_history_start_daa(&self) -> Result<u64> {
        parse_meta_u64(&self.conn, "delivery_history_start_daa")
    }

    pub fn delivery_history_order_complete(&self) -> Result<bool> {
        Ok(meta(&self.conn, "delivery_history_order_complete")?.as_deref() == Some("1"))
    }
}

fn meta(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .optional()
    .map_err(db_err)
}

fn parse_meta_u64(conn: &Connection, key: &'static str) -> Result<u64> {
    let raw = meta(conn, key)?.ok_or_else(|| Error::Invalid {
        what: key,
        value: "missing".to_owned(),
    })?;
    raw.parse().map_err(|_| Error::Invalid {
        what: key,
        value: raw,
    })
}

fn db_err(error: rusqlite::Error) -> Error {
    Error::Invalid {
        what: "sqlite",
        value: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{AcceptedBlockBatch, AcceptedTransaction, EventKind, NewEvent, NewUtxo, Store};
    use crate::{
        ApplicationOutput, ApplicationPreprocess, BlockHash, CovenantId, DeliveryKind,
        DecodeFailure, DeliveryRecord, Network, Outpoint, StreamCursor, StreamEpoch, TxId,
    };

    fn accepted_batch(block: u8, txid: u8) -> AcceptedBlockBatch {
        let covenant_id = CovenantId([1; 32]);
        let txid = TxId([txid; 32]);
        let application = ApplicationOutput {
            output_index: 0,
            covenant_id,
            application_id: "counter".into(),
            artifact_id: [2; 32],
            actor_path: "Counter".into(),
            state_json: "{}".into(),
        };
        AcceptedBlockBatch {
            accepting_block: BlockHash([block; 32]),
            accepting_daa: u64::from(block) * 100,
            accepting_time_ms: u64::from(block) * 1_000,
            accepting_blue_score: u64::from(block) * 100,
            events: vec![NewEvent {
                covenant_id,
                kind: EventKind::Genesis,
                txid,
                tx_index: 0,
                event_index: 0,
                payload: Some(b"ARGI".to_vec()),
                lane_namespace: None,
            }],
            created_utxos: vec![NewUtxo {
                outpoint: Outpoint { txid, index: 0 },
                covenant_id,
                value: 7,
                spk_version: 0,
                spk_script: vec![0x51],
            }],
            spent_utxos: vec![],
            transactions: vec![AcceptedTransaction {
                txid,
                application: ApplicationPreprocess {
                    raw_envelope: Some(b"ARGI".to_vec()),
                    application_payload: Some(b"move".to_vec()),
                    outputs: vec![application],
                    failures: vec![DecodeFailure {
                        output_index: Some(0),
                        application_id: Some("counter".into()),
                        artifact_id: Some([2; 32]),
                        code: "fixture_warning".into(),
                        detail: "bounded fixture failure".into(),
                    }],
                },
            }],
        }
    }

    #[test]
    fn fresh_database_has_delivery_identity_and_empty_bounds() {
        let path =
            std::env::temp_dir().join(format!("kascov-delivery-fresh-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();

        assert_eq!(0, store.delivery_high_water().unwrap().seq);
        assert_eq!(None, store.earliest_delivery_cursor().unwrap());
        assert!(store.delivery_backfill_complete().unwrap());
        assert_eq!(0, store.delivery_history_start_daa().unwrap());
        assert!(store.delivery_history_order_complete().unwrap());
        let tables: u64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name IN (
                    'delivery_log', 'delivery_applications', 'canonical_batches'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(3, tables);
    }

    #[test]
    fn legacy_database_migration_is_idempotent_and_requires_backfill() {
        let path =
            std::env::temp_dir().join(format!("kascov-delivery-legacy-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch(crate::store::SCHEMA).unwrap();
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES ('legacy_marker', 'preserved')",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(Store::open(&path, Network::Testnet(10)).is_err());
        let first = Store::open_for_delivery_migration(&path, Network::Testnet(10)).unwrap();
        assert!(!first.delivery_backfill_complete().unwrap());
        let epoch = first.stream_epoch().unwrap();
        drop(first);

        let second = Store::open_for_delivery_migration(&path, Network::Testnet(10)).unwrap();
        assert_eq!(epoch, second.stream_epoch().unwrap());
        assert!(!second.delivery_backfill_complete().unwrap());
        let marker: String = second
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'legacy_marker'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!("preserved", marker);
    }

    #[test]
    fn delivery_bounds_and_pages_use_the_database_epoch() {
        let path =
            std::env::temp_dir().join(format!("kascov-delivery-page-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();
        let epoch = store.stream_epoch().unwrap();
        let record = DeliveryRecord {
            cursor: StreamCursor { epoch, seq: 1 },
            kind: DeliveryKind::Accepted,
            source_cursor: None,
            covenant_id: CovenantId([1; 32]),
            covenant_event_seq: 1,
            txid: TxId([2; 32]),
            accepting_block: BlockHash([3; 32]),
            accepting_daa: 4,
            tx_index: Some(5),
            event_index: Some(0),
            order_complete: true,
            pending_id: None,
            applications: vec![],
        };
        store
            .conn
            .execute(
                "INSERT INTO delivery_log (
                    stream_seq, kind, covenant_id, covenant_event_seq, txid,
                    accepting_block, accepting_daa, tx_index, event_index,
                    order_complete, data_json
                 ) VALUES (1, 'accepted', ?1, 1, ?2, ?3, 4, 5, 0, 1, ?4)",
                rusqlite::params![
                    record.covenant_id.0.as_slice(),
                    record.txid.0.as_slice(),
                    record.accepting_block.0.as_slice(),
                    serde_json::to_string(&record).unwrap(),
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE meta SET value = '2' WHERE key = 'next_stream_seq'",
                [],
            )
            .unwrap();

        assert_eq!(record.cursor, store.delivery_high_water().unwrap());
        assert_eq!(
            Some(record.cursor),
            store.earliest_delivery_cursor().unwrap()
        );
        assert_eq!(vec![record], store.delivery_page(None, 10).unwrap());
        assert!(store
            .delivery_page(
                Some(StreamCursor {
                    epoch: StreamEpoch([0xff; 16]),
                    seq: 0,
                }),
                10,
            )
            .is_err());
    }

    #[test]
    fn writer_lease_is_exclusive_and_readers_do_not_take_it() {
        let path = std::env::temp_dir().join(format!(
            "kascov-writer-lease-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let writer = Store::open(&path, Network::Testnet(10)).unwrap();
        assert!(Store::open(&path, Network::Testnet(10)).is_err());
        let reader = Store::open_read_only(&path, Network::Testnet(10)).unwrap();
        assert_eq!(writer.stream_epoch().unwrap(), reader.stream_epoch().unwrap());
        drop(reader);
        drop(writer);
        Store::open(&path, Network::Testnet(10)).unwrap();
    }

    #[test]
    fn accepted_apply_allocates_once_and_duplicate_returns_committed_rows() {
        let path = std::env::temp_dir().join(format!(
            "kascov-accepted-atomic-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let mut batch = accepted_batch(1, 9);

        let first = store.apply_accepted_block(&batch).unwrap();
        assert_eq!(1, first.deliveries.len());
        assert_eq!(1, first.deliveries[0].cursor.seq);
        assert_eq!(1, first.deliveries[0].applications.len());
        assert_eq!(1, store.delivery_high_water().unwrap().seq);
        assert!(store
            .current_application_output("counter", "Counter", &CovenantId([1; 32]))
            .unwrap()
            .is_some());
        assert_eq!(1, store.decode_failures(10).unwrap().len());

        batch.accepting_daa = 999;
        let duplicate = store.apply_accepted_block(&batch).unwrap();
        assert_eq!(first, duplicate);
        assert_eq!(1, store.delivery_high_water().unwrap().seq);

        let second = store.apply_accepted_block(&accepted_batch(2, 10)).unwrap();
        assert_eq!(2, second.deliveries[0].cursor.seq);
    }

    #[test]
    fn accepted_removed_reaccepted_is_monotonic_and_rewinds_application_state() {
        let path = std::env::temp_dir().join(format!(
            "kascov-removed-delivery-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let first = accepted_batch(1, 9);
        let mut second = accepted_batch(2, 10);
        second.spent_utxos.push((
            Outpoint { txid: TxId([9; 32]), index: 0 },
            TxId([10; 32]),
            vec![],
            0,
            0,
        ));
        second.transactions[0].application.outputs[0].state_json = "{\"value\":2}".into();

        let accepted_first = store.apply_accepted_block(&first).unwrap();
        let accepted_second = store.apply_accepted_block(&second).unwrap();
        assert_eq!(1, accepted_first.deliveries[0].cursor.seq);
        assert_eq!(2, accepted_second.deliveries[0].cursor.seq);
        assert_eq!(
            "{\"value\":2}",
            store
                .current_application_output("counter", "Counter", &CovenantId([1; 32]))
                .unwrap()
                .unwrap()
                .state_json
        );

        let removed = store
            .rollback_removed_blocks(&[BlockHash([2; 32]), BlockHash([2; 32])])
            .unwrap();
        assert_eq!(vec![BlockHash([2; 32])], removed.removed_blocks);
        assert_eq!(1, removed.deliveries.len());
        assert_eq!(DeliveryKind::Removed, removed.deliveries[0].kind);
        assert_eq!(3, removed.deliveries[0].cursor.seq);
        assert_eq!(Some(accepted_second.deliveries[0].cursor), removed.deliveries[0].source_cursor);
        assert_eq!(
            "{}",
            store
                .current_application_output("counter", "Counter", &CovenantId([1; 32]))
                .unwrap()
                .unwrap()
                .state_json
        );

        let repeated = store
            .rollback_removed_blocks(&[BlockHash([2; 32])])
            .unwrap();
        assert!(repeated.removed_blocks.is_empty());
        assert!(repeated.deliveries.is_empty());
        assert_eq!(3, store.delivery_high_water().unwrap().seq);

        let reaccepted = store.apply_accepted_block(&second).unwrap();
        assert_eq!(1, reaccepted.deliveries.len());
        assert_eq!(DeliveryKind::Accepted, reaccepted.deliveries[0].kind);
        assert_eq!(4, reaccepted.deliveries[0].cursor.seq);
        assert_eq!(
            vec![
                DeliveryKind::Accepted,
                DeliveryKind::Accepted,
                DeliveryKind::Removed,
                DeliveryKind::Accepted,
            ],
            store
                .delivery_page(None, 10)
                .unwrap()
                .into_iter()
                .map(|record| record.kind)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn removal_insert_failure_preserves_canonical_and_application_state() {
        let path = std::env::temp_dir().join(format!(
            "kascov-removal-failure-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let first = accepted_batch(1, 9);
        let mut second = accepted_batch(2, 10);
        second.spent_utxos.push((
            Outpoint { txid: TxId([9; 32]), index: 0 },
            TxId([10; 32]),
            vec![],
            0,
            0,
        ));
        second.transactions[0].application.outputs[0].state_json = "{\"value\":2}".into();
        store.apply_accepted_block(&first).unwrap();
        let accepted = store.apply_accepted_block(&second).unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER fail_removal BEFORE INSERT ON delivery_log
                 WHEN NEW.kind = 'removed'
                 BEGIN SELECT RAISE(ABORT, 'test removal failure'); END;",
            )
            .unwrap();

        assert!(store
            .rollback_removed_blocks(&[BlockHash([2; 32])])
            .is_err());
        assert_eq!(2, store.delivery_high_water().unwrap().seq);
        assert_eq!(2, store.delivery_page(None, 10).unwrap().len());
        assert_eq!(accepted, store.apply_accepted_block(&second).unwrap());
        assert_eq!(
            "{\"value\":2}",
            store
                .current_application_output("counter", "Counter", &CovenantId([1; 32]))
                .unwrap()
                .unwrap()
                .state_json
        );
    }

    #[test]
    fn delivery_insert_failure_rolls_back_every_accepted_write() {
        let path = std::env::temp_dir().join(format!(
            "kascov-accepted-failure-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER fail_delivery BEFORE INSERT ON delivery_log
                 BEGIN SELECT RAISE(ABORT, 'test delivery failure'); END;",
            )
            .unwrap();

        assert!(store.apply_accepted_block(&accepted_batch(1, 9)).is_err());
        assert!(!store.known_covenant(&CovenantId([1; 32])).unwrap());
        assert_eq!(0, store.delivery_high_water().unwrap().seq);
        assert!(store.cursor().unwrap().is_none());
        assert!(store.application_history("counter", "Counter", 10).unwrap().is_empty());
    }
}
