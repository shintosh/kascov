use std::str::FromStr;

use rusqlite::{params, Connection, OptionalExtension};

use crate::delivery::{DeliveryRecord, StreamCursor, StreamEpoch};
use crate::store::Store;
use crate::{Error, Result};

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

impl Store {
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
    use crate::store::Store;
    use crate::{
        BlockHash, CovenantId, DeliveryKind, DeliveryRecord, Network, StreamCursor, StreamEpoch,
        TxId,
    };

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

        let first = Store::open(&path, Network::Testnet(10)).unwrap();
        assert!(!first.delivery_backfill_complete().unwrap());
        let epoch = first.stream_epoch().unwrap();
        drop(first);

        let second = Store::open(&path, Network::Testnet(10)).unwrap();
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
}
