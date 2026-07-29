use std::str::FromStr;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::delivery::{DeliveryRecord, StreamCursor, StreamEpoch};
use crate::store::Store;
use crate::{BlockHash, CovenantId, Error, Result};

const MAX_PUBLIC_DELIVERY_PAGE: u64 = 1_001;
pub const MAX_DELIVERY_REPLAY_PAGE: u64 = 1_024;

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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryFilter {
    pub covenant_id: Option<CovenantId>,
    pub application_id: Option<String>,
    pub artifact_id: Option<[u8; 32]>,
    pub actor_path: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryCursorPosition {
    Valid,
    ForeignEpoch,
    Ahead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct DeliveryStreamInfo {
    pub earliest: Option<StreamCursor>,
    pub current: StreamCursor,
    pub history_start_daa: u64,
    pub order_complete: bool,
}

impl DeliveryStreamInfo {
    pub fn classify(&self, cursor: StreamCursor) -> DeliveryCursorPosition {
        if cursor.epoch != self.current.epoch {
            DeliveryCursorPosition::ForeignEpoch
        } else if cursor.seq > self.current.seq {
            DeliveryCursorPosition::Ahead
        } else {
            DeliveryCursorPosition::Valid
        }
    }
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
        self.delivery_page_filtered(after, limit, &DeliveryFilter::default())
    }

    pub fn delivery_replay_page(
        &self,
        after: Option<StreamCursor>,
        limit: u64,
    ) -> Result<Vec<DeliveryRecord>> {
        self.delivery_page_filtered_with_cap(
            after,
            limit,
            &DeliveryFilter::default(),
            MAX_DELIVERY_REPLAY_PAGE,
        )
    }

    pub fn delivery_page_filtered(
        &self,
        after: Option<StreamCursor>,
        limit: u64,
        filter: &DeliveryFilter,
    ) -> Result<Vec<DeliveryRecord>> {
        self.delivery_page_filtered_with_cap(after, limit, filter, MAX_PUBLIC_DELIVERY_PAGE)
    }

    fn delivery_page_filtered_with_cap(
        &self,
        after: Option<StreamCursor>,
        limit: u64,
        filter: &DeliveryFilter,
        max_limit: u64,
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
        let covenant_id = filter.covenant_id.map(|id| id.0.to_vec());
        let artifact_id = filter.artifact_id.map(|id| id.to_vec());
        let mut statement = self
            .conn
            .prepare(
                "SELECT delivery_log.data_json FROM delivery_log
                 WHERE delivery_log.stream_seq > ?1
                   AND (?2 IS NULL OR delivery_log.covenant_id = ?2)
                   AND (
                       (?3 IS NULL AND ?4 IS NULL AND ?5 IS NULL)
                       OR EXISTS (
                           SELECT 1 FROM delivery_applications
                           WHERE delivery_applications.stream_seq = delivery_log.stream_seq
                             AND (?3 IS NULL OR delivery_applications.application_id = ?3)
                             AND (?4 IS NULL OR delivery_applications.artifact_id = ?4)
                             AND (?5 IS NULL OR delivery_applications.actor_path = ?5)
                       )
                   )
                 ORDER BY delivery_log.stream_seq LIMIT ?6",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map(
                params![
                    after_seq,
                    covenant_id,
                    filter.application_id.as_deref(),
                    artifact_id,
                    filter.actor_path.as_deref(),
                    limit.clamp(1, max_limit),
                ],
                |row| row.get::<_, String>(0),
            )
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

    pub fn delivery_stream_info(&self) -> Result<DeliveryStreamInfo> {
        let (epoch, next, earliest, history_start_daa, order_complete) = self
            .conn
            .query_row(
                "SELECT
                    (SELECT value FROM meta WHERE key = 'stream_epoch'),
                    (SELECT value FROM meta WHERE key = 'next_stream_seq'),
                    (SELECT MIN(stream_seq) FROM delivery_log),
                    (SELECT value FROM meta WHERE key = 'delivery_history_start_daa'),
                    (SELECT value FROM meta WHERE key = 'delivery_history_order_complete')",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<u64>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(db_err)?;
        let epoch = StreamEpoch::from_str(&epoch)?;
        let next = next.parse::<u64>().map_err(|_| Error::Invalid {
            what: "next_stream_seq",
            value: next,
        })?;
        let current = StreamCursor {
            epoch,
            seq: next.checked_sub(1).ok_or_else(|| Error::Invalid {
                what: "next stream sequence",
                value: next.to_string(),
            })?,
        };
        let history_start_daa = history_start_daa.parse().map_err(|_| Error::Invalid {
            what: "delivery_history_start_daa",
            value: history_start_daa,
        })?;
        let order_complete = match order_complete.as_str() {
            "0" => false,
            "1" => true,
            _ => {
                return Err(Error::Invalid {
                    what: "delivery_history_order_complete",
                    value: order_complete,
                })
            }
        };
        Ok(DeliveryStreamInfo {
            earliest: earliest.map(|seq| StreamCursor { epoch, seq }),
            current,
            history_start_daa,
            order_complete,
        })
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
    use super::{DeliveryCursorPosition, DeliveryFilter};
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
                transaction: crate::Transaction {
                    txid,
                    version: 1,
                    inputs: vec![],
                    outputs: vec![],
                    payload: b"ARGI".to_vec(),
                },
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
    fn replay_page_honors_the_full_internal_candidate() {
        let path = std::env::temp_dir().join(format!(
            "kascov-delivery-replay-limit-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let mut batch = accepted_batch(1, 9);
        let event = &batch.events[0];
        let covenant_id = event.covenant_id;
        let kind = event.kind;
        let txid = event.txid;
        let tx_index = event.tx_index;
        let payload = event.payload.clone();
        let lane_namespace = event.lane_namespace.clone();
        batch.events = (0..1_100)
            .map(|event_index| NewEvent {
                covenant_id,
                kind,
                txid,
                tx_index,
                event_index,
                payload: payload.clone(),
                lane_namespace: lane_namespace.clone(),
            })
            .collect();
        store.apply_accepted_block(&batch).unwrap();

        assert_eq!(1_001, store.delivery_page(None, u64::MAX).unwrap().len());
        assert_eq!(
            1_024,
            store.delivery_replay_page(None, 1_024).unwrap().len()
        );
    }

    #[test]
    fn delivery_pages_filter_without_changing_the_global_cursor() {
        let path = std::env::temp_dir().join(format!(
            "kascov-delivery-filter-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let first = store.apply_accepted_block(&accepted_batch(1, 2)).unwrap();
        let second = store.apply_accepted_block(&accepted_batch(2, 3)).unwrap();
        let first_cursor = first.deliveries[0].cursor;
        let second_cursor = second.deliveries[0].cursor;

        for filter in [
            DeliveryFilter {
                covenant_id: Some(CovenantId([1; 32])),
                ..Default::default()
            },
            DeliveryFilter {
                application_id: Some("counter".into()),
                ..Default::default()
            },
            DeliveryFilter {
                artifact_id: Some([2; 32]),
                ..Default::default()
            },
            DeliveryFilter {
                actor_path: Some("Counter".into()),
                ..Default::default()
            },
        ] {
            let page = store
                .delivery_page_filtered(Some(first_cursor), 10, &filter)
                .unwrap();
            assert_eq!(vec![second_cursor], page.iter().map(|row| row.cursor).collect::<Vec<_>>());
        }
        assert!(store
            .delivery_page_filtered(
                None,
                10,
                &DeliveryFilter {
                    application_id: Some("missing".into()),
                    ..Default::default()
                },
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn delivery_stream_info_classifies_cursor_bounds() {
        let path = std::env::temp_dir().join(format!(
            "kascov-delivery-info-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let empty = store.delivery_stream_info().unwrap();
        assert_eq!(None, empty.earliest);
        assert_eq!(0, empty.current.seq);

        let committed = store.apply_accepted_block(&accepted_batch(1, 2)).unwrap();
        let info = store.delivery_stream_info().unwrap();
        assert_eq!(Some(committed.deliveries[0].cursor), info.earliest);
        assert_eq!(committed.deliveries[0].cursor, info.current);
        assert_eq!(DeliveryCursorPosition::Valid, info.classify(info.current));
        assert_eq!(
            DeliveryCursorPosition::Ahead,
            info.classify(StreamCursor {
                epoch: info.current.epoch,
                seq: info.current.seq + 1,
            })
        );
        assert_eq!(
            DeliveryCursorPosition::ForeignEpoch,
            info.classify(StreamCursor {
                epoch: StreamEpoch([0xff; 16]),
                seq: 0,
            })
        );
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
        let reader = Store::open_reader(&path, Network::Testnet(10)).unwrap();
        reader.reader_is_healthy().unwrap();
        assert!(reader.delete_subscription(1).is_err());
        assert_eq!(writer.stream_epoch().unwrap(), reader.stream_epoch().unwrap());
        drop(reader);
        drop(writer);
        Store::open(&path, Network::Testnet(10)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn writer_lease_uses_the_canonical_database_identity() {
        let base = std::env::temp_dir().join(format!(
            "kascov-writer-alias-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let database = base.join("database.db");
        let alias = base.join("alias.db");
        let _ = std::fs::remove_file(&database);
        let _ = std::fs::remove_file(&alias);
        let writer = Store::open(&database, Network::Testnet(10)).unwrap();
        std::os::unix::fs::symlink(&database, &alias).unwrap();

        assert!(Store::open(&alias, Network::Testnet(10)).is_err());
        let writer_lock = base.join("database.db.writer.lock");
        assert!(Store::backup_database(&alias, Network::Testnet(10), &writer_lock).is_err());

        drop(writer);
    }

    #[test]
    fn online_backup_uses_a_reader_while_the_writer_lease_is_held() {
        let base = std::env::temp_dir().join(format!(
            "kascov-online-backup-{}",
            std::process::id()
        ));
        let source = base.with_extension("source.db");
        let backup = base.with_extension("backup.db");
        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_file(&backup);
        let writer = Store::open(&source, Network::Testnet(10)).unwrap();

        assert!(Store::backup_database(&source, Network::Testnet(10), &source).is_err());
        let wal = std::path::PathBuf::from(format!("{}-wal", source.display()));
        let shm = std::path::PathBuf::from(format!("{}-shm", source.display()));
        assert!(Store::backup_database(&source, Network::Testnet(10), &wal).is_err());
        assert!(Store::backup_database(&source, Network::Testnet(10), &shm).is_err());
        let hard_link = base.with_extension("hard-link.db");
        let _ = std::fs::remove_file(&hard_link);
        std::fs::hard_link(&source, &hard_link).unwrap();
        assert!(Store::backup_database(&source, Network::Testnet(10), &hard_link).is_err());
        std::fs::remove_file(&hard_link).unwrap();

        Store::backup_database(&source, Network::Testnet(10), &backup).unwrap();

        let restored = Store::open_reader(&backup, Network::Testnet(10)).unwrap();
        assert_eq!(
            writer.delivery_high_water().unwrap(),
            restored.delivery_high_water().unwrap()
        );
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
