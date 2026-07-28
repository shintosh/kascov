use std::collections::BTreeSet;

use rusqlite::{params, OptionalExtension};
use serde::Serialize;

use crate::store::Store;
use crate::{CovenantId, Error, Result, StreamCursor, StreamEpoch};

const INITIALIZED_META: &str = "optional_projection_initialized";
const CURSOR_META: &str = "optional_projection_cursor";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionStatus {
    pub cursor: StreamCursor,
    pub high_water: StreamCursor,
    pub lag: u64,
    pub queued: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionDrain {
    pub processed: u64,
    pub deferred: bool,
    pub status: ProjectionStatus,
}

pub(crate) fn initialize(conn: &rusqlite::Connection) -> Result<()> {
    let initialized = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [INITIALIZED_META],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(db_err)?
        .is_some();
    if initialized {
        return Ok(());
    }
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let queued = tx
        .query_row("SELECT COUNT(*) FROM optional_projection_work", [], |row| {
            row.get::<_, u64>(0)
        })
        .map_err(db_err)?;
    if queued == 0 {
        let high_water = next_stream_seq(&tx)?.saturating_sub(1);
        tx.execute(
            "UPDATE meta SET value = ?1 WHERE key = ?2",
            params![high_water.to_string(), CURSOR_META],
        )
        .map_err(db_err)?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, '1')",
        [INITIALIZED_META],
    )
    .map_err(db_err)?;
    tx.commit().map_err(db_err)
}

pub(crate) fn enqueue(
    tx: &rusqlite::Transaction<'_>,
    stream_seq: u64,
    covenant_ids: &[CovenantId],
) -> Result<()> {
    let covenant_ids_json =
        serde_json::to_string(covenant_ids).map_err(|error| Error::Invalid {
            what: "projection covenant ids",
            value: error.to_string(),
        })?;
    tx.execute(
        "INSERT OR REPLACE INTO optional_projection_work (
            delivery_stream_seq, covenant_ids_json
         ) VALUES (?1, ?2)",
        params![stream_seq, covenant_ids_json],
    )
    .map_err(db_err)?;
    Ok(())
}

impl Store {
    pub fn optional_projection_status(&self) -> Result<ProjectionStatus> {
        status(&self.conn)
    }

    pub fn drain_optional_projection_chunk(
        &mut self,
        accepted_pending: bool,
        limit: u64,
    ) -> Result<ProjectionDrain> {
        if accepted_pending {
            return Ok(ProjectionDrain {
                processed: 0,
                deferred: true,
                status: status(&self.conn)?,
            });
        }
        let tx = self.conn.transaction().map_err(db_err)?;
        let cursor = projection_cursor(&tx)?;
        let rows = {
            let mut statement = tx
                .prepare(
                    "SELECT delivery_stream_seq, covenant_ids_json
                     FROM optional_projection_work
                     WHERE delivery_stream_seq > ?1
                     ORDER BY delivery_stream_seq LIMIT ?2",
                )
                .map_err(db_err)?;
            let rows = statement
                .query_map(params![cursor, limit.clamp(1, 256)], |row| {
                    Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(db_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)?;
            rows
        };
        let mut touched = BTreeSet::new();
        for (_, json) in &rows {
            let ids: Vec<CovenantId> =
                serde_json::from_str(json).map_err(|error| Error::Invalid {
                    what: "projection covenant ids",
                    value: error.to_string(),
                })?;
            touched.extend(ids.into_iter().map(|id| id.0));
        }
        if let Some((last, _)) = rows.last() {
            let mut tokens = BTreeSet::new();
            let mut minters = BTreeSet::new();
            for id in touched {
                if crate::tokens::has_token_evidence(&tx, &id)?
                    || crate::tokens::is_token(&tx, &id)?
                {
                    tokens.insert(id);
                }
                if crate::tokens::has_minter_evidence(&tx, &id)?
                    || crate::tokens::is_minter(&tx, &id)?
                {
                    minters.insert(id);
                }
            }
            crate::tokens::rederive_affected(&tx, &minters, &tokens)?;
            tx.execute(
                "UPDATE meta SET value = ?1 WHERE key = ?2",
                params![last.to_string(), CURSOR_META],
            )
            .map_err(db_err)?;
            tx.execute(
                "DELETE FROM optional_projection_work WHERE delivery_stream_seq <= ?1",
                [last],
            )
            .map_err(db_err)?;
        }
        tx.commit().map_err(db_err)?;
        Ok(ProjectionDrain {
            processed: rows.len() as u64,
            deferred: false,
            status: status(&self.conn)?,
        })
    }
}

fn status(conn: &rusqlite::Connection) -> Result<ProjectionStatus> {
    let epoch = stream_epoch(conn)?;
    let cursor = projection_cursor(conn)?;
    let high_water = next_stream_seq(conn)?.saturating_sub(1);
    let queued = conn
        .query_row(
            "SELECT COUNT(*) FROM optional_projection_work
             WHERE delivery_stream_seq > ?1",
            [cursor],
            |row| row.get::<_, u64>(0),
        )
        .map_err(db_err)?;
    Ok(ProjectionStatus {
        cursor: StreamCursor { epoch, seq: cursor },
        high_water: StreamCursor {
            epoch,
            seq: high_water,
        },
        lag: high_water.saturating_sub(cursor),
        queued,
    })
}

fn stream_epoch(conn: &rusqlite::Connection) -> Result<StreamEpoch> {
    let raw = meta(conn, "stream_epoch")?;
    raw.parse()
}

fn projection_cursor(conn: &rusqlite::Connection) -> Result<u64> {
    parse_meta_u64(conn, CURSOR_META)
}

fn next_stream_seq(conn: &rusqlite::Connection) -> Result<u64> {
    parse_meta_u64(conn, "next_stream_seq")
}

fn parse_meta_u64(conn: &rusqlite::Connection, key: &'static str) -> Result<u64> {
    let raw = meta(conn, key)?;
    raw.parse().map_err(|_| Error::Invalid {
        what: key,
        value: raw,
    })
}

fn meta(conn: &rusqlite::Connection, key: &'static str) -> Result<String> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .map_err(db_err)
}

fn db_err(error: rusqlite::Error) -> Error {
    Error::Invalid {
        what: "sqlite",
        value: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{AcceptedBlockBatch, EventKind, NewEvent, Store};
    use crate::{BlockHash, CovenantId, Network, TxId};

    fn batch(block: u8) -> AcceptedBlockBatch {
        let mut batch = AcceptedBlockBatch::empty(BlockHash([block; 32]));
        batch.accepting_daa = u64::from(block) * 100;
        batch.accepting_blue_score = u64::from(block) * 100;
        batch.events.push(NewEvent {
            covenant_id: CovenantId([block; 32]),
            kind: EventKind::Genesis,
            txid: TxId([block; 32]),
            tx_index: 0,
            event_index: 0,
            payload: None,
            lane_namespace: None,
        });
        batch
    }

    #[test]
    fn work_is_durable_bounded_and_defers_to_accepted_reconciliation() {
        let path = std::env::temp_dir().join(format!(
            "kascov-projection-durable-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        for block in 1..=3 {
            store.apply_accepted_block(&batch(block)).unwrap();
        }
        let queued = store.optional_projection_status().unwrap();
        assert_eq!(0, queued.cursor.seq);
        assert_eq!(3, queued.high_water.seq);
        assert_eq!(3, queued.lag);
        assert_eq!(3, queued.queued);

        let deferred = store.drain_optional_projection_chunk(true, 1).unwrap();
        assert!(deferred.deferred);
        assert_eq!(0, deferred.processed);
        assert_eq!(0, deferred.status.cursor.seq);

        let first = store.drain_optional_projection_chunk(false, 1).unwrap();
        assert!(!first.deferred);
        assert_eq!(1, first.processed);
        assert_eq!(1, first.status.cursor.seq);
        assert_eq!(2, first.status.queued);
        drop(store);

        let mut resumed = Store::open(&path, Network::Testnet(10)).unwrap();
        assert_eq!(1, resumed.optional_projection_status().unwrap().cursor.seq);
        let finished = resumed.drain_optional_projection_chunk(false, 2).unwrap();
        assert_eq!(2, finished.processed);
        assert_eq!(3, finished.status.cursor.seq);
        assert_eq!(0, finished.status.lag);
        assert_eq!(0, finished.status.queued);
    }

    #[test]
    fn rollback_enqueues_new_work_after_the_accepted_cursor() {
        let path =
            std::env::temp_dir().join(format!("kascov-projection-reorg-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        store.apply_accepted_block(&batch(1)).unwrap();
        store.drain_optional_projection_chunk(false, 10).unwrap();
        let removed = store
            .rollback_removed_blocks(&[BlockHash([1; 32])])
            .unwrap();
        assert_eq!(2, removed.deliveries[0].cursor.seq);
        let status = store.optional_projection_status().unwrap();
        assert_eq!(1, status.cursor.seq);
        assert_eq!(2, status.high_water.seq);
        assert_eq!(1, status.queued);

        let drained = store.drain_optional_projection_chunk(false, 10).unwrap();
        assert_eq!(1, drained.processed);
        assert_eq!(2, drained.status.cursor.seq);
        assert_eq!(0, drained.status.lag);
    }
}
