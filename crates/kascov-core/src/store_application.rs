use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::application::{ApplicationOutput, DecodeFailure};
use crate::store::{AcceptedBlockBatch, Store};
use crate::{BlockHash, CovenantId, Error, Result, TxId};

const APPLICATION_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS application_envelopes (
    txid BLOB PRIMARY KEY,
    accepting_block BLOB NOT NULL,
    accepting_daa INTEGER NOT NULL,
    raw_envelope BLOB NOT NULL,
    application_payload BLOB NOT NULL,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS application_envelope_by_block
    ON application_envelopes(accepting_block);

CREATE TABLE IF NOT EXISTS application_outputs (
    txid BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    covenant_id BLOB NOT NULL,
    application_id TEXT NOT NULL,
    artifact_id BLOB NOT NULL,
    actor_path TEXT NOT NULL,
    state_json TEXT NOT NULL,
    created_block BLOB NOT NULL,
    created_daa INTEGER NOT NULL,
    spent_block BLOB,
    PRIMARY KEY (txid, output_index)
);
CREATE INDEX IF NOT EXISTS application_output_by_current_actor
    ON application_outputs(application_id, actor_path, covenant_id)
    WHERE spent_block IS NULL;
CREATE INDEX IF NOT EXISTS application_output_by_covenant
    ON application_outputs(covenant_id, created_daa);
CREATE INDEX IF NOT EXISTS application_output_by_created
    ON application_outputs(created_block);
CREATE INDEX IF NOT EXISTS application_output_by_spent
    ON application_outputs(spent_block)
    WHERE spent_block IS NOT NULL;

CREATE TABLE IF NOT EXISTS application_decode_failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    txid BLOB NOT NULL,
    accepting_block BLOB NOT NULL,
    accepting_daa INTEGER NOT NULL,
    output_index INTEGER,
    application_id TEXT,
    artifact_id BLOB,
    code TEXT NOT NULL,
    detail TEXT NOT NULL,
    repaired_stream_seq INTEGER
);
CREATE INDEX IF NOT EXISTS application_failure_unrepaired
    ON application_decode_failures(accepting_daa, id)
    WHERE repaired_stream_seq IS NULL;
CREATE INDEX IF NOT EXISTS application_failure_by_block
    ON application_decode_failures(accepting_block);

CREATE TABLE IF NOT EXISTS optional_projection_work (
    delivery_stream_seq INTEGER PRIMARY KEY,
    covenant_ids_json TEXT NOT NULL
);
";

pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(APPLICATION_SCHEMA).map_err(db_err)
}

pub(crate) fn apply_accepted(
    tx: &rusqlite::Transaction<'_>,
    batch: &AcceptedBlockBatch,
) -> Result<()> {
    for accepted in &batch.transactions {
        let application = &accepted.application;
        if let Some(raw_envelope) = &application.raw_envelope {
            let status = if application.failures.is_empty() {
                "valid"
            } else if application.outputs.is_empty() {
                "failed"
            } else {
                "partial"
            };
            tx.execute(
                "INSERT INTO application_envelopes (
                    txid, accepting_block, accepting_daa, raw_envelope,
                    application_payload, status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    accepted.txid.0.as_slice(),
                    batch.accepting_block.0.as_slice(),
                    batch.accepting_daa,
                    raw_envelope,
                    application.application_payload.as_deref().unwrap_or_default(),
                    status,
                ],
            )
            .map_err(db_err)?;
        }
        for output in &application.outputs {
            tx.execute(
                "INSERT INTO application_outputs (
                    txid, output_index, covenant_id, application_id,
                    artifact_id, actor_path, state_json, created_block,
                    created_daa, spent_block
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
                params![
                    accepted.txid.0.as_slice(),
                    output.output_index,
                    output.covenant_id.0.as_slice(),
                    output.application_id,
                    output.artifact_id.as_slice(),
                    output.actor_path,
                    output.state_json,
                    batch.accepting_block.0.as_slice(),
                    batch.accepting_daa,
                ],
            )
            .map_err(db_err)?;
        }
        for failure in &application.failures {
            tx.execute(
                "INSERT INTO application_decode_failures (
                    txid, accepting_block, accepting_daa, output_index,
                    application_id, artifact_id, code, detail
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    accepted.txid.0.as_slice(),
                    batch.accepting_block.0.as_slice(),
                    batch.accepting_daa,
                    failure.output_index,
                    failure.application_id,
                    failure.artifact_id.as_ref().map(|id| id.as_slice()),
                    failure.code,
                    failure.detail,
                ],
            )
            .map_err(db_err)?;
        }
    }
    for (outpoint, _, _, _, _) in &batch.spent_utxos {
        tx.execute(
            "UPDATE application_outputs SET spent_block = ?1
             WHERE txid = ?2 AND output_index = ?3 AND spent_block IS NULL",
            params![
                batch.accepting_block.0.as_slice(),
                outpoint.txid.0.as_slice(),
                outpoint.index,
            ],
        )
        .map_err(db_err)?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApplicationHistoryRow {
    pub txid: TxId,
    pub output: ApplicationOutput,
    pub created_block: BlockHash,
    pub created_daa: u64,
    pub spent_block: Option<BlockHash>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StoredDecodeFailure {
    pub id: u64,
    pub txid: TxId,
    pub accepting_block: BlockHash,
    pub accepting_daa: u64,
    pub failure: DecodeFailure,
    pub repaired_stream_seq: Option<u64>,
}

impl Store {
    pub fn current_application_output(
        &self,
        application_id: &str,
        actor_path: &str,
        covenant_id: &CovenantId,
    ) -> Result<Option<ApplicationOutput>> {
        self.conn
            .query_row(
                "SELECT output_index, covenant_id, application_id, artifact_id,
                        actor_path, state_json
                 FROM application_outputs
                 WHERE application_id = ?1 AND actor_path = ?2
                   AND covenant_id = ?3 AND spent_block IS NULL",
                params![application_id, actor_path, covenant_id.0.as_slice()],
                application_output_from_row,
            )
            .optional()
            .map_err(db_err)
    }

    pub fn application_history(
        &self,
        application_id: &str,
        actor_path: &str,
        limit: u64,
    ) -> Result<Vec<ApplicationHistoryRow>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT txid, output_index, covenant_id, application_id,
                        artifact_id, actor_path, state_json, created_block,
                        created_daa, spent_block
                 FROM application_outputs
                 WHERE application_id = ?1 AND actor_path = ?2
                 ORDER BY created_daa DESC, txid DESC, output_index DESC
                 LIMIT ?3",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map(
                params![application_id, actor_path, limit.clamp(1, 1000)],
                |row| {
                    Ok(ApplicationHistoryRow {
                        txid: TxId(row.get(0)?),
                        output: ApplicationOutput {
                            output_index: row.get(1)?,
                            covenant_id: CovenantId(row.get(2)?),
                            application_id: row.get(3)?,
                            artifact_id: row.get(4)?,
                            actor_path: row.get(5)?,
                            state_json: row.get(6)?,
                        },
                        created_block: BlockHash(row.get(7)?),
                        created_daa: row.get(8)?,
                        spent_block: row.get::<_, Option<[u8; 32]>>(9)?.map(BlockHash),
                    })
                },
            )
            .map_err(db_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err);
        rows
    }

    pub fn decode_failures(&self, limit: u64) -> Result<Vec<StoredDecodeFailure>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, txid, accepting_block, accepting_daa, output_index,
                        application_id, artifact_id, code, detail,
                        repaired_stream_seq
                 FROM application_decode_failures
                 ORDER BY accepting_daa, id LIMIT ?1",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map([limit.clamp(1, 1000)], |row| {
                Ok(StoredDecodeFailure {
                    id: row.get(0)?,
                    txid: TxId(row.get(1)?),
                    accepting_block: BlockHash(row.get(2)?),
                    accepting_daa: row.get(3)?,
                    failure: DecodeFailure {
                        output_index: row.get(4)?,
                        application_id: row.get(5)?,
                        artifact_id: row.get(6)?,
                        code: row.get(7)?,
                        detail: row.get(8)?,
                    },
                    repaired_stream_seq: row.get(9)?,
                })
            })
            .map_err(db_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err);
        rows
    }
}

fn application_output_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApplicationOutput> {
    Ok(ApplicationOutput {
        output_index: row.get(0)?,
        covenant_id: CovenantId(row.get(1)?),
        application_id: row.get(2)?,
        artifact_id: row.get(3)?,
        actor_path: row.get(4)?,
        state_json: row.get(5)?,
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
    use crate::{BlockHash, CovenantId, Network, TxId};

    #[test]
    fn fresh_database_has_empty_application_reads() {
        let path = std::env::temp_dir().join(format!(
            "kascov-application-fresh-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();

        assert!(store
            .current_application_output("counter", "root", &CovenantId([1; 32]))
            .unwrap()
            .is_none());
        assert!(store
            .application_history("counter", "root", 10)
            .unwrap()
            .is_empty());
        assert!(store.decode_failures(10).unwrap().is_empty());
        let tables: u64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name IN (
                    'application_envelopes', 'application_outputs',
                    'application_decode_failures', 'optional_projection_work'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(4, tables);
    }

    #[test]
    fn application_point_history_and_failure_reads_round_trip() {
        let path = std::env::temp_dir().join(format!(
            "kascov-application-reads-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Store::open(&path, Network::Testnet(10)).unwrap();
        let covenant_id = CovenantId([1; 32]);
        store
            .conn
            .execute(
                "INSERT INTO application_outputs (
                    txid, output_index, covenant_id, application_id, artifact_id,
                    actor_path, state_json, created_block, created_daa
                 ) VALUES (?1, 2, ?2, 'counter', ?3, 'root', '{\"value\":7}', ?4, 9)",
                rusqlite::params![
                    TxId([2; 32]).0.as_slice(),
                    covenant_id.0.as_slice(),
                    [3u8; 32].as_slice(),
                    BlockHash([4; 32]).0.as_slice(),
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO application_decode_failures (
                    txid, accepting_block, accepting_daa, output_index,
                    application_id, artifact_id, code, detail
                 ) VALUES (?1, ?2, 9, 2, 'counter', ?3, 'state', 'invalid')",
                rusqlite::params![
                    TxId([2; 32]).0.as_slice(),
                    BlockHash([4; 32]).0.as_slice(),
                    [3u8; 32].as_slice(),
                ],
            )
            .unwrap();

        let current = store
            .current_application_output("counter", "root", &covenant_id)
            .unwrap()
            .unwrap();
        assert_eq!(2, current.output_index);
        assert_eq!("{\"value\":7}", current.state_json);
        assert_eq!(
            1,
            store
                .application_history("counter", "root", 10)
                .unwrap()
                .len()
        );
        let failures = store.decode_failures(10).unwrap();
        assert_eq!(1, failures.len());
        assert_eq!("state", failures[0].failure.code);
    }
}
