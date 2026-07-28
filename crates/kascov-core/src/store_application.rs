use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::application::{ApplicationOutput, DecodeFailure};
use crate::store::{AcceptedBlockBatch, Store};
use crate::{
    ApplicationDecoder, BlockHash, CovenantId, DeliveryKind, DeliveryRecord, Error, Result,
    StreamCursor, TxId,
};

const APPLICATION_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS application_envelopes (
    txid BLOB PRIMARY KEY,
    accepting_block BLOB NOT NULL,
    accepting_daa INTEGER NOT NULL,
    raw_envelope BLOB NOT NULL,
    application_payload BLOB NOT NULL,
    transaction_json TEXT,
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
    conn.execute_batch(APPLICATION_SCHEMA).map_err(db_err)?;
    match conn.execute(
        "ALTER TABLE application_envelopes ADD COLUMN transaction_json TEXT",
        [],
    ) {
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
                    application_payload, transaction_json, status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    accepted.txid.0.as_slice(),
                    batch.accepting_block.0.as_slice(),
                    batch.accepting_daa,
                    raw_envelope,
                    application.application_payload.as_deref().unwrap_or_default(),
                    serde_json::to_string(&accepted.transaction).map_err(|error| Error::Invalid {
                        what: "accepted application transaction",
                        value: error.to_string(),
                    })?,
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

pub(crate) fn rollback_removed(
    tx: &rusqlite::Transaction<'_>,
    removed: &[BlockHash],
) -> Result<()> {
    for block in removed {
        let hash = block.0.as_slice();
        tx.execute(
            "UPDATE application_outputs SET spent_block = NULL WHERE spent_block = ?1",
            [hash],
        )
        .map_err(db_err)?;
        tx.execute("DELETE FROM application_outputs WHERE created_block = ?1", [hash])
            .map_err(db_err)?;
        tx.execute(
            "DELETE FROM application_decode_failures WHERE accepting_block = ?1",
            [hash],
        )
        .map_err(db_err)?;
        tx.execute("DELETE FROM application_envelopes WHERE accepting_block = ?1", [hash])
            .map_err(db_err)?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApplicationHistoryRow {
    pub id: u64,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ApplicationRepairResult {
    pub transactions_scanned: u64,
    pub outputs_repaired: u64,
    pub failures_repaired: u64,
    pub deliveries_appended: u64,
    pub failures_remaining: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApplicationTransactionResult {
    pub txid: TxId,
    pub accepting_block: BlockHash,
    pub accepting_daa: u64,
    pub status: String,
    pub application_payload: Vec<u8>,
    pub outputs: Vec<ApplicationOutput>,
    pub failures: Vec<StoredDecodeFailure>,
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
                "SELECT rowid, txid, output_index, covenant_id, application_id,
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
                        id: row.get(0)?,
                        txid: TxId(row.get(1)?),
                        output: ApplicationOutput {
                            output_index: row.get(2)?,
                            covenant_id: CovenantId(row.get(3)?),
                            application_id: row.get(4)?,
                            artifact_id: row.get(5)?,
                            actor_path: row.get(6)?,
                            state_json: row.get(7)?,
                        },
                        created_block: BlockHash(row.get(8)?),
                        created_daa: row.get(9)?,
                        spent_block: row.get::<_, Option<[u8; 32]>>(10)?.map(BlockHash),
                    })
                },
            )
            .map_err(db_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err);
        rows
    }

    pub fn application_outputs_page(
        &self,
        application_id: &str,
        actor_path: Option<&str>,
        covenant_id: Option<&CovenantId>,
        current_only: bool,
        after_id: u64,
        limit: u64,
    ) -> Result<Vec<ApplicationHistoryRow>> {
        let covenant = covenant_id.map(|id| id.0.to_vec());
        let mut statement = self
            .conn
            .prepare(
                "SELECT rowid, txid, output_index, covenant_id, application_id,
                        artifact_id, actor_path, state_json, created_block,
                        created_daa, spent_block
                 FROM application_outputs
                 WHERE application_id = ?1
                   AND (?2 IS NULL OR actor_path = ?2)
                   AND (?3 IS NULL OR covenant_id = ?3)
                   AND (?4 = 0 OR spent_block IS NULL)
                   AND rowid > ?5
                 ORDER BY rowid LIMIT ?6",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map(
                params![
                    application_id,
                    actor_path,
                    covenant,
                    current_only,
                    after_id,
                    limit.clamp(1, 1001),
                ],
                application_history_from_row,
            )
            .map_err(db_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err)?;
        Ok(rows)
    }

    pub fn application_output_by_outpoint(
        &self,
        application_id: &str,
        txid: &TxId,
        output_index: u32,
    ) -> Result<Option<ApplicationHistoryRow>> {
        self.conn
            .query_row(
                "SELECT rowid, txid, output_index, covenant_id, application_id,
                        artifact_id, actor_path, state_json, created_block,
                        created_daa, spent_block
                 FROM application_outputs
                 WHERE application_id = ?1 AND txid = ?2 AND output_index = ?3",
                params![application_id, txid.0.as_slice(), output_index],
                application_history_from_row,
            )
            .optional()
            .map_err(db_err)
    }

    pub fn application_transaction(
        &self,
        application_id: &str,
        txid: &TxId,
    ) -> Result<Option<ApplicationTransactionResult>> {
        let envelope = self
            .conn
            .query_row(
                "SELECT accepting_block, accepting_daa, application_payload, status
                 FROM application_envelopes WHERE txid = ?1",
                [txid.0.as_slice()],
                |row| {
                    Ok((
                        row.get::<_, [u8; 32]>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(db_err)?;
        let Some((accepting_block, accepting_daa, application_payload, status)) = envelope else {
            return Ok(None);
        };
        let outputs = {
            let mut statement = self
                .conn
                .prepare(
                    "SELECT output_index, covenant_id, application_id, artifact_id,
                            actor_path, state_json
                     FROM application_outputs
                     WHERE txid = ?1 AND application_id = ?2 ORDER BY output_index",
                )
                .map_err(db_err)?;
            let rows = statement
                .query_map(params![txid.0.as_slice(), application_id], application_output_from_row)
                .map_err(db_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)?;
            rows
        };
        let failures = {
            let mut statement = self
                .conn
                .prepare(
                    "SELECT id, txid, accepting_block, accepting_daa, output_index,
                            application_id, artifact_id, code, detail, repaired_stream_seq
                     FROM application_decode_failures
                     WHERE txid = ?1 AND application_id = ?2 ORDER BY id LIMIT 1000",
                )
                .map_err(db_err)?;
            let rows = statement
                .query_map(params![txid.0.as_slice(), application_id], decode_failure_from_row)
                .map_err(db_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)?;
            rows
        };
        if outputs.is_empty() && failures.is_empty() {
            return Ok(None);
        }
        Ok(Some(ApplicationTransactionResult {
            txid: *txid,
            accepting_block: BlockHash(accepting_block),
            accepting_daa,
            status,
            application_payload,
            outputs,
            failures,
        }))
    }

    pub fn decode_failures(&self, limit: u64) -> Result<Vec<StoredDecodeFailure>> {
        self.application_decode_failures_page(None, 0, limit)
    }

    pub fn application_decode_failures_page(
        &self,
        application_id: Option<&str>,
        after_id: u64,
        limit: u64,
    ) -> Result<Vec<StoredDecodeFailure>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, txid, accepting_block, accepting_daa, output_index,
                        application_id, artifact_id, code, detail,
                        repaired_stream_seq
                 FROM application_decode_failures
                 WHERE (?1 IS NULL OR application_id = ?1) AND id > ?2
                 ORDER BY id LIMIT ?3",
            )
            .map_err(db_err)?;
        let rows = statement
            .query_map(
                params![application_id, after_id, limit.clamp(1, 1001)],
                decode_failure_from_row,
            )
            .map_err(db_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err);
        rows
    }

    pub fn repair_application_failures(
        &mut self,
        decoder: &(impl ApplicationDecoder + ?Sized),
        limit: u64,
    ) -> Result<ApplicationRepairResult> {
        let candidates = {
            let mut statement = self
                .conn
                .prepare(
                    "SELECT envelope.txid, envelope.transaction_json
                     FROM application_envelopes envelope
                     WHERE envelope.transaction_json IS NOT NULL
                       AND EXISTS (
                           SELECT 1 FROM application_decode_failures failure
                           WHERE failure.txid = envelope.txid
                             AND failure.repaired_stream_seq IS NULL
                       )
                     ORDER BY envelope.accepting_daa, envelope.txid
                     LIMIT ?1",
                )
                .map_err(db_err)?;
            let rows = statement
                .query_map([limit.clamp(1, 10_000)], |row| {
                    Ok((row.get::<_, [u8; 32]>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(db_err)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)?;
            rows
        };
        let mut decoded = Vec::with_capacity(candidates.len());
        for (stored_txid, transaction_json) in candidates {
            let transaction: crate::Transaction = serde_json::from_str(&transaction_json)
                .map_err(|error| Error::Invalid {
                    what: "stored application transaction",
                    value: error.to_string(),
                })?;
            if transaction.txid.0 != stored_txid {
                return Err(Error::Invalid {
                    what: "stored application transaction ID",
                    value: transaction.txid.to_string(),
                });
            }
            let application = decoder.preprocess(&transaction);
            decoded.push((transaction, application));
        }

        let tx = self.conn.transaction().map_err(db_err)?;
        let epoch = crate::store_delivery::transaction_stream_epoch(&tx)?;
        let mut next_stream_seq = crate::store_delivery::transaction_next_stream_seq(&tx)?;
        let mut result = ApplicationRepairResult {
            transactions_scanned: decoded.len() as u64,
            ..Default::default()
        };
        for (transaction, application) in decoded {
            let mut repaired_sequences = Vec::new();
            for output in &application.outputs {
                let Some(accepted) = transaction.outputs.get(output.output_index as usize) else {
                    continue;
                };
                let Some(binding) = accepted.covenant else { continue };
                if binding.covenant_id != output.covenant_id {
                    continue;
                }
                let stored = tx
                    .query_row(
                        "SELECT covenant_id, value, spk_version, spk_script,
                                created_block, created_daa, spent_block
                         FROM covenant_utxos WHERE txid = ?1 AND output_index = ?2",
                        params![transaction.txid.0.as_slice(), output.output_index],
                        |row| {
                            Ok((
                                row.get::<_, [u8; 32]>(0)?,
                                row.get::<_, u64>(1)?,
                                row.get::<_, u16>(2)?,
                                row.get::<_, Vec<u8>>(3)?,
                                row.get::<_, [u8; 32]>(4)?,
                                row.get::<_, u64>(5)?,
                                row.get::<_, Option<[u8; 32]>>(6)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(db_err)?;
                let Some((covenant, value, version, script, created_block, created_daa, spent)) =
                    stored
                else {
                    continue;
                };
                if covenant != output.covenant_id.0
                    || value != accepted.value
                    || version != accepted.spk_version
                    || script != accepted.spk_script
                {
                    continue;
                }
                let event = tx
                    .query_row(
                        "SELECT seq, accepting_block, accepting_daa, tx_index,
                                event_index, delivery_stream_seq
                         FROM covenant_events
                         WHERE txid = ?1 AND covenant_id = ?2
                         ORDER BY seq DESC LIMIT 1",
                        params![transaction.txid.0.as_slice(), output.covenant_id.0.as_slice()],
                        |row| {
                            Ok((
                                row.get::<_, u64>(0)?,
                                row.get::<_, [u8; 32]>(1)?,
                                row.get::<_, u64>(2)?,
                                row.get::<_, Option<u32>>(3)?,
                                row.get::<_, Option<u32>>(4)?,
                                row.get::<_, Option<u64>>(5)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(db_err)?;
                let Some((event_seq, accepting_block, accepting_daa, tx_index, event_index, source)) =
                    event
                else {
                    continue;
                };
                let inserted = tx
                    .execute(
                        "INSERT OR IGNORE INTO application_outputs (
                            txid, output_index, covenant_id, application_id,
                            artifact_id, actor_path, state_json, created_block,
                            created_daa, spent_block
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            transaction.txid.0.as_slice(),
                            output.output_index,
                            output.covenant_id.0.as_slice(),
                            output.application_id,
                            output.artifact_id.as_slice(),
                            output.actor_path,
                            output.state_json,
                            created_block.as_slice(),
                            created_daa,
                            spent.as_ref().map(|block| block.as_slice()),
                        ],
                    )
                    .map_err(db_err)?;
                if inserted == 0 {
                    continue;
                }
                let cursor = StreamCursor {
                    epoch,
                    seq: next_stream_seq,
                };
                let delivery = DeliveryRecord {
                    cursor,
                    kind: DeliveryKind::ProjectionRepaired,
                    source_cursor: source.map(|seq| StreamCursor { epoch, seq }),
                    covenant_id: output.covenant_id,
                    covenant_event_seq: event_seq,
                    txid: transaction.txid,
                    accepting_block: BlockHash(accepting_block),
                    accepting_daa,
                    tx_index,
                    event_index,
                    order_complete: tx_index.is_some() && event_index.is_some(),
                    pending_id: event_index.map(|ordinal| {
                        crate::pending_event_id(transaction.txid, output.covenant_id, ordinal)
                    }),
                    applications: vec![output.clone()],
                };
                crate::store_delivery::insert_delivery(&tx, &delivery)?;
                crate::projection::enqueue(&tx, next_stream_seq, &[output.covenant_id])?;
                tx.execute(
                    "UPDATE application_decode_failures
                     SET repaired_stream_seq = ?1
                     WHERE txid = ?2 AND output_index = ?3
                       AND repaired_stream_seq IS NULL",
                    params![next_stream_seq, transaction.txid.0.as_slice(), output.output_index],
                )
                .map(|count| result.failures_repaired += count as u64)
                .map_err(db_err)?;
                repaired_sequences.push(next_stream_seq);
                result.outputs_repaired += 1;
                result.deliveries_appended += 1;
                next_stream_seq = next_stream_seq.checked_add(1).ok_or_else(|| Error::Invalid {
                    what: "next stream sequence",
                    value: u64::MAX.to_string(),
                })?;
            }
            if application.failures.is_empty() {
                if let Some(&repair_seq) = repaired_sequences.first() {
                    let count = tx
                        .execute(
                            "UPDATE application_decode_failures
                             SET repaired_stream_seq = ?1
                             WHERE txid = ?2 AND repaired_stream_seq IS NULL",
                            params![repair_seq, transaction.txid.0.as_slice()],
                        )
                        .map_err(db_err)?;
                    result.failures_repaired += count as u64;
                }
            }
            let status = if application.failures.is_empty() {
                "valid"
            } else if application.outputs.is_empty() {
                "failed"
            } else {
                "partial"
            };
            tx.execute(
                "UPDATE application_envelopes SET status = ?1 WHERE txid = ?2",
                params![status, transaction.txid.0.as_slice()],
            )
            .map_err(db_err)?;
        }
        tx.execute(
            "UPDATE meta SET value = ?1 WHERE key = 'next_stream_seq'",
            [next_stream_seq.to_string()],
        )
        .map_err(db_err)?;
        result.failures_remaining = tx
            .query_row(
                "SELECT COUNT(*) FROM application_decode_failures
                 WHERE repaired_stream_seq IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(result)
    }
}

fn application_history_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ApplicationHistoryRow> {
    Ok(ApplicationHistoryRow {
        id: row.get(0)?,
        txid: TxId(row.get(1)?),
        output: ApplicationOutput {
            output_index: row.get(2)?,
            covenant_id: CovenantId(row.get(3)?),
            application_id: row.get(4)?,
            artifact_id: row.get(5)?,
            actor_path: row.get(6)?,
            state_json: row.get(7)?,
        },
        created_block: BlockHash(row.get(8)?),
        created_daa: row.get(9)?,
        spent_block: row.get::<_, Option<[u8; 32]>>(10)?.map(BlockHash),
    })
}

fn decode_failure_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredDecodeFailure> {
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
    use super::ApplicationRepairResult;
    use crate::store::{AcceptedBlockBatch, AcceptedTransaction, EventKind, NewEvent, NewUtxo, Store};
    use crate::{
        ApplicationDecoder, ApplicationOutput, ApplicationPreprocess, BlockHash,
        CovenantBinding, CovenantId, DecodeFailure, DeliveryKind, Input, Network, Outpoint,
        Output, Transaction, TxId,
    };

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
        assert_eq!(
            1,
            store
                .application_outputs_page("counter", Some("root"), None, true, 0, 10)
                .unwrap()
                .len()
        );
        assert!(store
            .application_outputs_page("other", None, None, false, 0, 10)
            .unwrap()
            .is_empty());
        assert_eq!(
            1,
            store
                .application_decode_failures_page(Some("counter"), 0, 10)
                .unwrap()
                .len()
        );
        assert!(store
            .application_decode_failures_page(Some("other"), 0, 10)
            .unwrap()
            .is_empty());
    }

    struct RepairDecoder(ApplicationOutput);

    impl ApplicationDecoder for RepairDecoder {
        fn preprocess(&self, transaction: &Transaction) -> ApplicationPreprocess {
            ApplicationPreprocess {
                raw_envelope: Some(transaction.payload.clone()),
                application_payload: Some(vec![]),
                outputs: vec![self.0.clone()],
                failures: vec![],
            }
        }
    }

    #[test]
    fn repair_is_atomic_idempotent_and_appends_a_durable_delivery() {
        let path = std::env::temp_dir().join(format!(
            "kascov-application-repair-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = Store::open(&path, Network::Testnet(10)).unwrap();
        let txid = TxId([2; 32]);
        let covenant_id = CovenantId([1; 32]);
        let transaction = Transaction {
            txid,
            version: 1,
            inputs: vec![Input {
                previous_outpoint: Outpoint {
                    txid: TxId([8; 32]),
                    index: 0,
                },
                signature_script: vec![],
                compute_budget: 1,
            }],
            outputs: vec![Output {
                value: 7,
                spk_version: 0,
                spk_script: vec![0x51],
                covenant: Some(CovenantBinding {
                    covenant_id,
                    authorizing_input: 0,
                }),
            }],
            payload: b"ARGI-invalid-before-approval".to_vec(),
        };
        let output = ApplicationOutput {
            output_index: 0,
            covenant_id,
            application_id: "duel".into(),
            artifact_id: [3; 32],
            actor_path: "Match".into(),
            state_json: "{\"turn\":1}".into(),
        };
        let mut batch = AcceptedBlockBatch::empty(BlockHash([4; 32]));
        batch.accepting_daa = 100;
        batch.accepting_blue_score = 100;
        batch.events.push(NewEvent {
            covenant_id,
            kind: EventKind::Genesis,
            txid,
            tx_index: 0,
            event_index: 0,
            payload: Some(transaction.payload.clone()),
            lane_namespace: None,
        });
        batch.created_utxos.push(NewUtxo {
            outpoint: Outpoint { txid, index: 0 },
            covenant_id,
            value: 7,
            spk_version: 0,
            spk_script: vec![0x51],
        });
        batch.transactions.push(AcceptedTransaction {
            txid,
            transaction,
            application: ApplicationPreprocess {
                raw_envelope: Some(b"ARGI-invalid-before-approval".to_vec()),
                application_payload: Some(vec![]),
                outputs: vec![],
                failures: vec![DecodeFailure {
                    output_index: Some(0),
                    application_id: Some("duel".into()),
                    artifact_id: Some([3; 32]),
                    code: "application_not_approved".into(),
                    detail: "not approved".into(),
                }],
            },
        });
        store.apply_accepted_block(&batch).unwrap();

        let repaired = store
            .repair_application_failures(&RepairDecoder(output.clone()), 10)
            .unwrap();
        assert_eq!(1, repaired.transactions_scanned);
        assert_eq!(1, repaired.outputs_repaired);
        assert_eq!(1, repaired.failures_repaired);
        assert_eq!(1, repaired.deliveries_appended);
        assert_eq!(0, repaired.failures_remaining);
        assert_eq!(
            Some(output),
            store
                .current_application_output("duel", "Match", &covenant_id)
                .unwrap()
        );
        let deliveries = store.delivery_page(None, 10).unwrap();
        assert_eq!(2, deliveries.len());
        assert_eq!(DeliveryKind::ProjectionRepaired, deliveries[1].kind);
        assert_eq!(Some(deliveries[0].cursor), deliveries[1].source_cursor);

        assert_eq!(
            ApplicationRepairResult {
                failures_remaining: 0,
                ..Default::default()
            },
            store
                .repair_application_failures(&RepairDecoder(deliveries[1].applications[0].clone()), 10)
                .unwrap()
        );
    }
}
