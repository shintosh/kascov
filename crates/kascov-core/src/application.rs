use serde::{Deserialize, Serialize};

use crate::{CovenantId, Transaction};

pub trait ApplicationDecoder: Send + Sync {
    fn preprocess(&self, tx: &Transaction) -> ApplicationPreprocess;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoApplicationDecoder;

impl ApplicationDecoder for NoApplicationDecoder {
    fn preprocess(&self, _tx: &Transaction) -> ApplicationPreprocess {
        ApplicationPreprocess::default()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationPreprocess {
    pub raw_envelope: Option<Vec<u8>>,
    pub application_payload: Option<Vec<u8>>,
    pub outputs: Vec<ApplicationOutput>,
    pub failures: Vec<DecodeFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationOutput {
    pub output_index: u32,
    pub covenant_id: CovenantId,
    pub application_id: String,
    pub artifact_id: [u8; 32],
    pub actor_path: String,
    pub state_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeFailure {
    pub output_index: Option<u32>,
    pub application_id: Option<String>,
    pub artifact_id: Option<[u8; 32]>,
    pub code: String,
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::{ApplicationDecoder, NoApplicationDecoder};
    use crate::{Transaction, TxId};

    #[test]
    fn no_decoder_preserves_the_raw_pipeline() {
        let transaction = Transaction {
            txid: TxId([1; 32]),
            version: 1,
            inputs: vec![],
            outputs: vec![],
            payload: b"not-an-application-envelope".to_vec(),
        };

        assert_eq!(
            super::ApplicationPreprocess::default(),
            NoApplicationDecoder.preprocess(&transaction)
        );
    }
}
