use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::application::ApplicationOutput;
use crate::{BlockHash, CovenantId, Error, TxId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamEpoch(pub [u8; 16]);

impl StreamEpoch {
    pub fn generate() -> std::result::Result<Self, getrandom::Error> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for StreamEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for StreamEpoch {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::Invalid {
                what: "stream epoch",
                value: value.to_owned(),
            });
        }
        let mut epoch = [0; 16];
        hex::decode_to_slice(value, &mut epoch).map_err(|_| Error::Invalid {
            what: "stream epoch",
            value: value.to_owned(),
        })?;
        Ok(Self(epoch))
    }
}

impl Serialize for StreamEpoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StreamEpoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamCursor {
    pub epoch: StreamEpoch,
    pub seq: u64,
}

impl StreamCursor {
    pub fn checked_next(self) -> Option<Self> {
        self.seq.checked_add(1).map(|seq| Self {
            epoch: self.epoch,
            seq,
        })
    }
}

impl fmt::Display for StreamCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.epoch, self.seq)
    }
}

impl FromStr for StreamCursor {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((epoch, seq)) = value.split_once(':') else {
            return Err(Error::Invalid {
                what: "stream cursor",
                value: value.to_owned(),
            });
        };
        if seq.contains(':') {
            return Err(Error::Invalid {
                what: "stream cursor",
                value: value.to_owned(),
            });
        }
        let epoch = epoch.parse()?;
        let seq = seq.parse().map_err(|_| Error::Invalid {
            what: "stream cursor",
            value: value.to_owned(),
        })?;
        Ok(Self { epoch, seq })
    }
}

impl Serialize for StreamCursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StreamCursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryKind {
    Accepted,
    Removed,
    ProjectionRepaired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub cursor: StreamCursor,
    pub kind: DeliveryKind,
    pub source_cursor: Option<StreamCursor>,
    pub covenant_id: CovenantId,
    pub covenant_event_seq: u64,
    pub txid: TxId,
    pub accepting_block: BlockHash,
    pub accepting_daa: u64,
    pub tx_index: Option<u32>,
    pub event_index: Option<u32>,
    pub order_complete: bool,
    pub pending_id: Option<String>,
    pub applications: Vec<ApplicationOutput>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBatch {
    pub cursor: BlockHash,
    pub processed_daa: u64,
    pub deliveries: Vec<DeliveryRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedRemovalBatch {
    pub removed_blocks: Vec<BlockHash>,
    pub deliveries: Vec<DeliveryRecord>,
}

#[cfg(test)]
mod tests {
    use super::{StreamCursor, StreamEpoch};
    use std::str::FromStr;

    #[test]
    fn cursor_round_trips_with_lowercase_epoch_and_zero_sequence() {
        let cursor = StreamCursor {
            epoch: StreamEpoch([0xab; 16]),
            seq: 0,
        };

        assert_eq!("abababababababababababababababab:0", cursor.to_string());
        assert_eq!(cursor, StreamCursor::from_str(&cursor.to_string()).unwrap());
    }

    #[test]
    fn cursor_rejects_malformed_epochs_and_sequences() {
        assert!(StreamCursor::from_str("ab:1").is_err());
        assert!(StreamCursor::from_str("ABABABABABABABABABABABABABABABAB:1").is_err());
        assert!(StreamCursor::from_str("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz:1").is_err());
        assert!(StreamCursor::from_str("abababababababababababababababab:-1").is_err());
        assert!(
            StreamCursor::from_str("abababababababababababababababab:18446744073709551616")
                .is_err()
        );
        assert!(StreamCursor::from_str("abababababababababababababababab:1:2").is_err());
    }

    #[test]
    fn cursor_next_rejects_overflow() {
        let cursor = StreamCursor {
            epoch: StreamEpoch([0; 16]),
            seq: u64::MAX,
        };

        assert!(cursor.checked_next().is_none());
    }
}
