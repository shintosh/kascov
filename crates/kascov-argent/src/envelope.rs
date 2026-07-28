use std::collections::HashSet;

const MAGIC: &[u8; 4] = b"ARGI";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct EnvelopeLimits {
    pub max_envelope_bytes: usize,
    pub max_output_declarations: usize,
    pub max_actor_name_bytes: usize,
    pub max_state_bytes: usize,
}

impl EnvelopeLimits {
    pub fn valid(self) -> bool {
        self.max_envelope_bytes > 0
            && self.max_output_declarations > 0
            && self.max_actor_name_bytes > 0
            && self.max_state_bytes > 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArgiEnvelope {
    pub application_payload: Vec<u8>,
    pub outputs: Vec<OutputDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputDeclaration {
    pub output_index: u16,
    pub application_id: String,
    pub artifact_id: [u8; 32],
    pub actor_path: String,
    pub state_json: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("ARGI envelope exceeds {limit} bytes")]
    EnvelopeLimit { limit: usize },
    #[error("ARGI output count exceeds {limit}")]
    OutputLimit { limit: usize },
    #[error("ARGI actor or application name exceeds {limit} bytes")]
    NameLimit { limit: usize },
    #[error("ARGI state exceeds {limit} bytes")]
    StateLimit { limit: usize },
    #[error("invalid ARGI magic")]
    Magic,
    #[error("unsupported ARGI version {0}")]
    Version(u8),
    #[error("unknown ARGI flags {0:#04x}")]
    UnknownFlags(u8),
    #[error("truncated ARGI envelope")]
    Truncated,
    #[error("invalid UTF-8 in ARGI {0}")]
    Utf8(&'static str),
    #[error("duplicate ARGI output index {0}")]
    DuplicateOutputIndex(u16),
    #[error("ARGI envelope has trailing bytes")]
    TrailingBytes,
    #[error("ARGI field length cannot be encoded")]
    LengthOverflow,
}

pub fn encode_envelope(
    envelope: &ArgiEnvelope,
    limits: EnvelopeLimits,
) -> Result<Vec<u8>, EnvelopeError> {
    check_output_count(envelope.outputs.len(), limits)?;
    let payload_len = u32::try_from(envelope.application_payload.len())
        .map_err(|_| EnvelopeError::LengthOverflow)?;
    let output_count =
        u16::try_from(envelope.outputs.len()).map_err(|_| EnvelopeError::LengthOverflow)?;
    let mut seen = HashSet::with_capacity(envelope.outputs.len());
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.push(0);
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&envelope.application_payload);
    bytes.extend_from_slice(&output_count.to_le_bytes());
    for output in &envelope.outputs {
        if !seen.insert(output.output_index) {
            return Err(EnvelopeError::DuplicateOutputIndex(output.output_index));
        }
        check_name(output.application_id.len(), limits)?;
        check_name(output.actor_path.len(), limits)?;
        check_state(output.state_json.len(), limits)?;
        bytes.extend_from_slice(&output.output_index.to_le_bytes());
        put_u16_bytes(&mut bytes, output.application_id.as_bytes())?;
        bytes.extend_from_slice(&output.artifact_id);
        put_u16_bytes(&mut bytes, output.actor_path.as_bytes())?;
        put_u32_bytes(&mut bytes, output.state_json.as_bytes())?;
    }
    if bytes.len() > limits.max_envelope_bytes {
        return Err(EnvelopeError::EnvelopeLimit {
            limit: limits.max_envelope_bytes,
        });
    }
    Ok(bytes)
}

pub fn decode_envelope(
    bytes: &[u8],
    limits: EnvelopeLimits,
) -> Result<ArgiEnvelope, EnvelopeError> {
    if bytes.len() > limits.max_envelope_bytes {
        return Err(EnvelopeError::EnvelopeLimit {
            limit: limits.max_envelope_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.take(4)? != MAGIC {
        return Err(EnvelopeError::Magic);
    }
    let version = reader.u8()?;
    if version != VERSION {
        return Err(EnvelopeError::Version(version));
    }
    let flags = reader.u8()?;
    if flags != 0 {
        return Err(EnvelopeError::UnknownFlags(flags));
    }
    let application_payload = reader.bytes_u32()?.to_vec();
    let output_count = usize::from(reader.u16()?);
    check_output_count(output_count, limits)?;
    let mut seen = HashSet::with_capacity(output_count);
    let mut outputs = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let output_index = reader.u16()?;
        if !seen.insert(output_index) {
            return Err(EnvelopeError::DuplicateOutputIndex(output_index));
        }
        let application_id = reader.string_u16("application ID")?;
        check_name(application_id.len(), limits)?;
        let artifact_id = reader
            .take(32)?
            .try_into()
            .expect("fixed artifact ID length");
        let actor_path = reader.string_u16("actor path")?;
        check_name(actor_path.len(), limits)?;
        let state_json = reader.string_u32("state JSON")?;
        check_state(state_json.len(), limits)?;
        outputs.push(OutputDeclaration {
            output_index,
            application_id,
            artifact_id,
            actor_path,
            state_json,
        });
    }
    if !reader.done() {
        return Err(EnvelopeError::TrailingBytes);
    }
    Ok(ArgiEnvelope {
        application_payload,
        outputs,
    })
}

fn check_output_count(count: usize, limits: EnvelopeLimits) -> Result<(), EnvelopeError> {
    if count > limits.max_output_declarations {
        return Err(EnvelopeError::OutputLimit {
            limit: limits.max_output_declarations,
        });
    }
    Ok(())
}

fn check_name(len: usize, limits: EnvelopeLimits) -> Result<(), EnvelopeError> {
    if len > limits.max_actor_name_bytes {
        return Err(EnvelopeError::NameLimit {
            limit: limits.max_actor_name_bytes,
        });
    }
    Ok(())
}

fn check_state(len: usize, limits: EnvelopeLimits) -> Result<(), EnvelopeError> {
    if len > limits.max_state_bytes {
        return Err(EnvelopeError::StateLimit {
            limit: limits.max_state_bytes,
        });
    }
    Ok(())
}

fn put_u16_bytes(target: &mut Vec<u8>, value: &[u8]) -> Result<(), EnvelopeError> {
    let len = u16::try_from(value.len()).map_err(|_| EnvelopeError::LengthOverflow)?;
    target.extend_from_slice(&len.to_le_bytes());
    target.extend_from_slice(value);
    Ok(())
}

fn put_u32_bytes(target: &mut Vec<u8>, value: &[u8]) -> Result<(), EnvelopeError> {
    let len = u32::try_from(value.len()).map_err(|_| EnvelopeError::LengthOverflow)?;
    target.extend_from_slice(&len.to_le_bytes());
    target.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], EnvelopeError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(EnvelopeError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(EnvelopeError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, EnvelopeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, EnvelopeError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("fixed u16 length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, EnvelopeError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed u32 length"),
        ))
    }

    fn bytes_u32(&mut self) -> Result<&'a [u8], EnvelopeError> {
        let len = usize::try_from(self.u32()?).map_err(|_| EnvelopeError::LengthOverflow)?;
        self.take(len)
    }

    fn string_u16(&mut self, field: &'static str) -> Result<String, EnvelopeError> {
        let len = usize::from(self.u16()?);
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| EnvelopeError::Utf8(field))
    }

    fn string_u32(&mut self, field: &'static str) -> Result<String, EnvelopeError> {
        String::from_utf8(self.bytes_u32()?.to_vec()).map_err(|_| EnvelopeError::Utf8(field))
    }

    fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
