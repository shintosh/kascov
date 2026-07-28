use kascov_core::{
    ApplicationDecoder, ApplicationOutput, ApplicationPreprocess, DecodeFailure, Transaction,
};

use crate::{decode_envelope, ApprovedManifest, OutputDeclaration};

const MAGIC: &[u8; 4] = b"ARGI";
const MAX_FAILURE_DETAIL_BYTES: usize = 512;

pub struct ArgentDecoder {
    manifest: ApprovedManifest,
}

impl ArgentDecoder {
    pub fn new(manifest: ApprovedManifest) -> Self {
        Self { manifest }
    }

    pub fn manifest(&self) -> &ApprovedManifest {
        &self.manifest
    }
}

impl ApplicationDecoder for ArgentDecoder {
    fn preprocess(&self, tx: &Transaction) -> ApplicationPreprocess {
        if !tx.payload.starts_with(MAGIC) {
            return ApplicationPreprocess::default();
        }
        let Some(limits) = self.manifest.decode_limits() else {
            return ApplicationPreprocess {
                raw_envelope: Some(tx.payload.clone()),
                failures: vec![failure(
                    None,
                    None,
                    None,
                    "no_approved_application",
                    "no enabled Argent application is approved",
                )],
                ..ApplicationPreprocess::default()
            };
        };
        let envelope = match decode_envelope(&tx.payload, limits) {
            Ok(envelope) => envelope,
            Err(error) => {
                return ApplicationPreprocess {
                    raw_envelope: Some(tx.payload.clone()),
                    failures: vec![failure(
                        None,
                        None,
                        None,
                        "invalid_envelope",
                        error.to_string(),
                    )],
                    ..ApplicationPreprocess::default()
                };
            }
        };
        let mut result = ApplicationPreprocess {
            raw_envelope: Some(tx.payload.clone()),
            application_payload: Some(envelope.application_payload),
            ..ApplicationPreprocess::default()
        };
        let output_count = envelope.outputs.len();
        for declaration in envelope.outputs {
            match self.decode_output(tx, &declaration, output_count) {
                Ok(output) => result.outputs.push(output),
                Err(decode_failure) => result.failures.push(decode_failure),
            }
        }
        result
    }
}

impl ArgentDecoder {
    fn decode_output(
        &self,
        tx: &Transaction,
        declaration: &OutputDeclaration,
        output_count: usize,
    ) -> Result<ApplicationOutput, DecodeFailure> {
        let index = u32::from(declaration.output_index);
        let failure = |code: &'static str, detail: String| {
            failure(
                Some(index),
                Some(declaration.application_id.clone()),
                Some(declaration.artifact_id),
                code,
                detail,
            )
        };
        let application = self
            .manifest
            .application(&declaration.application_id)
            .ok_or_else(|| {
                failure(
                    "application_not_approved",
                    "application is absent from the approved manifest".to_string(),
                )
            })?;
        if !application.enabled() {
            return Err(failure(
                "application_disabled",
                "application is disabled".to_string(),
            ));
        }
        if application.artifact_id() != &declaration.artifact_id {
            return Err(failure(
                "artifact_not_approved",
                "artifact ID does not match the approved artifact".to_string(),
            ));
        }
        let limits = application.limits();
        if tx.payload.len() > limits.max_envelope_bytes
            || output_count > limits.max_output_declarations
            || declaration.application_id.len() > limits.max_actor_name_bytes
            || declaration.actor_path.len() > limits.max_actor_name_bytes
            || declaration.state_json.len() > limits.max_state_bytes
        {
            return Err(failure(
                "application_limit",
                "declaration exceeds the approved application limits".to_string(),
            ));
        }
        let accepted = tx
            .outputs
            .get(usize::from(declaration.output_index))
            .ok_or_else(|| {
                failure(
                    "output_index",
                    "declared accepted output does not exist".to_string(),
                )
            })?;
        let covenant = accepted.covenant.ok_or_else(|| {
            failure(
                "output_covenant",
                "declared accepted output has no covenant binding".to_string(),
            )
        })?;
        if usize::from(covenant.authorizing_input) >= tx.inputs.len() {
            return Err(failure(
                "output_covenant",
                "covenant authorizing input does not exist".to_string(),
            ));
        }
        let rebuilt = application
            .rebuild_output(&declaration.actor_path, &declaration.state_json, accepted)
            .map_err(|detail| failure("artifact_rebuild", detail))?;
        let rebuilt_covenant = rebuilt.covenant.ok_or_else(|| {
            failure(
                "artifact_rebuild",
                "rebuilt output has no covenant binding".to_string(),
            )
        })?;
        if rebuilt.value != accepted.value
            || rebuilt.spk_version != accepted.spk_version
            || rebuilt.spk_script != accepted.spk_script
            || rebuilt_covenant.authorizing_input != covenant.authorizing_input
            || rebuilt_covenant.covenant_id != covenant.covenant_id
        {
            return Err(failure(
                "output_mismatch",
                "rebuilt Argent output does not match the accepted output".to_string(),
            ));
        }
        Ok(ApplicationOutput {
            output_index: index,
            covenant_id: covenant.covenant_id,
            application_id: declaration.application_id.clone(),
            artifact_id: declaration.artifact_id,
            actor_path: declaration.actor_path.clone(),
            state_json: declaration.state_json.clone(),
        })
    }
}

fn failure(
    output_index: Option<u32>,
    application_id: Option<String>,
    artifact_id: Option<[u8; 32]>,
    code: impl Into<String>,
    detail: impl Into<String>,
) -> DecodeFailure {
    let mut detail = detail.into();
    if detail.len() > MAX_FAILURE_DETAIL_BYTES {
        let mut end = MAX_FAILURE_DETAIL_BYTES;
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        detail.truncate(end);
    }
    DecodeFailure {
        output_index,
        application_id,
        artifact_id,
        code: code.into(),
        detail,
    }
}
