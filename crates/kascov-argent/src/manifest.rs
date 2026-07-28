use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use argent_artifact::Artifact;
use argent_kaspa_consensus_core::{tx::CovenantBinding, Hash};
use argent_runtime::{ArtifactBundle, ArtifactValue, TxBuilder};
use kascov_core::Output;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::EnvelopeLimits;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("cannot read Argent manifest {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid Argent manifest JSON: {0}")]
    Json(serde_json::Error),
    #[error("unsupported Argent manifest version {0}")]
    Version(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRejection {
    pub application_id: String,
    pub code: String,
    pub detail: String,
}

pub struct ApprovedManifest {
    applications: Vec<ApprovedApplication>,
    rejections: Vec<ManifestRejection>,
}

pub struct ApprovedApplication {
    id: String,
    enabled: bool,
    limits: EnvelopeLimits,
    artifact_id: [u8; 32],
    primary: Artifact,
    dependencies: Vec<Artifact>,
}

#[derive(Clone, Debug)]
pub struct RebuiltOutput {
    pub value: u64,
    pub spk_version: u16,
    pub spk_script: Vec<u8>,
    pub covenant: Option<kascov_core::CovenantBinding>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    version: u32,
    applications: Vec<ApplicationFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationFile {
    id: String,
    enabled: bool,
    artifact: ArtifactFile,
    #[serde(default)]
    dependencies: Vec<ArtifactFile>,
    limits: EnvelopeLimits,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactFile {
    path: PathBuf,
    sha256: String,
}

impl ApprovedManifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest: ManifestFile = serde_json::from_slice(&bytes).map_err(ManifestError::Json)?;
        if manifest.version != 1 {
            return Err(ManifestError::Version(manifest.version));
        }
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let mut applications = Vec::new();
        let mut rejections = Vec::new();
        let mut ids = HashSet::new();
        for entry in manifest.applications {
            let id = entry.id.clone();
            let result = if id.is_empty() {
                Err((
                    "invalid_application_id",
                    "application ID is empty".to_string(),
                ))
            } else if !ids.insert(id.clone()) {
                Err((
                    "duplicate_application_id",
                    format!("duplicate application ID `{id}`"),
                ))
            } else {
                load_application(base, entry)
            };
            match result {
                Ok(application) => applications.push(application),
                Err((code, detail)) => rejections.push(ManifestRejection {
                    application_id: id,
                    code: code.to_string(),
                    detail,
                }),
            }
        }
        Ok(Self {
            applications,
            rejections,
        })
    }

    pub fn application(&self, id: &str) -> Option<&ApprovedApplication> {
        self.applications
            .iter()
            .find(|application| application.id == id)
    }

    pub fn applications(&self) -> &[ApprovedApplication] {
        &self.applications
    }

    pub fn rejections(&self) -> &[ManifestRejection] {
        &self.rejections
    }

    pub(crate) fn decode_limits(&self) -> Option<EnvelopeLimits> {
        self.applications
            .iter()
            .filter(|application| application.enabled)
            .map(|application| application.limits)
            .reduce(|a, b| EnvelopeLimits {
                max_envelope_bytes: a.max_envelope_bytes.max(b.max_envelope_bytes),
                max_output_declarations: a.max_output_declarations.max(b.max_output_declarations),
                max_actor_name_bytes: a.max_actor_name_bytes.max(b.max_actor_name_bytes),
                max_state_bytes: a.max_state_bytes.max(b.max_state_bytes),
            })
    }
}

impl ApprovedApplication {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn limits(&self) -> EnvelopeLimits {
        self.limits
    }

    pub fn artifact_id(&self) -> &[u8; 32] {
        &self.artifact_id
    }

    pub fn rebuild_output(
        &self,
        actor_path: &str,
        state_json: &str,
        accepted: &Output,
    ) -> Result<RebuiltOutput, String> {
        let binding = accepted
            .covenant
            .ok_or_else(|| "accepted output has no covenant binding".to_string())?;
        let state: std::collections::BTreeMap<String, ArtifactValue> =
            serde_json::from_str(state_json)
                .map_err(|error| format!("invalid source state JSON: {error}"))?;
        let mut bundle = ArtifactBundle::new(&self.primary).map_err(|error| error.to_string())?;
        for dependency in &self.dependencies {
            bundle = bundle
                .with_artifact(dependency)
                .map_err(|error| error.to_string())?;
        }
        let builder = TxBuilder::from_bundle(&bundle).map_err(|error| error.to_string())?;
        let output = builder
            .genesis_output(actor_path, state, accepted.value)
            .map_err(|error| error.to_string())?;
        let covenant = CovenantBinding::new(
            binding.authorizing_input,
            Hash::from_bytes(binding.covenant_id.0),
        );
        Ok(RebuiltOutput {
            value: output.value,
            spk_version: output.script_public_key.version(),
            spk_script: output.script_public_key.script().to_vec(),
            covenant: Some(kascov_core::CovenantBinding {
                authorizing_input: covenant.authorizing_input,
                covenant_id: kascov_core::CovenantId(covenant.covenant_id.as_bytes()),
            }),
        })
    }
}

fn load_application(
    base: &Path,
    entry: ApplicationFile,
) -> Result<ApprovedApplication, (&'static str, String)> {
    if !entry.limits.valid() {
        return Err((
            "invalid_limits",
            "all four envelope limits must be positive".to_string(),
        ));
    }
    let primary = load_artifact(base, &entry.artifact)?;
    let mut dependencies = Vec::with_capacity(entry.dependencies.len());
    for dependency in &entry.dependencies {
        dependencies.push(load_artifact(base, dependency)?);
    }
    let mut bundle =
        ArtifactBundle::new(&primary).map_err(|error| ("artifact_bundle", error.to_string()))?;
    for dependency in &dependencies {
        bundle = bundle
            .with_artifact(dependency)
            .map_err(|error| ("artifact_bundle", error.to_string()))?;
    }
    TxBuilder::from_bundle(&bundle).map_err(|error| ("artifact_bundle", error.to_string()))?;
    let artifact_id: [u8; 32] = hex::decode(&primary.id)
        .map_err(|error| ("artifact_id", error.to_string()))?
        .try_into()
        .map_err(|value: Vec<u8>| {
            (
                "artifact_id",
                format!("artifact ID has {} bytes", value.len()),
            )
        })?;
    Ok(ApprovedApplication {
        id: entry.id,
        enabled: entry.enabled,
        limits: entry.limits,
        artifact_id,
        primary,
        dependencies,
    })
}

fn load_artifact(base: &Path, file: &ArtifactFile) -> Result<Artifact, (&'static str, String)> {
    if file.sha256.len() != 64
        || file
            .sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err((
            "content_hash",
            "SHA-256 must be 64 lower-case hexadecimal characters".to_string(),
        ));
    }
    let path = if file.path.is_absolute() {
        file.path.clone()
    } else {
        base.join(&file.path)
    };
    let bytes = fs::read(&path)
        .map_err(|error| ("artifact_read", format!("{}: {error}", path.display())))?;
    let found = hex::encode(Sha256::digest(&bytes));
    if found != file.sha256 {
        return Err((
            "content_hash",
            format!(
                "{}: expected {}, found {found}",
                path.display(),
                file.sha256
            ),
        ));
    }
    let artifact: Artifact = serde_json::from_slice(&bytes)
        .map_err(|error| ("artifact_json", format!("{}: {error}", path.display())))?;
    artifact
        .check_schema_version()
        .map_err(|error| ("artifact_version", error.to_string()))?;
    artifact
        .verify_template_plan()
        .map_err(|error| ("artifact_template_plan", error.to_string()))?;
    artifact
        .verify_sil_abi()
        .map_err(|error| ("artifact_sil_abi", error.to_string()))?;
    artifact
        .verify_id()
        .map_err(|error| ("artifact_id", error.to_string()))?;
    Ok(artifact)
}
