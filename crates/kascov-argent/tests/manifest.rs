use std::{fs, path::Path};

use kascov_argent::{
    encode_envelope, ApprovedManifest, ArgentDecoder, ArgiEnvelope, OutputDeclaration,
};
use kascov_core::{
    ApplicationDecoder, CovenantBinding, CovenantId, Input, Outpoint, Output, Transaction, TxId,
};
use sha2::{Digest, Sha256};

const FIXTURE: &[u8] = include_bytes!("fixtures/counter-artifact.json");

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_manifest(dir: &Path, sha256: &str) -> std::path::PathBuf {
    let artifact = dir.join("counter-artifact.json");
    fs::write(&artifact, FIXTURE).unwrap();
    let manifest = dir.join("argent-applications.json");
    fs::write(
        &manifest,
        serde_json::json!({
            "version": 1,
            "applications": [{
                "id": "counter",
                "enabled": true,
                "artifact": {"path": "counter-artifact.json", "sha256": sha256},
                "dependencies": [],
                "limits": {
                    "max_envelope_bytes": 4096,
                    "max_output_declarations": 4,
                    "max_actor_name_bytes": 64,
                    "max_state_bytes": 1024
                }
            }]
        })
        .to_string(),
    )
    .unwrap();
    manifest
}

#[test]
fn loads_content_addressed_artifact_and_checks_identity() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = write_manifest(dir.path(), &hash(FIXTURE));
    let approved = ApprovedManifest::load(&manifest).unwrap();
    let app = approved.application("counter").unwrap();
    assert!(app.enabled());
    assert_eq!(
        app.artifact_id(),
        hex::decode("976dca39752a8bafeb238b59118cf90dc1d950900c3aa642bad7257c112207f3")
            .unwrap()
            .as_slice()
    );
}

#[test]
fn rejects_content_hash_and_artifact_id_changes() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = write_manifest(dir.path(), &"00".repeat(32));
    let rejected = ApprovedManifest::load(&manifest).unwrap();
    assert_eq!(rejected.rejections()[0].code, "content_hash");

    let artifact = dir.path().join("counter-artifact.json");
    let mut value: serde_json::Value = serde_json::from_slice(FIXTURE).unwrap();
    value["id"] = serde_json::Value::String("00".repeat(32));
    let bytes = serde_json::to_vec_pretty(&value).unwrap();
    fs::write(&artifact, &bytes).unwrap();
    let manifest = write_manifest(dir.path(), &hash(&bytes));
    fs::write(&artifact, bytes).unwrap();
    let rejected = ApprovedManifest::load(&manifest).unwrap();
    assert_eq!(rejected.rejections()[0].code, "artifact_id");
}

#[test]
fn requires_declared_dependencies_and_valid_limits() {
    let dir = tempfile::tempdir().unwrap();
    let artifact = dir.path().join("counter-artifact.json");
    fs::write(&artifact, FIXTURE).unwrap();
    let manifest = dir.path().join("manifest.json");
    fs::write(&manifest, serde_json::json!({
        "version": 1,
        "applications": [{
            "id": "counter", "enabled": true,
            "artifact": {"path": "counter-artifact.json", "sha256": hash(FIXTURE)},
            "dependencies": [{"path": "missing.json", "sha256": "00".repeat(32)}],
            "limits": {"max_envelope_bytes": 0, "max_output_declarations": 0, "max_actor_name_bytes": 0, "max_state_bytes": 0}
        }]
    }).to_string()).unwrap();
    let rejected = ApprovedManifest::load(&manifest).unwrap();
    assert_eq!(rejected.rejections()[0].code, "invalid_limits");

    let manifest = dir.path().join("missing-dependency.json");
    fs::write(&manifest, serde_json::json!({
        "version": 1,
        "applications": [{
            "id": "counter", "enabled": true,
            "artifact": {"path": "counter-artifact.json", "sha256": hash(FIXTURE)},
            "dependencies": [{"path": "missing.json", "sha256": "00".repeat(32)}],
            "limits": {"max_envelope_bytes": 4096, "max_output_declarations": 4, "max_actor_name_bytes": 64, "max_state_bytes": 1024}
        }]
    }).to_string()).unwrap();
    let rejected = ApprovedManifest::load(&manifest).unwrap();
    assert_eq!(rejected.rejections()[0].code, "artifact_read");
}

#[test]
fn decoder_validates_the_declared_accepted_output() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = write_manifest(dir.path(), &hash(FIXTURE));
    let approved = ApprovedManifest::load(&manifest).unwrap();
    let app = approved.application("counter").unwrap();
    let state_json = r#"{"count":{"kind":"int","value":7}}"#;
    let mut accepted = Output {
        value: 42,
        spk_version: 0,
        spk_script: Vec::new(),
        covenant: Some(CovenantBinding {
            covenant_id: CovenantId([9; 32]),
            authorizing_input: 0,
        }),
    };
    let rebuilt = app
        .rebuild_output("Counter", state_json, &accepted)
        .unwrap();
    accepted.spk_version = rebuilt.spk_version;
    accepted.spk_script = rebuilt.spk_script;
    let payload = encode_envelope(
        &ArgiEnvelope {
            application_payload: b"move".to_vec(),
            outputs: vec![OutputDeclaration {
                output_index: 0,
                application_id: "counter".into(),
                artifact_id: *app.artifact_id(),
                actor_path: "Counter".into(),
                state_json: state_json.into(),
            }],
        },
        app.limits(),
    )
    .unwrap();
    let transaction = Transaction {
        txid: TxId([3; 32]),
        version: 1,
        inputs: vec![Input {
            previous_outpoint: Outpoint {
                txid: TxId([2; 32]),
                index: 0,
            },
            signature_script: Vec::new(),
            compute_budget: 0,
        }],
        outputs: vec![accepted],
        payload,
    };
    let decoded = ArgentDecoder::new(approved).preprocess(&transaction);
    assert_eq!(decoded.application_payload, Some(b"move".to_vec()));
    assert_eq!(decoded.outputs.len(), 1);
    assert!(decoded.failures.is_empty());
}

#[test]
fn facade_rebuilds_owned_bytes_from_stable_kascov_values() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = write_manifest(dir.path(), &hash(FIXTURE));
    let approved = ApprovedManifest::load(&manifest).unwrap();
    let app = approved.application("counter").unwrap();
    let accepted = Output {
        value: 42,
        spk_version: 0,
        spk_script: Vec::new(),
        covenant: Some(CovenantBinding {
            covenant_id: CovenantId([9; 32]),
            authorizing_input: 2,
        }),
    };
    let rebuilt = app
        .rebuild_output(
            "Counter",
            r#"{"count":{"kind":"int","value":7}}"#,
            &accepted,
        )
        .unwrap();
    assert_eq!(rebuilt.value, 42);
    assert!(!rebuilt.spk_script.is_empty());
    assert_eq!(rebuilt.covenant.unwrap().covenant_id, CovenantId([9; 32]));
}
