use kascov_argent::{
    decode_envelope, encode_envelope, ArgiEnvelope, EnvelopeError, EnvelopeLimits,
    OutputDeclaration,
};

fn limits() -> EnvelopeLimits {
    EnvelopeLimits {
        max_envelope_bytes: 1024,
        max_output_declarations: 4,
        max_actor_name_bytes: 32,
        max_state_bytes: 128,
    }
}

fn envelope() -> ArgiEnvelope {
    ArgiEnvelope {
        application_payload: vec![0xaa, 0xbb],
        outputs: vec![OutputDeclaration {
            output_index: 3,
            application_id: "counter".into(),
            artifact_id: [0x11; 32],
            actor_path: "Counter".into(),
            state_json: r#"{"count":{"kind":"int","value":7}}"#.into(),
        }],
    }
}

#[test]
fn encodes_exact_argi_v1_bytes() {
    let bytes = encode_envelope(&envelope(), limits()).unwrap();
    let mut expected =
        b"ARGI\x01\x00\x02\x00\x00\x00\xaa\xbb\x01\x00\x03\x00\x07\x00counter".to_vec();
    expected.extend_from_slice(&[0x11; 32]);
    expected.extend_from_slice(
        b"\x07\x00Counter\x22\x00\x00\x00{\"count\":{\"kind\":\"int\",\"value\":7}}",
    );
    assert_eq!(bytes, expected);
    assert_eq!(decode_envelope(&bytes, limits()).unwrap(), envelope());
}

#[test]
fn rejects_truncation_unknown_flags_and_trailing_bytes() {
    let bytes = encode_envelope(&envelope(), limits()).unwrap();
    for end in 0..bytes.len() {
        assert!(
            decode_envelope(&bytes[..end], limits()).is_err(),
            "accepted prefix {end}"
        );
    }
    let mut flags = bytes.clone();
    flags[5] = 1;
    assert!(matches!(
        decode_envelope(&flags, limits()),
        Err(EnvelopeError::UnknownFlags(1))
    ));
    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(
        decode_envelope(&trailing, limits()),
        Err(EnvelopeError::TrailingBytes)
    ));
}

#[test]
fn rejects_duplicate_output_indices_and_each_limit() {
    let mut duplicate = envelope();
    duplicate.outputs.push(duplicate.outputs[0].clone());
    assert!(matches!(
        encode_envelope(&duplicate, limits()),
        Err(EnvelopeError::DuplicateOutputIndex(3))
    ));

    let mut constrained = limits();
    constrained.max_output_declarations = 0;
    assert!(encode_envelope(&envelope(), constrained).is_err());
    constrained = limits();
    constrained.max_actor_name_bytes = 3;
    assert!(encode_envelope(&envelope(), constrained).is_err());
    constrained = limits();
    constrained.max_state_bytes = 3;
    assert!(encode_envelope(&envelope(), constrained).is_err());
    constrained = limits();
    constrained.max_envelope_bytes = 3;
    assert!(encode_envelope(&envelope(), constrained).is_err());
}
