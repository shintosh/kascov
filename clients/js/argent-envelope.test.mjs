import assert from 'node:assert/strict';
import test from 'node:test';

import { encodeArgiEnvelope } from './argent-envelope.mjs';

test('encodes the exact ARGI v1 worked example', () => {
  const stateJson = '{"count":{"kind":"int","value":7}}';
  const actual = encodeArgiEnvelope({
    applicationPayload: Uint8Array.of(0xaa, 0xbb),
    outputs: [{
      outputIndex: 3,
      applicationId: 'counter',
      artifactId: new Uint8Array(32).fill(0x11),
      actorPath: 'Counter',
      stateJson,
    }],
  });
  const prefix = Buffer.from('ARGI\x01\x00\x02\x00\x00\x00\xaa\xbb\x01\x00\x03\x00\x07\x00counter', 'binary');
  const suffix = Buffer.from(`\x07\x00Counter\x22\x00\x00\x00${stateJson}`, 'binary');
  assert.deepEqual(Buffer.from(actual), Buffer.concat([prefix, Buffer.alloc(32, 0x11), suffix]));
});

test('rejects duplicate outputs and every public bound', () => {
  const output = {
    outputIndex: 1,
    applicationId: 'duel',
    artifactId: new Uint8Array(32),
    actorPath: 'Match',
    stateJson: '{}',
  };
  assert.throws(() => encodeArgiEnvelope({ outputs: [output, output] }), /duplicate/);
  assert.throws(() => encodeArgiEnvelope({ outputs: [output] }, {
    maxEnvelopeBytes: 1,
    maxOutputDeclarations: 1,
    maxActorNameBytes: 16,
    maxStateBytes: 16,
  }), /envelope/);
});
