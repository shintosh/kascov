const encoder = new TextEncoder();

export const DEFAULT_ARGI_LIMITS = Object.freeze({
  maxEnvelopeBytes: 64 * 1024,
  maxOutputDeclarations: 64,
  maxActorNameBytes: 128,
  maxStateBytes: 32 * 1024,
});

function bytes(value, field) {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new TypeError(`${field} must be a Uint8Array`);
}

function putU16(target, value) {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
    throw new RangeError('ARGI u16 value is out of range');
  }
  target.push(value & 0xff, value >>> 8);
}

function putU32(target, value) {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
    throw new RangeError('ARGI u32 value is out of range');
  }
  target.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, value >>> 24);
}

function putBytes16(target, value) {
  putU16(target, value.length);
  target.push(...value);
}

function putBytes32(target, value) {
  putU32(target, value.length);
  target.push(...value);
}

/** Encode the exact ARGI v1 payload consumed by kascov-argent. */
export function encodeArgiEnvelope(envelope, limits = DEFAULT_ARGI_LIMITS) {
  const payload = bytes(envelope.applicationPayload || new Uint8Array(), 'applicationPayload');
  const outputs = envelope.outputs || [];
  if (outputs.length > limits.maxOutputDeclarations || outputs.length > 0xffff) {
    throw new RangeError('ARGI output count exceeds its limit');
  }

  const target = [0x41, 0x52, 0x47, 0x49, 1, 0];
  putBytes32(target, payload);
  putU16(target, outputs.length);
  const seen = new Set();
  for (const output of outputs) {
    if (seen.has(output.outputIndex)) throw new RangeError('duplicate ARGI output index');
    seen.add(output.outputIndex);
    const application = encoder.encode(output.applicationId);
    const actor = encoder.encode(output.actorPath);
    const state = encoder.encode(output.stateJson);
    const artifact = bytes(output.artifactId, 'artifactId');
    if (artifact.length !== 32) throw new RangeError('artifactId must contain 32 bytes');
    if (application.length > limits.maxActorNameBytes || actor.length > limits.maxActorNameBytes) {
      throw new RangeError('ARGI actor or application name exceeds its limit');
    }
    if (state.length > limits.maxStateBytes) throw new RangeError('ARGI state exceeds its limit');
    putU16(target, output.outputIndex);
    putBytes16(target, application);
    target.push(...artifact);
    putBytes16(target, actor);
    putBytes32(target, state);
  }
  if (target.length > limits.maxEnvelopeBytes) {
    throw new RangeError('ARGI envelope exceeds its limit');
  }
  return Uint8Array.from(target);
}
