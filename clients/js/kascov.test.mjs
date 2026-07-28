import assert from 'node:assert/strict';
import test from 'node:test';

import { Kascov } from './kascov.mjs';

class FakeEventSource {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    this.closed = false;
    FakeEventSource.instances.push(this);
  }

  addEventListener(kind, handler) { this.listeners.set(kind, handler); }
  close() { this.closed = true; }
  emit(kind, data, lastEventId = '') {
    const event = { data: JSON.stringify(data), lastEventId };
    if (kind === 'message') this.onmessage?.(event);
    else this.listeners.get(kind)?.(event);
  }
}

test('durable pages encode every cursor and identity filter', async () => {
  const prior = globalThis.fetch;
  let requested;
  globalThis.fetch = async (url) => {
    requested = url;
    return { ok: true, json: async () => ({ deliveries: [] }) };
  };
  try {
    await new Kascov('testnet-10', 'https://example.test').events({
      after: '00112233445566778899aabbccddeeff:7',
      limit: 9,
      covenant: 'aa',
      application: 'duel',
      artifact: 'bb',
      actor: 'match/4',
    });
  } finally {
    globalThis.fetch = prior;
  }
  const url = new URL(requested);
  assert.equal(url.searchParams.get('after'), '00112233445566778899aabbccddeeff:7');
  assert.equal(url.searchParams.get('application'), 'duel');
  assert.equal(url.searchParams.get('actor'), 'match/4');
});

test('native EventSource carries initial cursor and filters then closes on reset', () => {
  let reset;
  const source = new Kascov('mainnet', 'https://example.test').stream({
    EventSource: FakeEventSource,
    after: '00112233445566778899aabbccddeeff:11',
    application: 'duel',
    actor: 'match/4',
    onReset: (value) => { reset = value; },
  });
  const url = new URL(source.url);
  assert.equal(url.searchParams.get('after'), '00112233445566778899aabbccddeeff:11');
  assert.equal(url.searchParams.get('application'), 'duel');
  assert.equal(url.searchParams.get('actor'), 'match/4');

  source.emit('reset', { reason: 'foreign_epoch', current: 'new:9' });
  assert.equal(source.closed, true);
  assert.deepEqual(reset, { reason: 'foreign_epoch', current: 'new:9' });
});

test('native EventSource delivers parsed durable messages with browser cursor metadata', () => {
  let received;
  const source = new Kascov().stream({
    EventSource: FakeEventSource,
    onMessage: (value, event) => { received = [value, event.lastEventId]; },
  });
  source.emit('accepted', { kind: 'accepted' }, '00112233445566778899aabbccddeeff:12');
  assert.deepEqual(received, [{ kind: 'accepted' }, '00112233445566778899aabbccddeeff:12']);

});
