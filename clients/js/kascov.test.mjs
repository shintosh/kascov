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

test('native EventSource reloads a snapshot and reopens from its cursor on reset', async () => {
  FakeEventSource.instances = [];
  const prior = globalThis.fetch;
  let reset;
  let snapshot;
  globalThis.fetch = async (url) => {
    assert.equal(url, 'https://example.test/data/mainnet.json');
    return {
      ok: true,
      json: async () => ({ stream_cursor: 'ffeeddccbbaa99887766554433221100:9' }),
    };
  };
  try {
    const subscription = new Kascov('mainnet', 'https://example.test').stream({
      EventSource: FakeEventSource,
      after: '00112233445566778899aabbccddeeff:11',
      application: 'duel',
      actor: 'match/4',
      onReset: (value) => { reset = value; },
      onSnapshot: (value) => { snapshot = value; },
    });
    const first = FakeEventSource.instances[0];
    const url = new URL(first.url);
    assert.equal(url.searchParams.get('after'), '00112233445566778899aabbccddeeff:11');
    assert.equal(url.searchParams.get('application'), 'duel');
    assert.equal(url.searchParams.get('actor'), 'match/4');

    first.emit('reset', {
      reason: 'foreign_epoch',
      current: 'ffeeddccbbaa99887766554433221100:9',
      snapshot: '/data/mainnet.json',
    });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(first.closed, true);
    assert.equal(FakeEventSource.instances.length, 2);
    const reopened = FakeEventSource.instances[1];
    const reopenedUrl = new URL(reopened.url);
    assert.equal(reopenedUrl.searchParams.get('after'), 'ffeeddccbbaa99887766554433221100:9');
    assert.equal(reopenedUrl.searchParams.get('application'), 'duel');
    assert.equal(reopenedUrl.searchParams.get('actor'), 'match/4');
    assert.equal(subscription.source, reopened);
    assert.equal(snapshot.stream_cursor, 'ffeeddccbbaa99887766554433221100:9');
    assert.equal(reset.reason, 'foreign_epoch');
    subscription.close();
    assert.equal(reopened.closed, true);
  } finally {
    globalThis.fetch = prior;
  }
});

test('native EventSource delivers parsed durable messages with browser cursor metadata', () => {
  FakeEventSource.instances = [];
  let received;
  const subscription = new Kascov().stream({
    EventSource: FakeEventSource,
    onMessage: (value, event) => { received = [value, event.lastEventId]; },
  });
  const source = subscription.source;
  source.emit('accepted', { kind: 'accepted' }, '00112233445566778899aabbccddeeff:12');
  assert.deepEqual(received, [{ kind: 'accepted' }, '00112233445566778899aabbccddeeff:12']);

});
