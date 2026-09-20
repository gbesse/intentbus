// Purpose: Verify event transitions, duplicate handling, recovery, expiry and at-least-once delivery.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { openBus, webhookSink, validateRules } from '../src/index.mjs';
import { retentionRules } from '../examples/retention-rules.mjs';
const now = Date.UTC(2026, 8, 20, 10);
const event = (id, offset, facts, entityId = 'customer-1') => ({ id, entityId, occurredAt: new Date(now - 10_000 + offset).toISOString(), state: { facts } });
const active = { cancelIntent: 0.99, unresolved: 0.99 }, inactive = { cancelIntent: 0.99, unresolved: 0.01 }, uncertain = { cancelIntent: 0.5, unresolved: 0.99 };
async function fixture(t, overrides = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'intentbus-test-'));
  const options = { directory, rules: retentionRules, evaluator: async state => state.facts, evaluatorVersion: 'synthetic-v1', now: () => now, ...overrides };
  let bus = await openBus(options);
  t.after(async () => { await bus.close(); await rm(directory, { recursive: true }); });
  return { get bus() { return bus; }, options, directory, async reopen() { await bus.close(); bus = await openBus(options); return bus; } };
}
test('emits enter, uncertainty, confirmation and exit without repeated enters', async t => {
  const { bus } = await fixture(t);
  assert.equal((await bus.ingest(event('a', 0, active))).events[0].type, 'customer.retention.entered');
  assert.equal((await bus.ingest(event('b', 1, active))).events.length, 0);
  assert.equal((await bus.ingest(event('c', 2, uncertain))).events[0].type, 'customer.retention.uncertain');
  assert.equal(bus.states()[0].active, true);
  assert.equal((await bus.ingest(event('d', 3, active))).events[0].type, 'customer.retention.confirmed');
  assert.equal((await bus.ingest(event('e', 4, inactive))).events[0].type, 'customer.retention.exited');
});
test('duplicate ids are idempotent, conflicting content is rejected', async t => {
  const { bus } = await fixture(t); const input = event('a', 0, active);
  await bus.ingest(input);
  assert.equal((await bus.ingest(input)).duplicate, true);
  await assert.rejects(bus.ingest({ ...input, state: { facts: inactive } }), /different content/);
  assert.equal(bus.pending().length, 1);
});
test('out-of-order, equal-time, future and expired observations are rejected', async t => {
  const { bus } = await fixture(t);
  await bus.ingest(event('b', 5, active));
  await assert.rejects(bus.ingest(event('a', 4, inactive)), /Out-of-order/);
  await assert.rejects(bus.ingest(event('c', 5, inactive)), /Out-of-order/);
  await assert.rejects(bus.ingest(event('future', 80_000, active)), /future/);
  await assert.rejects(bus.ingest(event('old', -100_000_000, active, 'another')), /expired/);
});
test('invalid facts and evaluator failures leave no committed observation', async t => {
  const { bus } = await fixture(t);
  await assert.rejects(bus.ingest(event('a', 0, { cancelIntent: 1 })), /Missing/);
  await bus.ingest(event('a', 0, active));
  assert.equal(bus.pending().length, 1);
});
test('state, deduplication and pending events survive restart', async t => {
  const f = await fixture(t); const input = event('a', 0, active);
  await f.bus.ingest(input); const id = f.bus.pending()[0].id;
  await f.reopen();
  assert.equal(f.bus.pending()[0].id, id);
  assert.equal((await f.bus.ingest(input)).duplicate, true);
  assert.equal((await f.bus.ingest(event('b', 1, inactive))).events[0].type, 'customer.retention.exited');
});
test('failed sink retains event for retry using the same id', async t => {
  const f = await fixture(t); await f.bus.ingest(event('a', 0, active));
  const id = f.bus.pending()[0].id;
  await assert.rejects(f.bus.flush(async () => { throw new Error('sink down'); }), /sink down/);
  await f.reopen(); const delivered = [];
  assert.equal(await f.bus.flush(async e => { delivered.push(e.id); }), 1);
  assert.deepEqual(delivered, [id]); assert.equal(f.bus.pending().length, 0);
  await f.reopen(); assert.equal(f.bus.pending().length, 0);
});
test('expiry marks an active fact unknown and emits once', async t => {
  const { bus } = await fixture(t); await bus.ingest(event('a', 0, active));
  const expiredAt = new Date(now + 86_400_000).toISOString();
  assert.equal((await bus.expire(expiredAt))[0].status, 'unknown');
  assert.equal((await bus.expire(expiredAt)).length, 0);
});
test('single-writer lock rejects a competing process and policy changes require replay', async t => {
  const f = await fixture(t);
  await assert.rejects(openBus(f.options), /lock/);
  await f.bus.ingest(event('a', 0, active)); await f.bus.close();
  await assert.rejects(openBus({ ...f.options, evaluatorVersion: 'changed' }), /changed policy/);
});
test('malformed persisted state fails explicitly', async t => {
  const f = await fixture(t); await f.bus.close();
  await writeFile(join(f.directory, 'snapshot.json'), '{broken');
  await assert.rejects(openBus(f.options), /snapshot/);
});
test('queued ingestion remains ordered and separate entity ids cannot collide', async t => {
  const { bus } = await fixture(t);
  await Promise.all([bus.ingest(event('a', 0, active, '__proto__')), bus.ingest(event('b', 1, inactive, '__proto__')), bus.ingest(event('c', 0, active, 'constructor'))]);
  assert.deepEqual(bus.pending().map(e => e.sequence), [1, 2, 3]);
});
test('evaluator has a hard deadline even if it ignores cancellation', async t => {
  const { bus } = await fixture(t, { evaluator: async () => new Promise(() => {}), evaluationTimeoutMs: 10 });
  await assert.rejects(bus.ingest(event('a', 0, active)), /timeout/);
  assert.equal(bus.pending().length, 0);
});
test('invalid policy and store capacity fail before processing extra observations', async t => {
  assert.throws(() => validateRules([{ ...retentionRules[0], all: [{ fact: 'x', off: 0.9, on: 0.1 }] }]), /off/);
  const { bus } = await fixture(t, { maxObservations: 1 });
  await bus.ingest(event('a', 0, active));
  await assert.rejects(bus.ingest(event('b', 1, active)), /capacity/);
});
test('sink has a deadline and timed-out deliveries remain pending', async t => {
  const { bus } = await fixture(t, { sinkTimeoutMs: 10 });
  await bus.ingest(event('a', 0, active));
  await assert.rejects(bus.flush(async () => new Promise(() => {})), /Sink timeout/);
  assert.equal(bus.pending().length, 1);
});
test('expired-during-inference inputs cannot enter active state', async t => {
  let clock = now;
  const { bus } = await fixture(t, { now: () => clock, evaluator: async () => { clock += 100_000_000; return active; } });
  await assert.rejects(bus.ingest(event('a', 0, active)), /expired during/);
  assert.equal(bus.pending().length, 0);
});
test('webhook uses idempotency keys and HTTP errors are retryable by caller', async t => {
  const received = [];
  const server = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk;
    received.push({ key: req.headers['idempotency-key'], body: JSON.parse(body) });
    res.writeHead(received.length === 1 ? 503 : 204); res.end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); server.close(); });
  const { bus } = await fixture(t); await bus.ingest(event('a', 0, active));
  const sink = webhookSink(`http://127.0.0.1:${server.address().port}`);
  await assert.rejects(bus.flush(sink), /503/);
  await bus.flush(sink);
  assert.equal(received[0].key, received[1].key);
  assert.equal(received[0].key, received[0].body.id);
});
