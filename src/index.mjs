// Purpose: Convert bounded probabilistic facts into durable business-event transitions.
import { createHash } from 'node:crypto';
import { openStorage } from './storage.mjs';
function ensure(ok, message) { if (!ok) throw new Error(message); }
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    ensure([Object.prototype, null].includes(Object.getPrototypeOf(value)), 'Only plain JSON data is supported');
    return Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])]));
  }
  ensure(value === null || ['string', 'boolean'].includes(typeof value) || (typeof value === 'number' && Number.isFinite(value)), 'Only finite JSON values are supported');
  return value;
}
function hash(value) { return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex'); }
function probability(value) { return Number.isFinite(value) && value >= 0 && value <= 1; }
function timestamp(value) {
  ensure(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value)), 'Expected timezone-qualified timestamp');
  return Date.parse(value);
}
export function validateRules(rules) {
  ensure(Array.isArray(rules) && rules.length > 0, 'At least one subscription is required');
  const ids = new Set();
  for (const rule of rules) {
    ensure(typeof rule.id === 'string' && /^[a-z][a-z0-9._-]*$/.test(rule.id) && !ids.has(rule.id), 'Subscription ids must be unique'); ids.add(rule.id);
    ensure(typeof rule.description === 'string' && rule.description.length > 0, 'Subscription requires a description');
    ensure(typeof rule.version === 'string' && /^\d+\.\d+\.\d+$/.test(rule.version), 'Version must be x.y.z');
    ensure(Number.isInteger(rule.ttlMs) && rule.ttlMs > 0, 'ttlMs must be a positive integer');
    ensure(Array.isArray(rule.all) && rule.all.length > 0, 'Subscription requires facts');
    const facts = new Set();
    for (const condition of rule.all) {
      ensure(typeof condition.fact === 'string' && /^[a-z][a-zA-Z0-9_]*$/.test(condition.fact) && !facts.has(condition.fact), 'Fact names must be unique simple identifiers'); facts.add(condition.fact);
      ensure(probability(condition.on) && probability(condition.off) && condition.off < condition.on, 'Require 0 <= off < on <= 1');
    }
  }
  return rules;
}

function classify(rule, facts) {
  if (rule.all.some(c => facts[c.fact] <= c.off)) return 'inactive';
  if (rule.all.every(c => facts[c.fact] >= c.on)) return 'active';
  return 'unknown';
}

export async function openBus({ directory, rules, evaluator, evaluatorVersion, now = Date.now, evaluationTimeoutMs = 30_000, sinkTimeoutMs = 30_000, maxObservations = 10_000 } = {}) {
  validateRules(rules);
  ensure(typeof directory === 'string' && directory.length > 0, 'Storage directory is required');
  ensure(typeof evaluator === 'function', 'Provide an evaluator');
  ensure(typeof evaluatorVersion === 'string' && evaluatorVersion.length > 0, 'Pin evaluatorVersion to your model and question revision');
  ensure(Number.isInteger(evaluationTimeoutMs) && evaluationTimeoutMs > 0 && evaluationTimeoutMs <= 300_000, 'Invalid evaluation timeout');
  ensure(Number.isInteger(sinkTimeoutMs) && sinkTimeoutMs > 0 && sinkTimeoutMs <= 300_000, 'Invalid sink timeout');
  ensure(Number.isInteger(maxObservations) && maxObservations > 0, 'Invalid maxObservations');
  rules = structuredClone(rules);
  const policyHash = hash({ rules, evaluatorVersion });
  const storage = await openStorage(directory);
  let data = storage.snapshot ?? { schemaVersion: 1, policyHash, observations: {}, latest: {}, subscriptions: {}, outbox: [], sequence: 0 };
  if (data.schemaVersion !== 1 || data.policyHash !== policyHash || !data.observations || !data.latest || !data.subscriptions || !Array.isArray(data.outbox) || !Number.isInteger(data.sequence)) {
    await storage.close(); throw new Error('Invalid snapshot or changed policy/evaluator. Replay source events into a new directory.');
  }
  let tail = Promise.resolve(), closed = false, closing = false, fatal = false;
  function enqueue(operation) {
    if (closing || closed) return Promise.reject(new Error('Bus is closed'));
    const task = tail.then(async () => { ensure(!fatal, 'Persistence failed; close and inspect storage before continuing'); return operation(); });
    // Preserve queue liveness after reported evaluator/transport errors; task retains the rejection.
    tail = task.then(() => undefined, () => undefined);
    return task;
  }
  async function commit(next) {
    try { await storage.save(next); data = next; }
    catch (cause) { fatal = true; throw new Error('IntentBus persistence failed; no further operations allowed', { cause }); }
  }
  function emit(next, rule, entityId, sourceId, occurredAt, before, after, kind) {
    const sequence = ++next.sequence;
    const event = {
      schemaVersion: 1, id: hash({ policyHash, sequence, entityId, rule: rule.id }), sequence,
      type: `${rule.id}.${kind}`, subscription: { id: rule.id, version: rule.version }, evaluatorVersion,
      entityId, sourceId, occurredAt, status: after.status, active: after.active,
      previousStatus: before?.status ?? null, facts: after.facts, expiresAt: after.expiresAt,
    };
    next.outbox.push(event);
    return event;
  }
  return {
    ingest(observation) {
      // Snapshot immediately, before queueing or inference, to preserve source-event identity.
      observation = structuredClone(observation);
      return enqueue(async () => {
        ensure(observation && typeof observation === 'object', 'Observation required');
        ensure(typeof observation.id === 'string' && observation.id.length > 0, 'Observation id required');
        ensure(typeof observation.entityId === 'string' && observation.entityId.length > 0, 'Entity id required');
        ensure(observation.state !== undefined && observation.state !== null, 'State is required');
        const time = timestamp(observation.occurredAt), currentTime = now();
        ensure(time <= currentTime + 60_000, 'Observation is too far in the future');
        const key = hash(observation.id), entityKey = hash(observation.entityId), digest = hash(observation);
        if (Object.hasOwn(data.observations, key)) {
          ensure(data.observations[key].digest === digest, 'Duplicate id with different content');
          return { duplicate: true, events: [] };
        }
        ensure(Object.keys(data.observations).length < maxObservations, 'Observation capacity reached; archive and replay into a new store');
        ensure(!Object.hasOwn(data.latest, entityKey) || time > data.latest[entityKey], 'Out-of-order or equal-time observation for this entity');
        ensure(rules.every(rule => time + rule.ttlMs > currentTime), 'Observation is already expired for a subscription');
        const controller = new AbortController();
        let timer;
        const timeout = new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new Error('Evaluator timeout')); }, evaluationTimeoutMs); });
        let facts;
        try { facts = await Promise.race([evaluator(structuredClone(observation.state), { signal: controller.signal }), timeout]); }
        finally { clearTimeout(timer); }
        ensure(rules.every(rule => time + rule.ttlMs > now()), 'Observation expired during evaluation');
        ensure(facts && typeof facts === 'object' && !Array.isArray(facts), 'Evaluator must return a fact map');
        const required = [...new Set(rules.flatMap(rule => rule.all.map(c => c.fact)))];
        for (const fact of required) ensure(Object.hasOwn(facts, fact) && probability(facts[fact]), `Missing or invalid fact: ${fact}`);
        facts = Object.fromEntries(required.map(fact => [fact, facts[fact]]));
        const next = structuredClone(data), events = [];
        next.observations[key] = { digest, entityId: observation.entityId, occurredAt: observation.occurredAt };
        next.latest[entityKey] = time;
        for (const rule of rules) {
          const subKey = hash([observation.entityId, rule.id]), before = next.subscriptions[subKey];
          const status = classify(rule, facts);
          const after = { entityId: observation.entityId, ruleId: rule.id, sourceId: observation.id, status, active: status === 'unknown' ? before?.active ?? false : status === 'active', facts, expiresAt: new Date(time + rule.ttlMs).toISOString() };
          next.subscriptions[subKey] = after;
          let kind;
          if (status === 'unknown' && before?.status !== 'unknown') kind = 'uncertain';
          else if (after.active !== (before?.active ?? false)) kind = after.active ? 'entered' : 'exited';
          else if (before?.status === 'unknown' && status !== 'unknown') kind = 'confirmed';
          if (kind) events.push(emit(next, rule, observation.entityId, observation.id, observation.occurredAt, before, after, kind));
        }
        await commit(next);
        return { duplicate: false, events: structuredClone(events) };
      });
    },
    expire(at = new Date(now()).toISOString()) {
      return enqueue(async () => {
        const time = timestamp(at), next = structuredClone(data), events = [];
        for (const sub of Object.values(next.subscriptions)) {
          if (Date.parse(sub.expiresAt) > time || sub.expired) continue;
          const before = structuredClone(sub); sub.status = 'unknown'; sub.expired = true;
          const rule = rules.find(r => r.id === sub.ruleId);
          events.push(emit(next, rule, sub.entityId, sub.sourceId, at, before, sub, 'expired'));
        }
        if (events.length) await commit(next);
        return structuredClone(events);
      });
    },
    pending() { return structuredClone(data.outbox); },
    states() { return structuredClone(Object.values(data.subscriptions)); },
    flush(sink) {
      return enqueue(async () => {
        ensure(typeof sink === 'function', 'Provide an event sink');
        let delivered = 0;
        while (data.outbox.length) {
          const event = structuredClone(data.outbox[0]);
          // A crash between delivery and commit re-delivers the same id: consumers must deduplicate.
          const controller = new AbortController(); let timer;
          const deadline = new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new Error('Sink timeout; delivery may have occurred, retry with the same event id')); }, sinkTimeoutMs); });
          try { await Promise.race([sink(event, { signal: controller.signal }), deadline]); }
          finally { clearTimeout(timer); }
          const next = structuredClone(data); next.outbox.shift(); await commit(next); delivered++;
        }
        return delivered;
      });
    },
    async close() {
      if (closed) return;
      closing = true; await tail; await storage.close(); closed = true;
    },
  };
}

export function webhookSink(url, { token, timeoutMs = 10_000, fetchImpl = globalThis.fetch } = {}) {
  const endpoint = new URL(url);
  ensure(endpoint.protocol === 'https:' || (endpoint.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname)), 'Webhook must use HTTPS (loopback HTTP allowed)');
  ensure(!endpoint.username && !endpoint.password, 'Webhook URL cannot include credentials');
  ensure(Number.isInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 300_000, 'Invalid webhook timeout');
  return async (event, { signal } = {}) => {
    const timeout = AbortSignal.timeout(timeoutMs);
    const response = await fetchImpl(endpoint, {
      method: 'POST', redirect: 'error', signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      headers: { 'content-type': 'application/json', 'idempotency-key': event.id, ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify(event),
    });
    if (response.body) await response.body.cancel();
    ensure(response.ok, `Webhook HTTP ${response.status}; event remains in the outbox`);
  };
}
