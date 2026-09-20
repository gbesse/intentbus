// Purpose: Demonstrate durable semantic transitions using explicit synthetic facts, without API calls.
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openBus } from '../src/index.mjs';
import { retentionRules } from './retention-rules.mjs';
const directory = await mkdtemp(join(tmpdir(), 'intentbus-demo-'));
const start = Date.now() - 10_000;
const bus = await openBus({ directory, rules: retentionRules, evaluatorVersion: 'synthetic-facts-v1', evaluator: async state => state.facts });
try {
  const timeline = [{ cancelIntent: 0.98, unresolved: 0.99 }, { cancelIntent: 0.55, unresolved: 0.99 }, { cancelIntent: 0.98, unresolved: 0.02 }];
  for (const [i, facts] of timeline.entries()) await bus.ingest({ id: `demo-${i}`, entityId: 'synthetic-customer', occurredAt: new Date(start + i * 1000).toISOString(), state: { facts } });
  console.log('Synthetic fixture; no Jev API call.');
  await bus.flush(async event => console.log(JSON.stringify({ type: event.type, status: event.status, active: event.active })));
} finally { await bus.close(); await rm(directory, { recursive: true }); }
