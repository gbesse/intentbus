// Purpose: Connect the real Jev-backed DecisionPacks evaluator to an IntentBus subscription.
// Install DecisionPacks from GitHub as documented in README before running this paid API example.
import { readFile } from 'node:fs/promises';
import { evaluate, createJevProvider, fingerprint } from '@gbesse/decisionpacks';
import { openBus } from '../src/index.mjs';
import { retentionRules } from './retention-rules.mjs';
const pack = JSON.parse(await readFile(new URL('./cancellation-pack.json', import.meta.url), 'utf8'));
const provider = createJevProvider();
const bus = await openBus({
  directory: './local-data/retention', rules: retentionRules,
  evaluatorVersion: `${pack.name}@${pack.version}/${pack.model}/${fingerprint(pack)}`,
  evaluator: async (state, { signal }) => {
    const record = await evaluate(pack, state, { provider, signal });
    return { cancelIntent: record.answers.cancelIntent.noul, unresolved: 1 - record.answers.resolved.noul };
  },
});
try {
  await bus.ingest({ id: `example-${Date.now()}`, entityId: 'synthetic-customer', occurredAt: new Date().toISOString(), state: { text: 'Please cancel my subscription. My problem is still not resolved.' } });
  await bus.flush(async event => console.log(JSON.stringify(event, null, 2)));
} finally { await bus.close(); }
