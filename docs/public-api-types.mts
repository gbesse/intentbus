// Purpose: Compile-check public bus and sink APIs without executing I/O.
import { openBus, validateRules, webhookSink } from '../src/index.mjs';
const bus = await openBus({ directory: './unused', rules: validateRules([]), evaluatorVersion: 'types-only', evaluator: async () => ({ flag: 0.9 }) });
await bus.ingest({ id: 'one', entityId: 'customer', occurredAt: '2026-09-20T10:00:00Z', state: { text: 'hello' } });
await bus.flush(webhookSink('https://example.com'));
// @ts-expect-error Observations require a source event id.
await bus.ingest({ entityId: 'customer', occurredAt: '2026-09-20T10:00:00Z', state: {} });
