# IntentBus

**Subscribe to business situations, with explicit uncertainty and durable delivery.**

[![Tests](https://github.com/gbesse/intentbus/actions/workflows/test.yml/badge.svg)](https://github.com/gbesse/intentbus/actions/workflows/test.yml)
[MIT](LICENSE) · Node.js 22+ · No runtime dependencies · POSIX local storage · Public alpha

IntentBus turns probabilistic facts into versioned events such as
`customer.retention.entered`, `.uncertain`, `.confirmed`, `.exited`, and `.expired`.
It persists state and an outbox atomically, rejects conflicting duplicates, and resumes
pending delivery after restart. Jev can produce the facts through DecisionPacks.

```text
Source event → evaluator → facts → versioned subscription
                                      ↓
                              atomic state + outbox
                                      ↓
                             webhook / custom sink
```

## Try it

```sh
git clone https://github.com/gbesse/intentbus.git
cd intentbus
npm run demo
npm test
```

The demo uses explicit synthetic facts, makes no model call, and prints:

```text
customer.retention.entered    active
customer.retention.uncertain  unknown
customer.retention.exited     inactive
```

## Library

```sh
npm install github:gbesse/intentbus#v0.1.1
```

```js
import { openBus, webhookSink } from '@gbesse/intentbus';

const bus = await openBus({
  directory: './local-data/events',
  evaluatorVersion: 'my-questions-v1/jev-1.13.0',
  evaluator: async (state, { signal }) => getFacts(state, { signal }),
  rules: [{
    id: 'customer.retention', version: '0.1.0',
    description: 'Explicit cancellation intent without a confirmed resolution.',
    ttlMs: 86_400_000,
    all: [
      { fact: 'cancelIntent', on: 0.9, off: 0.1 },
      { fact: 'unresolved', on: 0.9, off: 0.1 },
    ],
  }],
});
try {
  await bus.ingest({ id: 'ticket-update-123', entityId: 'customer-42',
    occurredAt: new Date().toISOString(), state: { text: 'Please cancel. The issue persists.' } });
  await bus.expire();
  await bus.flush(webhookSink(process.env.WEBHOOK_URL));
} finally {
  await bus.close();
}
```

`getFacts` is your evaluator; a complete Jev adapter is included below.
The thresholds above are illustrative, not validated model guarantees.

## Real Jev integration

From this checkout:

```sh
npm install --no-save --package-lock=false --ignore-scripts github:gbesse/decisionpacks#v0.1.0
# Set TYPESAFE_API_KEY in your environment, then:
node examples/jev-adapter.mjs
```

This makes a paid Jev request using the bundled cancellation pack and prints any events.
The included adapter uses [DecisionPacks](https://github.com/gbesse/decisionpacks), validates
responses, pins the model, and forwards cancellation signals. Tests use a synthetic evaluator;
no live-model quality benchmark is claimed.

## CLI for precomputed facts

```sh
export INTENTBUS_EVALUATOR_VERSION=synthetic-facts-v1
node bin/intentbus.mjs replay ./local-data/demo examples/retention-rules.json examples/synthetic-timeline.jsonl
node bin/intentbus.mjs flush ./local-data/demo examples/retention-rules.json
```

Input lines have `id`, `entityId`, `occurredAt` and `state.facts`. Replay uses source event
time, then marks expired final states at the present time. The outbox preserves the full
historical transition sequence; consumers must check `expiresAt` before acting on old events.
Without a webhook URL, flush prints to stdout and acknowledges those events. Set
`INTENTBUS_WEBHOOK_URL` and optional `INTENTBUS_WEBHOOK_TOKEN` for delivery over HTTP.
This explicitly sends event facts and entity ids to that endpoint. Original source text
is not stored in the snapshot or delivered in the event.

## Shareable demo report

Run `npm run demo:report` to capture this repository’s bundled example as one JSON object with the project purpose, version and complete demo output. The command fails if the demo fails, so the report is useful when sharing a reproducible first look or reporting unexpected behavior. The bundled demo’s data and safety boundaries still apply.

## Guarantees and limits

- **At least once**, not exactly once: webhook requests carry an `Idempotency-Key`.
  If a crash happens after delivery but before acknowledgment, the same id is delivered again.
- State, deduplication metadata and the outbox commit in one atomic local snapshot with fsync.
- One writer per directory, enforced by an exclusive lock. POSIX local filesystems only;
  do not use a shared/network volume as a distributed database.
- Observations must be strictly newer per entity. Equal timestamps with different ids
  are rejected. Reusing an id with changed content is an error.
- `unknown` retains the last confirmed `active` value. **Check status and expiry before acting**;
  `active: true` alone is not a currently confirmed signal.
- Call `expire()` periodically. The library does not install timers or a background service.
- Rule or evaluator changes require replay into a new directory. Keep your source log externally.
- Evaluation and sink callbacks have deadlines. Errors reject; no silent classification
  fallback. Failed deliveries remain pending. Hosts own retries and administrator alerting.
- Default maximum: 10,000 observations per store. Snapshots are rewritten per commit;
  this alpha targets small services and integration prototypes, not distributed high throughput.

After an unclean process exit, inspect `writer.lock` and verify the writer is stopped before
removing only that lock. Never delete a snapshot to suppress an integrity error.

See [event semantics](docs/event-semantics.md), [validation](docs/validation.md), and
[contributing](CONTRIBUTING.md). Use [Autonomy Meter](https://github.com/gbesse/autonomy-meter)
to evaluate fact thresholds against independently labeled outcomes.

Future work could add transactional database adapters and subscription migrations.
There is no hosted broker, connector marketplace or background scheduler in this alpha.
