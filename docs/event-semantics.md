# Event semantics

This document explains subscription transitions, ordering, persistence and delivery.

For an `all` rule, all fact probabilities at or above `on` means active. Any probability
at or below `off` means inactive. Everything else is unknown. The uncertainty band is
intentional: do not convert ambiguous input into a confirmed false condition.

An active state emits `entered` when the last confirmed state was inactive. An inactive
state emits `exited` when the last confirmed state was active. Moving into uncertainty
emits `uncertain` once. Restoring the same last-confirmed state from unknown emits
`confirmed`, avoiding a second `entered` for an already active situation. Initial inactive
observations update state without emitting an exit event.

Unknown states retain the last confirmed `active` flag. This flag is historical memory;
`status: active` and a future `expiresAt` are needed for a currently confirmed condition.
`expire(at)` marks stale states unknown and emits one `expired` event per stale observation.
A new observation refreshes expiration. No expiry timer is installed automatically.

Each event carries a stable id, monotonically increasing local sequence, subscription id
and version, evaluator version, entity and source ids, current and previous status, facts
and expiry. There is no raw source text. Entity ids and facts can still be sensitive:
protect the store, configure retention and choose webhook destinations deliberately.

An ingestion snapshots caller inputs before queueing, evaluates once, then persists the
observation digest, entity state and resulting outbox entries in one atomic snapshot.
A checksum detects accidental persisted-file changes. It is not a signature or an access
control. JSON data is required. A persistence failure poisons the writer until reopened.

Concurrency is serialized within one bus. A directory lock prohibits another writer.
A crash leaves its lock behind; manual recovery must verify no writer still owns it.
Both disk-full failures and corrupt snapshots are errors, never a request to start fresh.

Delivery calls a supplied async sink. A failed or timed-out sink leaves the current event
pending and stops the flush. An acknowledged event is removed by another atomic snapshot.
A crash in between is inherently ambiguous: the next flush reuses the same event id.
Consumers must deduplicate. A custom sink should honor the supplied AbortSignal; if it
ignores cancellation, work can continue after its deadline and retries can overlap.

Observations are strictly ordered per entity using source timestamps, not arrival order.
Use distinct timestamps or your upstream normalizer before ingestion. Changed questions,
models or rules require a new evaluator revision and replay into a new directory. The
library does not store original input text and cannot reconstruct it for you.
