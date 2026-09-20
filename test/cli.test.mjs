// Purpose: Verify offline timeline replay, persistent delivery and deduplication through the CLI.
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const cwd = fileURLToPath(new URL('..', import.meta.url));
test('CLI replays historical facts and flushes each pending id once', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'intentbus-cli-test-'));
  t.after(() => rm(directory, { recursive: true }));
  const env = { ...process.env, INTENTBUS_EVALUATOR_VERSION: 'synthetic-facts-v1' };
  delete env.INTENTBUS_WEBHOOK_URL; delete env.INTENTBUS_WEBHOOK_TOKEN;
  const run = command => spawnSync(process.execPath, ['bin/intentbus.mjs', command, directory, 'examples/retention-rules.json', ...(command === 'replay' ? ['examples/synthetic-timeline.jsonl'] : [])], { cwd, env, encoding: 'utf8', timeout: 10_000 });
  const replay = run('replay'); assert.equal(replay.status, 0, replay.stderr);
  const flush = run('flush'); assert.equal(flush.status, 0, flush.stderr);
  const events = flush.stdout.trim().split('\n').map(JSON.parse);
  assert.deepEqual(events.slice(0, 3).map(e => e.type), ['customer.retention.entered', 'customer.retention.uncertain', 'customer.retention.exited']);
  assert.equal(run('flush').stdout, '');
  assert.equal(run('replay').status, 0);
  assert.equal(run('flush').stdout, '');
});
