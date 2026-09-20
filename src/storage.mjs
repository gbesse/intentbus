// Purpose: Persist one-process event state atomically and prevent competing writers.
import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
const checksum = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');

export async function openStorage(directory) {
  directory = resolve(directory);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const lockPath = join(directory, 'writer.lock');
  let lock;
  try { lock = await open(lockPath, 'wx', 0o600); }
  catch (cause) { throw new Error(`Cannot acquire IntentBus writer lock at ${lockPath}. Stop the other writer; inspect stale locks manually.`, { cause }); }
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await lock.close(); await unlink(lockPath); } };
  try {
    await lock.writeFile(JSON.stringify({ description: 'Exclusive IntentBus writer lock. Remove only after verifying the writer is stopped.', pid: process.pid, openedAt: new Date().toISOString() }));
    let snapshot = null;
    try {
      const saved = JSON.parse(await readFile(join(directory, 'snapshot.json'), 'utf8'));
      if (!saved.snapshot || saved.checksum !== checksum(saved.snapshot)) throw new Error('Snapshot checksum mismatch');
      snapshot = saved.snapshot;
    }
    catch (cause) { if (cause.code !== 'ENOENT') throw new Error('Cannot read IntentBus snapshot; refusing to reset state', { cause }); }
    return {
      snapshot, close,
      async save(value) {
        if (closed) throw new Error('Storage is closed');
        const temporary = join(directory, `.snapshot-${randomUUID()}.tmp`);
        const file = await open(temporary, 'wx', 0o600);
        try { await file.writeFile(JSON.stringify({ description: 'IntentBus atomic state and outbox snapshot. Do not edit manually.', checksum: checksum(value), snapshot: value })); await file.sync(); }
        finally { await file.close(); }
        // A same-directory rename commits observations and the outbox together.
        await rename(temporary, join(directory, 'snapshot.json'));
        const dir = await open(directory, 'r');
        try { await dir.sync(); } finally { await dir.close(); }
      },
    };
  } catch (cause) {
    try { await close(); } catch (cleanup) { throw new AggregateError([cause, cleanup], 'Storage initialization and lock cleanup failed'); }
    throw cause;
  }
}
