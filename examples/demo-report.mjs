// Purpose: Capture the bundled demo as one shareable JSON report.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = new URL('../', import.meta.url);
const packageInfo = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const run = spawnSync(process.execPath, [fileURLToPath(new URL('examples/demo.mjs', root))], {
  cwd: fileURLToPath(root),
  encoding: 'utf8',
  timeout: 60_000,
});
if (run.error || run.status !== 0) {
  const detail = run.error?.message ?? (run.stderr.trim() || `exit code ${run.status}`);
  throw new Error(`demo failed: ${detail}`);
}
console.log(JSON.stringify({
  project: packageInfo.name,
  version: packageInfo.version,
  purpose: packageInfo.description,
  command: 'npm run demo',
  output: run.stdout.trim(),
}, null, 2));
