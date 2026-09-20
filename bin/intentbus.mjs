#!/usr/bin/env node
// Purpose: Replay timestamped facts into a durable event store and optionally deliver its outbox.
import { readFile } from 'node:fs/promises';
import { openBus, webhookSink } from '../src/index.mjs';
async function main() {
  const [command, directory, rulesPath, observationsPath, ...extra] = process.argv.slice(2);
  if (!command || command === '--help') {
    console.log('intentbus replay STORE RULES.json OBSERVATIONS.jsonl\nintentbus flush STORE RULES.json\nintentbus expire STORE RULES.json\nSet INTENTBUS_EVALUATOR_VERSION to the fact producer revision (required).\nflush prints events unless INTENTBUS_WEBHOOK_URL is set; optional INTENTBUS_WEBHOOK_TOKEN.\nReplay input contains precomputed probabilities in state.facts. For Jev use the library adapter example.'); return;
  }
  if (!['replay', 'flush', 'expire'].includes(command) || !directory || !rulesPath || extra.length || (command !== 'replay' && observationsPath) || (command === 'replay' && !observationsPath)) throw new Error('Invalid arguments; use --help');
  const rules = JSON.parse(await readFile(rulesPath, 'utf8'));
  let replayClock;
  const bus = await openBus({ directory, rules, now: () => replayClock ?? Date.now(), evaluatorVersion: process.env.INTENTBUS_EVALUATOR_VERSION, evaluator: async state => state.facts });
  try {
    if (command === 'replay') {
      const lines = (await readFile(observationsPath, 'utf8')).split(/\r?\n/);
      for (const [i, line] of lines.entries()) {
        if (!line.trim()) continue;
        let observation;
        try { observation = JSON.parse(line); } catch (cause) { throw new Error(`Invalid JSON at line ${i + 1}`, { cause }); }
        replayClock = Date.parse(observation.occurredAt);
        console.log(JSON.stringify(await bus.ingest(observation)));
      }
      // Replayed events retain their original lifetime. Make stale final states explicit now.
      console.log(JSON.stringify({ expired: await bus.expire(new Date().toISOString()) }));
    } else if (command === 'expire') console.log(JSON.stringify(await bus.expire()));
    else {
      const sink = process.env.INTENTBUS_WEBHOOK_URL ? webhookSink(process.env.INTENTBUS_WEBHOOK_URL, { token: process.env.INTENTBUS_WEBHOOK_TOKEN }) : async event => { console.log(JSON.stringify(event)); };
      await bus.flush(sink);
    }
  } finally { await bus.close(); }
}
main().catch(error => { console.error(`intentbus: ${error.message}`); process.exitCode = 1; });
