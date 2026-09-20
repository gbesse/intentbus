# Release validation

This document records what was verified for the initial public alpha.

- Node.js 24.11.1 on macOS: source syntax checks, TypeScript 5.9.3 declaration-consumer checks, unit/integration tests, and offline demo.
- GitHub Actions is configured for Node.js 22 and 24; results are available on the repository Actions page.
- No local, production or remote build is configured or required.
- No live Jev API call or accuracy benchmark was performed: no TypeSafe credential was available in the implementation environment.
- Samples are synthetic and can be reproduced from the committed source.
- Tests cover transitions, uncertain/expired facts, deduplication, ordering, single-writer locks, restart recovery, HTTP delivery failures, deadlines and historical CLI replay.
- Storage tests use temporary POSIX local directories. Distributed/network filesystems are not supported.
