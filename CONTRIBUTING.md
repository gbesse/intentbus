# Contributing

This guide explains how to contribute code and examples.

Use Node.js 22 or newer. Runtime tests and demos need no dependency installation or build.
For declaration checks, run `npm ci --ignore-scripts` and `npm run typecheck`.
Run `npm run check`, `npm test`, and `npm run demo` before submitting a pull request.
Add a regression case when fixing nontrivial behavior. Keep sample data synthetic,
mark fixture-based demonstrations explicitly, and never commit credentials or customer records.
Describe the problem, resulting behavior, and validation in your pull request.
