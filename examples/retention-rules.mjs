// Purpose: Define a versioned cancellation-risk subscription with illustrative thresholds.
export const retentionRules = [{
  id: 'customer.retention', version: '0.1.0',
  description: 'Explicit cancellation intent with no confirmed resolution.',
  ttlMs: 86_400_000,
  all: [{ fact: 'cancelIntent', on: 0.9, off: 0.1 }, { fact: 'unresolved', on: 0.9, off: 0.1 }],
}];
