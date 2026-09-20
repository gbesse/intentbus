# Security

This file describes the security boundary of this early release.

Do not post secrets, personal data or exploit details in public issues. Use GitHub's
private vulnerability reporting when enabled. Models and third-party inputs are untrusted;
model judgments never grant authorization. The embedding application owns authentication,
access controls, retention and its administrator alerting integration. Errors propagate to
the caller; the CLI exits unsuccessfully. No automatic emails or telemetry are sent.
