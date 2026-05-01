# Enterprise RPA Automation Support System

Version: `V2.0`

## Scope

This project provides a compliant automation support system for high-latency, multi-step business workflows.

## Boundaries

- Only use official or explicitly permitted interfaces.
- Respect target-system rate limits and retry guidance.
- Escalate to human-in-the-loop when manual verification is required.
- No client spoofing, captcha bypass, or anti-bot evasion.

## Architecture

- Python control plane:
  - rule hot reload
  - task lifecycle management
  - human-in-the-loop routing
- Go data plane:
  - reliable queue consumption
  - timeout and backoff handling
  - result reporting
- Redis:
  - task queues
  - event broadcast
  - idempotency locks
- SQLite:
  - durable local task records

## Operational goals

- Keep the footprint small for a `2 core / 4 GB` host.
- Favor graceful retries over aggressive concurrency.
- Preserve auditability for every task transition.
