# Changelog

## 1.0.9

- Added a dedicated local context builder for client CSV data.
- Switched the rule engine to batch-insert mock tasks from assembled local contexts.
- Kept the workflow fully local and removed any proxy rotation assumptions.

## 1.0.8

- Activated the Python rule engine as a market monitor and signal generator for the internal backtest loop.
- Added SQLite task insertion on trigger with a 60-second cooldown guard.
- Kept the Redis handoff and Go executor alignment unchanged.

## 1.0.7

- Added a Redis-based pipeline verification script for end-to-end field alignment.
- Validated that fused task payloads match the Go executor struct exactly.
- Kept the internal worker and mock seed flow unchanged.

## 1.0.6

- Added a SQLite seed script for internal mock order data.
- Kept the seeded payload aligned with the Go executor task context format.
- Preserved the Python orchestrator as a pure internal task forwarder.

## 1.0.5

- Updated the Python orchestrator to act as a pure internal task forwarder.
- Added Redis async queue handoff for fused task context payloads.
- Kept SQLite task state at `processing` until the Go worker completes final confirmation.

## 1.0.4

- Added SQLite WAL-backed task state persistence for the internal worker.
- Added concurrent task status updates with transaction-safe success and failure writes.
- Kept the Redis-triggered batch worker structure and high-throughput HTTP execution skeleton.

## 1.0.3

- Cleaned generated Python cache artifacts from the repository.
- Extended ignore rules for `__pycache__`, `*.pyc`, and local SQLite files.

## 1.0.2

- Added the first compliant RPA architecture scaffold.
- Introduced Python orchestration, Go execution, Redis queueing, and SQLite persistence layout.
- Added a unified startup script and architecture documentation.

## 1.0.1

- Added the initial Goldspade scaffold.
- Recorded the release rule: local changes must be synced to the server before moving on.
- Established the versioning baseline at `1.0.1`.
