# Changelog

All notable changes to BlockQueue are documented in this file. The project is
pre-1.0; minor releases can contain breaking changes when called out here.

## [Unreleased]

### Added

- A typed, bounded Go worker runtime with lease heartbeats, graceful drain,
  panic recovery, batched completion, cancellation, and multi-worker
  supervision.
- Transactional worker completion through `Job.CompleteTx` and `Job.CancelTx`.
- PostgreSQL maintenance leadership, native UUID storage, and pgx-backed
  connections.
- Cursor-paginated message status, delivery failure history, schedules, and
  run history across the Go and HTTP APIs.
- A visual concepts guide for fan-out, topology, leases, transactions,
  workers, scheduling, and maintenance.
- Private vulnerability reporting guidance, package examples, and explicit
  public API compatibility boundaries.

### Changed

- Subscriber defaults and validation now come from one shared contract used by
  runtime and persistence code.
- Public message, priority, lease, polling, and checkpoint limits are
  centralized so embedded and HTTP deployments cannot silently drift.
- Topology deletion performs immediate logical removal followed by bounded,
  leader-owned physical cleanup.

### Fixed

- Updated `golang.org/x/text` to `v0.39.0` to remediate GO-2026-5970 in
  reachable PostgreSQL connection paths.
- Ambiguous publish commits reconcile by stable message identity without
  duplicate fan-out or false failure reports.
- Shutdown aborts an undrainable writer and reaches `stopped` without leaking
  admitted work ownership.
- Delivery reaping and scheduler retries yield after write failures instead of
  hot-spinning.
- Dashboard parsing follows the cursor-page HTTP response envelope.

## [v0.2.0] - 2026-07-11

- Rebuilt persistence around canonical messages and receipt-fenced subscriber
  deliveries.
- Added durable and async publishing, transactional enqueue/completion, retry
  policy, scheduling, bounded maintenance, OpenAPI 3.1, and SQLite/PostgreSQL
  contract coverage.
- Introduced a clean schema break from v0.1 that requires a new database.

[Unreleased]: https://github.com/yudhasubki/blockqueue/compare/v0.2.0...HEAD
[v0.2.0]: https://github.com/yudhasubki/blockqueue/releases/tag/v0.2.0
