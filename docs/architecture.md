# Architecture

BlockQueue has one engine, one canonical schema, and one current HTTP contract.
Versioned HTTP paths and optional execution layers do not create versioned
writers, schedulers, or database packages.

```text
Go caller -> worker (optional) ----------┐
Go caller -------------------------------+---> blockqueue.Queue
HTTP /v1 -> httpapi.Service port --------┘
                                         |
                  +----------------------+----------------------+
                  |                      |                      |
               writer              delivery runtime         scheduler
                  |                      |                      |
                  +----------------------+----------------------+
                                         v
                       internal/persistence.Store
                  topology / publish / delivery / schedule
                                         |
                            sqlDialect + store.Driver
                                  /                 \
                              SQLite             PostgreSQL
```

## Package boundaries

- The root `blockqueue` package is the import-first public API and owns
  lifecycle, admission, delivery, and scheduling primitives. Workflow/DAG
  orchestration remains a separate future layer.
- `worker` is an optional Go execution layer over the public delivery API. It
  owns bounded handler concurrency, lease heartbeat, automatic ACK/NACK, typed
  JSON decoding, receipt-fenced poison cancellation, set-based completion
  batching, metrics, group supervision, and handler drain. It depends on the
  root package and never owns `Queue` lifecycle or durable state.
- `httpapi` owns routing, strict wire DTO validation, body limits, status
  mapping, and the narrow service interface implemented by `Queue`.
- `internal/persistence` is the only package that owns queue SQL, schema
  migrations, prepared-statement caching, backend dialect strategy, and
  durable row models. The root engine reaches it through one concrete adapter;
  it is intentionally not a public extension point and does not expose a broad
  repository interface.
- `store` exposes a small `database/sql` driver contract. `store/sqlite` and
  `store/postgres` own connection configuration; PostgreSQL uses pgx and native
  UUID columns. `store/turso` is experimental.
- The persistence package's unexported `sqlDialect` strategy owns backend syntax, bind limits,
  lock clauses, notifications, and migration locking. Backend selection happens
  once during construction; queue operations do not scatter driver-name
  conditionals through hot paths.
- SQL is grouped by storage concern in `topology.go`, `publish.go`,
  `delivery.go`, `schedule.go`, `retention.go`, and `sqlite_maintenance.go` under
  `internal/persistence`. Queue orchestration does not embed control-plane or
  delivery queries.
- Public domain models live at the module root, so embedded callers never need
  transport or persistence implementation packages.

## Ownership and lock order

The database is authoritative for durable state and delivery ownership. Memory
contains only immutable routing snapshots and bounded admitted writes.

1. `Queue.admissionMu` fences lifecycle shutdown against admission. Each
   `topicRuntime` has its own admission RW fence: a destructive mutation takes
   only that topic's write fence, installs a writer barrier, and then commits
   its database transaction. Publisher admission on unrelated topics remains
   available while the barrier drains. Registered control operations are
   included in graceful shutdown before the database closes.
   `PublishTx` bypasses this process-local writer fence because its caller-owned
   transaction is already fenced by the authoritative topic row lock; this
   avoids lock inversion when one transaction publishes more than once.
2. A publisher fences affected topic rows in sorted UUID order, verifies that
   the topic and at least one subscriber still exist, inserts canonical
   messages, and fans out delivery rows in the same transaction. PostgreSQL
   uses `FOR SHARE`, so publishers do not serialize one another; destructive
   topology mutations use `FOR UPDATE` and wait for open publishers.
3. PostgreSQL claims lock candidate delivery rows with `FOR UPDATE SKIP LOCKED`.
   SQLite uses an immediate writer transaction. No process-local claim mutex is
   relied on for correctness.
4. Runtime registry swaps happen only after a successful database commit. The
   snapshots are immutable and published atomically, so hot-path reads do not
   hold a mutex while querying the database.
5. Scheduler ownership is fenced by owner, lease expiry, and an increasing
   fencing token. Run creation, occurrence publish, fan-out, and schedule
   advancement are one transaction.
6. Embedded migrations use one transaction and a checksummed ledger.
   PostgreSQL serializes migrators with a transaction-scoped advisory lock;
   the supported SQLite driver begins an immediate transaction before reading
   or updating the migration ledger.

No listener or scheduler callback executes SQL while holding a runtime registry
mutex. PostgreSQL `LISTEN/NOTIFY` wakes local workers but is only a hint; bounded
database reconciliation handles dropped notifications and multi-process use.
The listener reconnects after both startup and runtime failures, and readiness
exposes its health separately from the authoritative polling fallback.

## Publish durability

All admissions reserve both message count and estimated bytes. The reservation
is held until commit or a definitive permanent failure. Transient database
errors retain the batch and retry with jittered exponential backoff. Permanent
errors are isolated per admission so one invalid request cannot poison adjacent
valid admissions.

Durable wait cancellation is an ambiguous outcome, not a rollback request. The
writer keeps ownership and returns `CommitUnknownError` with stable IDs.
Persistence assigns immediate/delayed `created_at` and `visible_at` from the
database clock inside the write transaction. Durable receipts read back the
committed schedule, while async admitted receipts remain an admission-time
estimate until their status resource becomes persisted.

## Caller-owned transactions

`PublishTx` and `BatchPublishTx` stage the canonical message and complete
subscriber fan-out in an existing `database/sql` transaction. Delivery-side
`*Tx` methods allow application side effects and ACK, NACK, snooze, or
cancellation to share the same commit. `Queue.WithTx` is preferred because it
commits or rolls back consistently and sends local wake hints only after a
successful commit.

These APIs require application tables to live in the same physical database as
the queue. A transaction should remain short and must not perform remote I/O.
PostgreSQL holds only a shared topic fence during publish. SQLite has one writer
lock by design, so an open caller transaction serializes writer flushes, claims,
and acknowledgements until it ends; the contract and contention benchmark make
that cost explicit. Transactions created by `WithTx` are registered as in-flight
work and drained during shutdown without holding the admission mutex across the
callback. Raw caller transactions cannot be observed after a `*Tx` call returns,
so their owner must finish them before shutting the queue down.
`WithTx` never retries application code. A connection-level commit failure is
returned as `TransactionCommitUnknownError`; callers reconcile the business
operation and stable queue idempotency key rather than assuming rollback.

## Delivery state machine

```text
pending --claim/new receipt--> delivered --ACK--> processed
   ^       |                        |
   |       +-------- cancel --------+--------------------> cancelled
   |                                |
   +-- snooze (no failure) ---------+
   |                                |
   +-- retry delay <-- NACK/expiry -+--max failures-----> dead_letter
   |
   +---------------- DLQ replay --------------------------+
```

ACK, NACK, worker cancellation, and lease extension require the current receipt
token and an unexpired lease. Administrative cancellation remains available
without a receipt. Repeating successful ACK or worker cancellation with the
same token is idempotent; using a receipt from an older delivery returns lease
lost. Run completion is updated in the same transaction as terminal delivery
transitions.

`delivery_count` counts lease claims and is useful for observability.
`failure_count` counts only NACK and lease-expiry failures and controls the
dead-letter threshold. Retry delay uses the subscriber's bounded exponential
policy with deterministic jitter, so restart and concurrent nodes calculate the
same outcome. Each failure is appended to `delivery_errors`; snooze does not add
an error or consume the failure budget. `cancelled` is a terminal schedule-run
state equivalent to processed or dead-lettered for overlap completion.

## Bounded maintenance work

Hot delivery paths never perform a global schedule-run sweep. Terminal ACK,
DLQ, and cancellation transitions update only related runs; the reaper handles
bounded reconciliation. A reaper pass handles at most eight 1,000-row chunks,
and a scheduler pass handles at most 100 due occurrences; a saturated pass
yields before reconciliation continues so SQLite's single writer remains
available to publish, claim, and completion traffic.

Topic and subscriber deletion is logically atomic: once the API returns, the
resource is absent from the database's active topology and runtime registry.
Physical rows are then removed in dependency order by independently committed
1,000-row chunks under one maintenance deadline. A topic row is never removed
until its deliveries, runs, schedules, messages, and subscribers are gone, so
foreign-key cascades cannot turn cleanup back into an unbounded writer
transaction. The schema indexes delivery errors, retry leases, DLQ pages,
subscriber deletion, schedule-run ownership, and cursor-based control-plane
pages. Batch ACK, batch NACK, and bulk DLQ replay execute as set-based updates
in one transaction while retaining per-item outcomes.

PostgreSQL elects one session-advisory-lock leader for retention and physical
topology cleanup, preventing N nodes from repeating the same large scans. The
lease reaper and scheduler remain distributed: their row leases, receipt tokens,
`SKIP LOCKED`, and fencing tokens provide ownership without a single-node
availability dependency.

The optional server binary uses the standard `net/http` server with explicit
header, idle, and long-poll-compatible write timeouts. It binds to
`127.0.0.1` by default; public deployments should remain behind an authenticated
reverse proxy. Embedded routers may install a principal resolver followed by
authorization middleware; neither is enabled implicitly.

## Lifecycle

`Queue` moves through `new -> running -> stopping -> stopped`. Startup validates
migrations, database access, and the complete runtime snapshot before starting
maintenance goroutines. Shutdown stops admission, drains the writer, stops
maintenance, performs a final SQLite truncate checkpoint, and closes the
driver. A shutdown deadline with pending admitted messages returns an error
instead of silently dropping them.

An optional `worker.Worker` is single-use and deliberately has a separate
lifecycle. `Run` claims only into free concurrency slots. Canceling its context
stops new claims, keeps heartbeat active while handlers drain, and cancels
handler contexts only after the configured drain deadline. It performs one
short bounded hard-wait after cancellation; a handler that ignores its context
may still outlive `Run`. Applications stop workers before shutting down `Queue`,
so compliant handlers retain a live database connection throughout drain.
