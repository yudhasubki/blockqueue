# Architecture

BlockQueue v0.2.0 has one engine, one canonical schema, and one current HTTP
contract. Versioned HTTP paths do not create versioned writers, schedulers, or
database packages.

```text
Go caller ───────────────────────────────┐
                                         v
HTTP /v1 -> httpapi.Service port -> blockqueue.Queue
                                         |
                  +----------------------+----------------------+
                  |                      |                      |
               writer              delivery runtime         scheduler
                  |                      |                      |
                  +----------------------+----------------------+
                                         v
             db_control / db_delivery / db_schedule / db_sqlite
                                         |
                            sqlDialect + store.Driver
                                  /                 \
                              SQLite             PostgreSQL
```

## Package boundaries

- The root `blockqueue` package is the import-first public API and owns
  lifecycle, admission, delivery, and scheduling primitives. Workflow/DAG
  orchestration remains a separate future layer.
- `httpapi` owns routing, strict wire DTO validation, body limits, status
  mapping, and the narrow service interface implemented by `Queue`.
- `store` exposes a small `database/sql` driver contract. `store/sqlite` and
  `store/postgres` own connection configuration; PostgreSQL uses pgx and native
  UUID columns. `store/turso` is experimental.
- The unexported `sqlDialect` strategy owns backend syntax, bind limits,
  lock clauses, notifications, and migration locking. Backend selection happens
  once during construction; queue operations do not scatter driver-name
  conditionals through hot paths.
- SQL is grouped by storage concern in `db_control.go`, `db_writer.go`,
  `db_delivery.go`, `db_schedule.go`, and `db_sqlite.go`. Queue orchestration does not embed
  control-plane or delivery queries.
- Public domain models live at the module root, so embedded callers never need
  transport or persistence implementation packages.

## Ownership and lock order

The database is authoritative for durable state and delivery ownership. Memory
contains only immutable routing snapshots and bounded admitted writes.

1. `Queue.admissionMu` serializes topology mutations against publisher
   admission. A destructive mutation installs a writer barrier before its
   database transaction.
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

## Publish durability

All admissions reserve both message count and estimated bytes. The reservation
is held until commit or a definitive permanent failure. Transient database
errors retain the batch and retry with jittered exponential backoff. Permanent
errors are isolated per admission so one invalid request cannot poison adjacent
valid admissions.

Durable wait cancellation is an ambiguous outcome, not a rollback request. The
writer keeps ownership and returns `CommitUnknownError` with stable IDs.

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

ACK, NACK, and lease extension require the current receipt token and an
unexpired lease. Repeating a successful ACK with the same token is idempotent;
using a receipt from an older delivery returns lease lost. Run completion is
updated in the same transaction as terminal delivery transitions.

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
bounded reconciliation. Retention and subscriber cleanup are chunked, and the
schema indexes delivery errors, retry leases, DLQ pages, subscriber deletion,
and schedule-run ownership. Batch ACK executes as a set-based update in one
transaction.

PostgreSQL background jobs are fenced and safe on multiple nodes, but v0.2 does
not elect a single maintenance leader. Multiple nodes can therefore perform
redundant bounded reaper/pruner scans; leader election is planned with the typed
worker runtime rather than being implied by the current contract.

The optional server binary uses the standard `net/http` server with explicit
header, idle, and long-poll-compatible write timeouts. It binds to
`127.0.0.1` by default; public deployments should remain behind an authenticated
reverse proxy.

## Lifecycle

`Queue` moves through `new -> running -> stopping -> stopped`. Startup validates
migrations, database access, and the complete runtime snapshot before starting
workers. Shutdown stops admission, drains the writer, stops background workers,
performs a final SQLite truncate checkpoint, and closes the driver. A shutdown
deadline with pending admitted messages returns an error instead of silently
dropping them.
