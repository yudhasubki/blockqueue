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
  lifecycle, admission, delivery, scheduling, and orchestration.
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
2. The writer locks affected topic rows in sorted UUID order, verifies that the
   topic and at least one subscriber still exist, inserts canonical messages,
   and fans out delivery rows in the same transaction.
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

## Delivery state machine

```text
pending --claim/new receipt--> delivered --ACK--> processed
   ^                              |
   |                              +--NACK/lease expiry--> pending
   |                              |
   +----------- DLQ replay -------+--max attempts-------> dead_letter
```

ACK, NACK, and lease extension require the current receipt token and an
unexpired lease. Repeating a successful ACK with the same token is idempotent;
using a receipt from an older delivery returns lease lost. Run completion is
updated in the same transaction as terminal delivery transitions.

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
