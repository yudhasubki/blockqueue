<p align="center">
  <img src="docs/img/blockqueue-logo.png" alt="BlockQueue logo" width="176">
</p>

<h1 align="center">BlockQueue</h1>

<p align="center">
  A durable, embeddable message queue for Go, backed by SQLite or PostgreSQL.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/yudhasubki/blockqueue"><img src="https://pkg.go.dev/badge/github.com/yudhasubki/blockqueue.svg" alt="Go Reference"></a>
  <a href="https://github.com/yudhasubki/blockqueue/actions/workflows/ci.yaml"><img src="https://github.com/yudhasubki/blockqueue/actions/workflows/ci.yaml/badge.svg" alt="CI status"></a>
  <img src="https://img.shields.io/badge/Go-1.25.12%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.25.12+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache-2.0 license"></a>
</p>

BlockQueue is an embeddable, at-least-once message queue for Go backed by
SQLite or PostgreSQL. It stores one canonical message and one delivery row per
subscriber, so fan-out state, retries, leases, DLQ transitions, and schedules
remain transactional.

Turso/libSQL support is experimental.

> [!IMPORTANT]
> v0.2.0 is a clean schema break and requires a new, empty database. It does
> not perform an in-place database upgrade from v0.1.

## Core capabilities

- Durable publish is the default in the Go API; explicit async publish is
  available when admission latency matters more than crash durability.
- A weighted writer budget limits both pending message count and bytes. A
  reservation is released only after the database transaction finishes.
- Claims use database locking, a new receipt token for every delivery lease,
  idempotent ACK, fenced stale receipts, delayed NACK, snooze, cancellation,
  and lease extension.
- `PublishTx`, `AckDeliveryTx`, and the other `*Tx` methods can commit queue
  state atomically with application tables in the same SQLite or PostgreSQL
  database.
- Delivery claims and processing failures are counted separately. Subscriber
  retry policy supports exponential backoff with bounded deterministic jitter,
  and every failure is retained in a paginated error history.
- Priority, delayed delivery, absolute RFC3339 scheduling, recurring five-field
  cron schedules, IANA timezones, run history, and overlap protection.
- Transactional, checksummed embedded schema migrations.
- `/livez`, `/readyz`, an embedded OpenAPI 3.1 document, RFC 9457 problem
  responses, bounded retention, adaptive SQLite checkpoints, and optional
  Prometheus metrics.

There is one queue engine and one current HTTP contract at `/v1`; the project
does not maintain parallel v1/v2 engines or schemas. See the
[v0.2 migration guide](docs/migration-v0.2.md) for source-level changes and
fresh-database rollout instructions.

## Install

```bash
go get github.com/yudhasubki/blockqueue
```

The server binary is optional:

```bash
go build -o blockqueue ./cmd/blockqueue
```

## Embed in a Go application

Only the root package and the selected storage driver are required:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func main() {
	driver, err := sqlite.Open("blockqueue.db", sqlite.Config{})
	if err != nil {
		log.Fatal(err)
	}

	queue := blockqueue.New(driver, blockqueue.Options{})
	if err := queue.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	topic := blockqueue.NewTopic("orders")
	worker := blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{
		MaxAttempts:        5,
		VisibilityDuration: "30s",
		DequeueBatchSize:   10,
	})
	if err := queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{worker}); err != nil {
		log.Fatal(err)
	}

	receipt, err := queue.Publish(context.Background(), topic, blockqueue.Message{
		Message:        `{"order_id":"1022"}`,
		IdempotencyKey: "order-1022",
		Priority:       10,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("persisted %s", receipt.MessageID)

	deliveries, err := queue.ClaimWait(context.Background(), topic, worker.Name, 10, time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	for _, delivery := range deliveries {
		// Process the message before acknowledging this exact lease.
		if err := queue.AckDelivery(context.Background(), topic, worker.Name,
			delivery.ID, delivery.ReceiptToken); err != nil {
			log.Fatal(err)
		}
	}
}
```

`Publish` and `BatchPublish` wait for commit. `PublishAsync` and
`BatchPublishAsync` return after bounded in-memory admission. If a durable
caller's context expires after admission, it receives `*CommitUnknownError`
with stable message IDs; the writer continues owning the admitted messages.

### Transactional enqueue and completion

Use `WithTx` when application tables and BlockQueue use the same database. The
producer can atomically create application data and enqueue later work:

```go
err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
	if err := insertOrder(ctx, tx, orderID, "pending"); err != nil {
		return err
	}
	_, err := queue.PublishTx(ctx, tx, topic, blockqueue.Message{
		Message:        `{"order_id":"1022","action":"fulfill"}`,
		IdempotencyKey: "fulfill-order-1022",
	})
	return err
})
```

That commit does not include consumer execution. The consumer claims the
committed delivery later, then can atomically store its result and complete the
delivery:

```go
err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
	if err := markOrderFulfilled(ctx, tx, orderID); err != nil {
		return err
	}
	return queue.AckDeliveryTx(
		ctx, tx, topic, worker.Name, delivery.ID, delivery.ReceiptToken,
	)
})
```

`PublishTx` returns `state: staged`; the rows become visible only if the
transaction commits. `AckDeliveryTx`, `NackDeliveryTx`, `SnoozeDeliveryTx`,
`CancelDeliveryTx`, `CancelClaimedDeliveryTx`, and `CancelMessageTx` provide the
same atomic boundary for consumer side effects. Keep callbacks short and free
of network calls. SQLite
has one writer, so an open caller transaction intentionally blocks queue
writes until commit or rollback. PostgreSQL uses a shared topology fence, so
publishers can proceed concurrently while destructive subscriber mutations
wait for the transaction. `Shutdown` drains transactions created by `WithTx`;
callers that begin a raw transaction themselves must coordinate its lifetime
with shutdown.

`WithTx` never retries the callback. If the connection is lost while committing,
it returns `*TransactionCommitUnknownError`: both the business writes and queue
changes may already be committed, so reconcile by business/idempotency key
instead of repeating non-idempotent application logic.

The runnable [transactional example](example/transactional) shows both commits
against one SQLite database, including receipt-fenced consumer completion.

## Go worker runtime (v0.3)

The importable `worker` package turns the delivery API into a bounded consumer
runtime. It never owns or hides the queue lifecycle: start `Queue` first, run
one worker per topic/subscriber pair, stop workers, and then shut down `Queue`.

```go
type FulfillOrder struct {
	OrderID string `json:"order_id"`
}

runner, err := worker.NewJSON(
	queue,
	topic,
	subscriber.Name,
	worker.TypedHandlerFunc[FulfillOrder](func(ctx context.Context, job *worker.TypedJob[FulfillOrder]) error {
		return job.CompleteTx(ctx, nil, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				"UPDATE orders SET fulfilled_at = ? WHERE id = ?",
				time.Now().UTC(), job.Args.OrderID,
			)
			return err
		})
	}),
	worker.Options{Concurrency: 16},
)
if err != nil {
	log.Fatal(err)
}

// Cancellation stops new claims and drains active handlers before returning.
if err := runner.Run(ctx); err != nil {
	log.Fatal(err)
}
```

Handlers returning nil are ACKed automatically. Handler errors are NACKed with
the subscriber retry policy; `worker.RetryAfter` overrides the delay for one
attempt. `worker.CancelJob` and `Job.Cancel` receipt-fence explicitly permanent
business outcomes directly into `cancelled`. Malformed JSON in `NewJSON`
follows the normal NACK/DLQ policy so it remains observable and replayable.
Panics become NACKs instead of crashing the process.
The runtime never claims more work than its free concurrency slots and
heartbeats each active lease (one-minute lease, jittered 20-second heartbeat
window by default).
Automatic completions are transaction-batched when supported by the client;
failed items and transient/ambiguous failures retry through the single-item API
using the same receipt token.

`Job.CompleteTx` is the safe path for database side effects: application writes
and `AckDeliveryTx` commit together, and the worker does not issue a second ACK.
`Job.CancelTx` provides the same atomic boundary for permanent cancellation.
`Job.Ack`, `Job.Nack`, and `Job.Cancel` are available for explicit completion.
On `Run` context cancellation, claims stop immediately while active handlers
retain heartbeat for a 30-second drain window. The worker then cancels handler
contexts and waits one additional second. A handler that ignores cancellation
can still outlive `Run` and must not access `Queue` after `Run` returns.

`worker.Group` supervises several topic/subscriber workers, cancels peers when
one returns a terminal error, and drains them together. Concurrency remains
bounded per worker; the group intentionally does not imply a global limit.
Worker metrics include jobs by outcome, handler duration, active handlers, and
heartbeat success/failure/lease loss. Pass the same Prometheus registerer used
by `Queue`, or set `worker.Options.DisableMetrics` for a fully no-op path. The
collector names are `blockqueue_worker_jobs_total`,
`blockqueue_worker_handler_duration_seconds`,
`blockqueue_worker_active_handlers`, and `blockqueue_worker_heartbeat_total`.
For `worker_jobs_total`, `nacked` means one failed handler attempt was recorded;
it does not imply the delivery has exhausted retries or reached DLQ. Handler
duration uses `ok`, `error`, `panic`, and `cancel_requested` return semantics.
NACK errors and cancellation reasons are normalized to valid UTF-8 and bounded
to 16 KiB before they cross the worker client or persistence boundary.

The runnable [worker example](example/worker) demonstrates typed JSON handling,
transactional completion, graceful worker drain, and ordered queue shutdown
against SQLite. The package is part of the v0.3 public API.

## Run the HTTP server

Copy [config.yaml.example](config.yaml.example), then run:

```bash
./blockqueue migrate -config config.yaml
./blockqueue http -config config.yaml
```

`Queue.Run` also applies migrations, so the explicit migration command is
optional for embedded deployments.

Configuration decoding rejects unknown YAML fields and expands `${NAME}` from
the process environment. This keeps PostgreSQL passwords out of committed
configuration files; unset variables expand to an empty value and normal
connection validation still applies.

Create a topic:

```bash
curl -X POST http://127.0.0.1:8080/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"orders",
    "subscribers":[{
      "name":"fulfillment",
      "option":{
        "max_attempts":5,
        "visibility_duration":"30s",
        "dequeue_batch_size":10
      }
    }]
  }'
```

Async publish is the HTTP default and returns `202` with `state: admitted`:

```bash
curl -X POST http://127.0.0.1:8080/v1/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '{"message":"order-1022","idempotency_key":"order-1022","priority":10}'
```

Wait for commit with `?wait_for=commit`; this returns a definitive duplicate
result:

```bash
curl -X POST 'http://127.0.0.1:8080/v1/topics/orders/messages?wait_for=commit' \
  -H 'Content-Type: application/json' \
  -d '{"message":"order-1022","idempotency_key":"order-1022"}'
```

Claim and ACK the returned receipt token:

```bash
curl -X POST 'http://127.0.0.1:8080/v1/topics/orders/subscribers/fulfillment/claim?timeout=30s&limit=10'

curl -X POST http://127.0.0.1:8080/v1/topics/orders/subscribers/fulfillment/messages/MESSAGE_ID/ack \
  -H 'Content-Type: application/json' \
  -d '{"receipt_token":"RECEIPT_TOKEN"}'
```

JSON decoding is strict. The HTTP limits are 1 MiB per message, 1,000 messages
per batch, 16 MiB per request body, 16 KiB of headers, and 128 bytes per
idempotency key.

The complete OpenAPI 3.1 contract is served at `/openapi.json`. Errors use
`application/problem+json` with stable `code` values. The HTTP surface exposes
message status, delivery/message cancellation, snooze, and delivery error
history. Database transaction methods are intentionally Go-only: a remote HTTP
request cannot join the caller's local transaction; cross-database systems
should use the documented [transactional outbox relay](docs/http-outbox.md).
Async single-message publish returns a `Location` header for the canonical
message status resource. If an ambiguous commit is reconciled by the writer's
internal retry, a first durable call can return `duplicate: true`; this means
the same stable message row was already committed, not that BlockQueue created
a second publish. Embedders can install `AuthMiddleware`, a typed
`PrincipalResolver`, or both; the standalone binary remains loopback-only by
default and warns when configured on a non-loopback address. Topic, subscriber,
schedule, active-delivery, DLQ, failure-history, and schedule-run lists use
bounded cursor pagination (`limit` plus opaque `cursor`).

List responses are page objects inside the common `data` envelope, not bare
arrays. For example, topics use `{"data":{"topics":[...],"next_cursor":"..."}}`
and subscriber status uses
`{"data":{"subscribers":[...],"next_cursor":"..."}}`. Omit `cursor` for the
first request and pass the returned `next_cursor` unchanged for the next page.
Because claims may long-poll for up to 60 seconds, embedded HTTP servers must
configure `WriteTimeout` to at least 65 seconds.

## Delivery contract

- Delivery is at-least-once. Consumers must make side effects idempotent.
- A claim owns a delivery only for its current receipt token and lease. ACK,
  NACK, and lease extension reject stale receipts.
- `delivery_count` increases when a lease is claimed; `failure_count` increases
  only on NACK or lease expiry. Dead-lettering is based on failures, with three
  failures and exponential retry delay as the default.
- Snooze returns a claimed delivery to pending without consuming a failure.
  Cancellation is terminal and idempotent, and failure records remain
  queryable while the delivery is retained. NACK errors and cancellation
  reasons are stored as valid UTF-8 with a 16 KiB maximum.
- `Publish` waits for the canonical message and all subscriber delivery rows to
  commit. `PublishAsync` guarantees bounded process-local admission, not crash
  durability.
- The database is authoritative. PostgreSQL notifications and in-memory wakeups
  reduce latency but are never required for correctness. Immediate/delayed
  publish timestamps, lease timers, scheduler timers, and retention cutoffs use
  database time, avoiding application clock-skew errors in multi-node setups.
- `DeleteTopic` and `DeleteSubscriber` commit logical deletion before returning.
  Their data becomes inaccessible immediately and physical rows are reclaimed
  asynchronously in bounded maintenance chunks; callers must not use raw row
  disappearance as the completion signal.
- Built-in authentication and exactly-once execution are outside the project
  scope. Protect the HTTP server with a private network or reverse proxy.

v0.2 established the durable embedded/HTTP fan-out core. v0.3 adds the typed Go
worker runtime without changing the queue's delivery contract. PostgreSQL nodes
elect one advisory-lock leader for retention and topology cleanup; claims,
lease reaping, and scheduler ownership remain distributed and receipt/lease
fenced.

## Storage and durability

| Backend | Status | Coordination | Default durability |
| --- | --- | --- | --- |
| SQLite | Supported | Single writer, immediate claim transactions | WAL + `synchronous=FULL` |
| PostgreSQL | Supported | pgx, native UUID, `FOR UPDATE SKIP LOCKED` | TLS required + `synchronous_commit=on` |
| Turso/libSQL | Experimental | Smoke-test scope only | Backend dependent |

Set `store.DurabilityBalanced` explicitly when lower latency is more important
than the strict default. SQLite balanced mode uses `synchronous=NORMAL`: the
database remains consistent, but the newest acknowledged commits can roll back
after power loss. PostgreSQL balanced mode uses `synchronous_commit=local`: it
still waits for local WAL flush, but does not wait for synchronous replicas, so
a failover can lose an acknowledged commit. Async admission has no local disk
spool; use durable publish when a successful response must imply a committed
transaction.
Processed deliveries are retained for seven days and schedule-run history for
30 days by default. Dead letters are retained indefinitely unless
`Options.DeadLetterRetention` is set explicitly.
SQLite checkpoint intervals use a 30-second default; a nonzero
`Options.CheckpointInterval` below `blockqueue.MinimumCheckpointInterval` is
rejected at startup instead of being silently clamped.

Core Prometheus collectors cover persistence outcomes and lag, pending message
and byte budgets, flush behavior, delivery operations, checkpoints, scheduler
lag and health, lease-reaper health, PostgreSQL notification-listener health,
and bounded maintenance passes. Supply `Options.MetricRegisterer` to isolate
collectors in an application-owned registry, or set `Options.DisableMetrics`
for a fully no-op metrics path.

## Compatibility policy

BlockQueue is pre-1.0. The root package, `worker`, `httpapi`, `store`,
`store/sqlite`, and `store/postgres` are supported public APIs. Patch releases
within a minor line preserve their source and persistence contracts. A minor
release may make a breaking change only when it is called out in the changelog
and accompanied by migration guidance.

The HTTP `/v1` contract remains compatible throughout the v0.3 release line;
additive fields and endpoints may be introduced. `store/turso` is experimental
and is limited to smoke-test coverage. Internal packages, dashboard assets, and
the standalone binary's implementation details are not import contracts.

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem ./...
```

An embedding `http.Server` should set `WriteTimeout` to at least 65 seconds:
the HTTP claim contract permits a 60-second long poll and needs response-write
headroom. The standalone binary applies this floor by default.

The SQLite and PostgreSQL contract suite is shared. PostgreSQL tests create a
random schema per run, remove it during cleanup, and refuse a database whose
name does not end in `_test`:

```bash
createdb blockqueue_test
BLOCKQUEUE_TEST_POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/blockqueue_test?sslmode=disable' \
  go test -count=1 ./...
```

CI runs the complete suite and race detector against both storage backends,
plus lint, staticcheck, vet, govulncheck, and guarded PostgreSQL benchmark smoke. Benchmark scenarios and
exact persisted-row checks are documented in
[benchmark/README.md](benchmark/README.md).

For component boundaries and lock ownership, see
[docs/architecture.md](docs/architecture.md).

## Roadmap

- v0.3.x: release hardening and focused test helpers without schema changes.
- v0.4: tracing hooks and cross-language HTTP client ergonomics.
- Later: versioned workflow/DAG orchestration as a separate layer over the
  queue, after the scheduler and worker runtime have production evidence.

## Security

Report suspected vulnerabilities through
[GitHub private vulnerability reporting](SECURITY.md), not a public issue.

## License

Apache License 2.0.
