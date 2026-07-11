<p align="center">
  <img src="docs/img/blockqueue-logo.png" alt="BlockQueue logo" width="176">
</p>

<h1 align="center">BlockQueue</h1>

<p align="center">
  A durable, embeddable message queue for Go, backed by SQLite or PostgreSQL.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/yudhasubki/blockqueue"><img src="https://pkg.go.dev/badge/github.com/yudhasubki/blockqueue.svg" alt="Go Reference"></a>
  <img src="https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.23+">
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

## What v0.2.0 provides

- Durable publish is the default in the Go API; explicit async publish is
  available when admission latency matters more than crash durability.
- A weighted writer budget limits both pending message count and bytes. A
  reservation is released only after the database transaction finishes.
- Claims use database locking, a new receipt token for every delivery lease,
  idempotent ACK, fenced stale receipts, delayed NACK, and lease extension.
- Priority, delayed delivery, absolute RFC3339 scheduling, recurring five-field
  cron schedules, IANA timezones, run history, and overlap protection.
- Transactional, checksummed embedded schema migrations.
- `/livez`, `/readyz`, bounded retention, adaptive SQLite checkpoints, and
  optional Prometheus metrics.

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

## Delivery contract

- Delivery is at-least-once. Consumers must make side effects idempotent.
- A claim owns a delivery only for its current receipt token and lease. ACK,
  NACK, and lease extension reject stale receipts.
- `Publish` waits for the canonical message and all subscriber delivery rows to
  commit. `PublishAsync` guarantees bounded process-local admission, not crash
  durability.
- The database is authoritative. PostgreSQL notifications and in-memory wakeups
  reduce latency but are never required for correctness.
- Built-in authentication and exactly-once execution are outside the project
  scope. Protect the HTTP server with a private network or reverse proxy.

## Storage and durability

| Backend | Status | Coordination | Default durability |
| --- | --- | --- | --- |
| SQLite | Supported | Single writer, immediate claim transactions | WAL + `synchronous=FULL` |
| PostgreSQL | Supported | pgx, native UUID, `FOR UPDATE SKIP LOCKED` | TLS required + `synchronous_commit=on` |
| Turso/libSQL | Experimental | Smoke-test scope only | Backend dependent |

Set `store.DurabilityBalanced` explicitly when lower latency is more important
than the strict default. Async admission has no local disk spool; use durable
publish when a successful response must imply a committed transaction.
Processed deliveries are retained for seven days and schedule-run history for
30 days by default. Dead letters are retained indefinitely unless
`Options.DeadLetterRetention` is set explicitly.

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem ./...
```

The SQLite and PostgreSQL contract suite is shared. PostgreSQL tests create a
random schema per run, remove it during cleanup, and refuse a database whose
name does not end in `_test`:

```bash
createdb blockqueue_test
BLOCKQUEUE_TEST_POSTGRES_URL='postgres://postgres:postgres@127.0.0.1:5432/blockqueue_test?sslmode=disable' \
  go test -count=1 ./...
```

CI runs the complete suite and race detector against both storage backends,
plus vet and a guarded PostgreSQL benchmark smoke. Benchmark scenarios and
exact persisted-row checks are documented in
[benchmark/README.md](benchmark/README.md).

For component boundaries and lock ownership, see
[docs/architecture.md](docs/architecture.md).

## License

Apache License 2.0.
