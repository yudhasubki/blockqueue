# Migrating to BlockQueue v0.2.0

v0.2.0 replaces the previous queue contract in place. It does not run separate
v1 and v2 engines, handlers, or schemas. The current HTTP API is mounted only at
`/v1`, while the Go API is versioned by the Go module release.

## Breaking source changes

- `BlockQueue[T]` becomes the non-generic `Queue`.
- `New[T](driver, BlockQueueOption)` becomes `New(driver, Options)`.
- Storage constructors move to `store/sqlite`, `store/postgres`, and the
  experimental `store/turso` packages.
- Go callers use root types such as `blockqueue.Topic`, `Subscriber`, `Message`,
  `Delivery`, and `Schedule`; `pkg/core` and `pkg/io` have been removed.
- `AddJob`/`DeleteJob` and listener-oriented methods become `CreateTopic`,
  `DeleteTopic`, `CreateSubscribers`, and `DeleteSubscriber`.
- `Publish` and `BatchPublish` are durable by default. Explicit low-latency
  admission uses `PublishAsync` and `BatchPublishAsync`.
- `PublishTx`/`BatchPublishTx` and delivery-side `*Tx` methods can now share a
  transaction with application tables. Prefer `Queue.WithTx`; manually owned
  transactions must be committed or rolled back by the caller.
- Receipt-less read/ACK methods are removed. Claims return a receipt token and
  ACK, NACK, or lease extension must send that exact token.
- The old single `attempt`/`retry_count` concept is replaced by
  `delivery_count` and `failure_count`. Max attempts now means maximum failures,
  so a successful claim does not itself consume the dead-letter budget.
- The old dual/unversioned HTTP routes are removed. Use `/v1/topics/...`.

## Database migration

Back up the database before deploying, stop old writers, then run:

```bash
blockqueue migrate -config config.yaml
```

`Queue.Run` performs the same migration check during embedded startup.
Migrations are embedded, ordered, transactional, and recorded in
`schema_migrations` with SHA-256 checksums. Editing an already-applied migration
causes startup to fail with a checksum error.

v0.2.0 is a clean queue-schema break and requires a new, empty database. There
is no in-place database upgrade from v0.1. The v0.2 baseline creates only the
current canonical schema; runtime publishing and consumption use `messages`
and `message_deliveries`. It also creates `delivery_errors`, cancellation
metadata, retry-policy columns, and the indexes required by the bounded
maintenance and delivery paths.

## Publish behavior

The HTTP default remains fast async admission:

```http
POST /v1/topics/orders/messages
```

It returns `202` with `state: admitted`. This guarantee lasts while the process
owns the admission; it is not crash durability. To wait for the canonical
message and every subscriber delivery row to commit:

```http
POST /v1/topics/orders/messages?wait_for=commit
```

The durable response has `state: persisted` and a definitive `duplicate`
value. Use an `idempotency_key` when a client may retry after an ambiguous
timeout. Keys are scoped by topic and deterministically produce the same
message ID while the canonical message is retained.

`delay` and `schedule_at` are mutually exclusive. `schedule_at` must be RFC3339
with a timezone; priority ranges from -1000 to 1000.

Embedded callers can eliminate the application-write/publish gap when both
tables share a database:

```go
err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
	if err := updateApplicationState(ctx, tx); err != nil {
		return err
	}
	_, err := queue.PublishTx(ctx, tx, topic, message)
	return err
})
```

Keep the callback short. SQLite's single writer means every queue write waits
behind an open caller transaction. PostgreSQL publishers share a topology fence
with one another, while subscriber/topic deletion waits for those publishers to
commit. HTTP cannot join a transaction in another process; use the outbox
pattern when application data and BlockQueue are in different databases.

## Consumer migration

Claim with long polling:

```http
POST /v1/topics/orders/subscribers/worker/claim?timeout=30s&limit=10
```

Each delivery includes `receipt_token` and `lease_expires_at`. Use them with:

```http
POST /v1/topics/orders/subscribers/worker/messages/{message_id}/ack
POST /v1/topics/orders/subscribers/worker/messages/{message_id}/nack
POST /v1/topics/orders/subscribers/worker/messages/{message_id}/lease
POST /v1/topics/orders/subscribers/worker/messages/{message_id}/snooze
POST /v1/topics/orders/subscribers/worker/messages/{message_id}/cancel
GET  /v1/topics/orders/subscribers/worker/messages/{message_id}/errors
```

A stale receipt after redelivery returns `409`. Repeating an ACK with the same
successful receipt is idempotent. Batch ACK/NACK and DLQ replay return a result
for every requested item.

Subscribers default to three failures and an exponential retry policy starting
at one second, capped at one hour, with multiplier 2 and bounded deterministic
jitter. An explicit NACK delay overrides the policy for that failure. Snooze
changes visibility without incrementing `failure_count`; cancellation is a
terminal, idempotent transition. Message-level status and fan-out cancellation
are available at:

```http
GET  /v1/topics/orders/messages/{message_id}
POST /v1/topics/orders/messages/{message_id}/cancel
```

The HTTP contract is served at `/openapi.json`. Error bodies now use RFC 9457
`application/problem+json` and include a stable machine-readable `code`.

## Safe rollout

1. Provision a new, empty v0.2 database.
2. Run the v0.2.0 migration command.
3. Start one v0.2.0 instance and verify `/livez` and `/readyz`.
4. Update consumers to claim and ACK with receipt tokens.
5. Scale out PostgreSQL instances only after the single-instance smoke test.

SQLite defaults to strict WAL durability. PostgreSQL defaults to TLS-required
connections and synchronous commit. Select balanced durability only as an
explicit latency-versus-durability decision.
