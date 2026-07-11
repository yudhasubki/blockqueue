# Transactional producer and consumer

This example keeps an application `orders` table and BlockQueue in the same
SQLite database. It demonstrates two separate atomic boundaries:

```text
Producer: INSERT order + PublishTx      -> one commit
Consumer: UPDATE order + AckDeliveryTx -> another commit, later
```

The producer transaction does not wait for the consumer. Once it commits, the
order and delivery are durable and the consumer can claim the delivery.

The consumer performs work later. If its order update or receipt-fenced ACK
fails, `WithTx` rolls both changes back. The unacknowledged delivery becomes
eligible for retry after its lease expires.

Run it from the repository root:

```bash
go run ./example/transactional
```

Use this pattern only when the application tables and BlockQueue use the same
physical database. Remote APIs, email, files, and other external side effects
cannot participate in the database transaction and must be idempotent.
