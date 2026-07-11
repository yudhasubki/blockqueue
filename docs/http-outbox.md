# Transactional outbox for HTTP publishers

An HTTP request cannot participate in the transaction that changes application
data in another process or database. When an application must commit business
state and eventually publish a BlockQueue message without a loss window, store
an outbox row in the same local transaction as the business change.

```sql
CREATE TABLE application_outbox (
    id              VARCHAR(128) PRIMARY KEY,
    topic           VARCHAR(150) NOT NULL,
    message         TEXT NOT NULL,
    headers         TEXT NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at    TIMESTAMP NULL,
    last_error      TEXT NULL
);
```

The application transaction writes both records atomically:

```sql
BEGIN;
UPDATE orders SET status = 'paid' WHERE id = :order_id;
INSERT INTO application_outbox (id, topic, message)
VALUES (:event_id, 'orders.paid', :json_payload);
COMMIT;
```

Use a stable, globally unique outbox `id`. The relay sends it as BlockQueue's
`idempotency_key` and requests a durable response:

```http
POST /v1/topics/orders.paid/messages?wait_for=commit
Content-Type: application/json

{
  "message": "{\"order_id\":\"order-42\"}",
  "idempotency_key": "order-paid-018f..."
}
```

Only mark the outbox row published after HTTP `200`. Do not mark it published
after async `202`, because that response guarantees process-local admission,
not a committed queue transaction. On a timeout, disconnect, `5xx`, or unknown
response, retry the same payload with the same idempotency key. A crash after
BlockQueue commits but before `published_at` is updated is safe: the next relay
attempt resolves to the same canonical message instead of creating another
fan-out.

PostgreSQL relays can claim rows in short transactions with `FOR UPDATE SKIP
LOCKED`; SQLite relays should use one short writer transaction and a bounded
batch. Never hold an application transaction open across the HTTP call. Claim
or read a batch, commit, publish it, then mark successful rows in a separate
short transaction.

Outbox retention must exceed the maximum relay retry window. BlockQueue's
idempotency guarantee lasts while the canonical message is retained, so outbox
IDs must never be reused after message retention removes the original row.
Monitor oldest unpublished age, retry count, and terminal authentication or
validation failures. Authentication failures and permanent `4xx` responses
require operator action; transient failures retain the row for retry.
