# Typed worker example

This example runs two workers under one `worker.Group` against SQLite. One
published JSON order fans out to independent `fulfillment` and `notifications`
deliveries. The fulfillment worker uses `Job.CompleteTx` to insert its business
effect and ACK atomically, while the notification worker ACKs automatically
after its handler returns `nil`.

From the repository root:

```bash
go run ./example/worker
```

Press Ctrl-C after both jobs are processed. The group stops both claim loops,
drains their active handlers, and only then shuts down the queue. The example
intentionally uses `worker-example.db`; subsequent runs safely reuse its topic,
subscribers, and idempotent business table.
