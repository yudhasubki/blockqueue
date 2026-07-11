# Typed worker example

This example runs the v0.3 worker package against SQLite. It publishes one JSON
order, decodes it into a Go struct, and uses `Job.CompleteTx` to insert the
business effect and ACK the delivery in one transaction.

From the repository root:

```bash
go run ./example/worker
```

Press Ctrl-C after the job is processed. The worker stops claiming, drains its
active handlers, and only then shuts down the queue. The example intentionally
uses `worker-example.db`; subsequent runs safely reuse its topic and idempotent
business table.
