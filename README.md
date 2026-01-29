# BlockQueue

BlockQueue is a cost-effective, durable, and lightweight message queue system designed for simplicity and reliability. Built on top of SQLite (with WAL mode), it offers transactional integrity and low-latency persistence without the operational complexity of distributed message brokers like Kafka or the memory constraints of Redis.

It supports multiple storage backends including SQLite, PostgreSQL, and Turso (LibSQL), making it versatile for both single-node deployments and scalable infrastructure.

## Key Features

*   **Transactional Durability**: All messages are persisted to disk using atomic transactions, ensuring data integrity even during system failures.
*   **High Throughput**: Optimized write buffering and batch processing capabilities allow handling thousands of messages per second with minimal latency.
*   **Scheduled Delivery**: Built-in support for delayed message publishing, allowing messages to become visible only after a specific duration.
*   **Dead Letter Queue (DLQ)**: Automatic handling of failed message deliveries with a dedicated inspection and replay mechanism.
*   **Pub/Sub Architecture**: Flexible topic-based routing with multiple subscriber support, allowing fan-out patterns and decoupled services.
*   **Active Queue Inspection**: Peek into "pending" and "delivered" messages without consuming them, useful for debugging and monitoring visibility timeouts.
*   **Built-in Dashboard**: A modern, dark-mode web interface for monitoring topics, managing subscribers, and inspecting dead letter queues in real-time.
*   **Multiple Backends**: First-class support for SQLite (default), PostgreSQL, and Turso.

## Installation

### Binary Distribution
Download the latest release for your platform or build from source:

```bash
go build -o blockqueue ./cmd/blockqueue
```

### Go Library
Integrate BlockQueue directly into your Go application:

```bash
go get -u github.com/yudhasubki/blockqueue
```

### Embedding BlockQueue (UI + API)
You can mount BlockQueue's API and Dashboard into your existing Go application

```go
func main() {
    // 1. Initialize BlockQueue
    bq := blockqueue.New(sqlite.New("blockqueue.db", nil), blockqueue.BlockQueueOption{})
    defer bq.Close()
    
    bqHttp := &blockqueue.Http{Stream: bq, UIPath: "./ui"}

    mux := http.NewServeMux()
    // Mount the handler with StripPrefix to handle subpath correctly
    mux.Handle("/admin/queue/", http.StripPrefix("/admin/queue", bqHttp.Router()))

    http.ListenAndServe(":8080", mux)
}
```

## Quick Start

### Running the Server

Start the BlockQueue server using the HTTP mode:

```bash
./blockqueue http -config=config.yaml
```

**Configuration (config.yaml):**

```yaml
http:
  port: 8080
  shutdown: "30s"
  driver: "sqlite"
sqlite:
  db_name: "blockqueue.db"
  busy_timeout: 5000
write_buffer:
  batch_size: 100
  flush_interval: "100ms"
  buffer_size: 10000
```

### Accessing the Dashboard

Once the server is running, access the monitoring dashboard at:

`http://localhost:8080`

The dashboard provides visual management for:
*   Real-time topic statistics (pending/unacked messages).
*   Subscriber health and configuration.
*   Dead Letter Queue inspection and message replay.

## API Usage

BlockQueue exposes a RESTful HTTP API for easy integration with any language.

### Topic Management

**Create a Topic**
```bash
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "subscribers": [
      {
        "name": "payment-processor",
        "option": {
          "max_attempts": 5,
          "visibility_duration": "30s"
        }
      }
    ]
  }'
```

### Publishing Messages

**Standard Publish**
```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"message": "order_id:1022"}'
```

**Delayed Publish (Scheduled)**
To make a message visible only after a delay (e.g., 10 minutes):
```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "message": "follow_up_email",
    "delay": "10m"
  }'
```

**Batch Publish**
For higher throughput, publish multiple messages in a single request:
```bash
curl -X POST http://localhost:8080/topics/orders/messages/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"message": "order_1"},
    {"message": "order_2"},
    {"message": "order_3", "delay": "5s"}
  ]'
```

### Consuming Messages

**Read Message (Long Polling)**
BlockQueue supports long-polling to reduce empty responses and network chatter.
```bash
curl "http://localhost:8080/topics/orders/subscribers/payment-processor?timeout=5s"
```

**Inspect Queue (Peek)**
View pending or delivered messages without consuming them. Useful for debugging.
```bash
curl "http://localhost:8080/topics/orders/subscribers/payment-processor/messages?limit=10&offset=0"
```

**Acknowledge Message**
After processing, the consumer must acknowledge the message to remove it from the queue.
```bash
curl -X DELETE "http://localhost:8080/topics/orders/subscribers/payment-processor/messages/{message_id}"
```

### Dead Letter Queue (DLQ)

Messages that exceed their `max_attempts` are automatically moved to the DLQ.

**Inspect DLQ**
```bash
curl "http://localhost:8080/topics/orders/subscribers/payment-processor/dlq?limit=10&offset=0"
```

**Replay Message**
Move a message from DLQ back to the active queue for reprocessing:
```bash
curl -X POST "http://localhost:8080/topics/orders/subscribers/payment-processor/dlq/{message_id}/replay"
```

## Performance

BlockQueue is designed for speed. By utilizing SQLite's Write-Ahead Logging (WAL) and an in-memory write buffer, it achieves high throughput suitable for most production workloads.

**SQLite Benchmark (MacBook Pro M1)**
*   **Write Throughput**: ~47,000 req/sec (100 concurrent users)
*   **Read Latency (Median)**: 1.19ms

## License

Copyright (c) 2024.
Licensed under the Apache 2.0 License.