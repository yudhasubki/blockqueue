# Block Queue

**Block Queue** is a lightweight message queue with pub/sub mechanism for a cheap, robust, reliable, and durable messaging system.

Built on [SQLite3](https://www.sqlite.org/index.html) with support for [Turso Database](https://turso.tech/) and [PostgreSQL](https://www.postgresql.org/).

## Why BlockQueue

While Kafka, Redis, or SQS are excellent products, they are complex and require significant resources. BlockQueue is built for simplicity, low resource usage, and cost-effectiveness.

## Features
- Cost-Effective: Budget-friendly solution for messaging needs
- Pub/Sub Mechanism: Easy communication and real-time updates
- Low Latency: Minimized network latency with SQLite as default storage
- Multiple Drivers: SQLite, Turso, and PostgreSQL support

## Installation

### Binary
Download from releases or build from source:
```bash
go build -o blockqueue ./cmd/blockqueue
```

### As Library
```bash
go get -u github.com/yudhasubki/blockqueue
```

## Usage

BlockQueue can be used in two ways:

### 1. HTTP Server Mode

Start the server:
```bash
./blockqueue http -config=config.yaml
```

Example config.yaml:
```yaml
http:
  port: 8080
  shutdown: "30s"
  driver: "sqlite"
sqlite:
  db_name: "blockqueue"
  busy_timeout: 5000
write_buffer:
  batch_size: 100
  flush_interval: "100ms"
  buffer_size: 10000
```

Then use HTTP API:
```bash
# Create topic with subscriber
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "subscribers": [{"name": "processor", "option": {"max_attempts": 5, "visibility_duration": "5m"}}]
  }'

# Publish message
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"message": "order created"}'

# Read message (long-polling)
curl http://localhost:8080/topics/orders/subscribers/processor?timeout=5s

# Acknowledge message
curl -X DELETE http://localhost:8080/topics/orders/subscribers/processor/messages/{message_id}
```

### 2. Library Mode (Embedded)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yudhasubki/blockqueue"
    "github.com/yudhasubki/blockqueue/pkg/io"
    "github.com/yudhasubki/blockqueue/pkg/sqlite"
)

func main() {
    // Initialize SQLite driver
    db, err := sqlite.New("queue.db", sqlite.Config{BusyTimeout: 5000})
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Create BlockQueue instance
    stream := blockqueue.New(db, blockqueue.BlockQueueOption{
        WriteBufferConfig: blockqueue.WriteBufferConfig{
            BatchSize:     100,
            FlushInterval: 100 * time.Millisecond,
            BufferSize:    1000,
        },
    })

    if err := stream.Run(ctx); err != nil {
        log.Fatal(err)
    }
    defer stream.Close()

    // Create topic and subscriber
    request := io.Topic{
        Name: "orders",
        Subscribers: io.Subscribers{
            {
                Name: "processor",
                Option: io.SubscriberOpt{
                    MaxAttempts:        3,
                    VisibilityDuration: "1m",
                },
            },
        },
    }
    topic := request.Topic()
    stream.AddJob(ctx, topic, request.Subscriber(topic.Id))

    // Start consumer goroutine
    go func() {
        for {
            messages, err := stream.Read(ctx, topic, "processor")
            if err != nil {
                log.Printf("read error: %v", err)
                continue
            }
            for _, msg := range messages {
                log.Printf("received: %s", msg.Message)
                stream.Ack(ctx, topic, "processor", msg.Id)
            }
        }
    }()

    // Publish messages
    for i := 0; i < 10; i++ {
        stream.Publish(ctx, topic, io.Publish{
            Message: fmt.Sprintf("order-%d", i),
        })
    }
}
```

## Drivers

### SQLite (Recommended)
Best for single-node deployments. Highest throughput with minimal latency.

```go
db, _ := sqlite.New("queue.db", sqlite.Config{
    BusyTimeout: 5000,
    CacheSize:   -4000,  // 4MB cache
    MmapSize:    0,      // Disable mmap for minimal memory
})
```

### PostgreSQL
For multi-client scenarios or when you already have PostgreSQL infrastructure.

```go
db, _ := postgre.New(postgre.Config{
    Host:     "localhost",
    Username: "user",
    Password: "pass",
    Name:     "blockqueue",
    Port:     5432,
})
```

### Turso
For edge deployments with LibSQL.

```go
db, _ := turso.New("libsql://your-db.turso.io?authToken=TOKEN")
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| /topics | POST | Create topic with subscribers |
| /topics/{topic}/messages | POST | Publish message |
| /topics/{topic}/subscribers/{sub} | GET | Read messages (long-polling) |
| /topics/{topic}/subscribers/{sub}/messages/{id} | DELETE | Acknowledge message |
| /topics/{topic}/subscribers | POST | Add subscribers |
| /topics/{topic}/subscribers/{sub} | DELETE | Remove subscriber |

### Subscriber Options
| Option | Example | Description |
|--------|---------|-------------|
| max_attempts | 5 | Maximum redelivery attempts |
| visibility_duration | 5m | Time before unacked message is redelivered |

## Benchmark

MacBook Pro M1, 8GB RAM

### SQLite (100 VUs, 10s)
```
http_reqs..................: 388908  38885/s
http_req_duration..........: med=1.19ms p(95)=7.02ms p(99.9)=30.47ms
```

### PostgreSQL (100 VUs, 10s)
```
http_reqs..................: 113626  11340/s
http_req_duration..........: med=4.87ms p(95)=18.26ms p(99.9)=275.74ms
```

## Architecture

![Publish Architecture](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/publisher_architecture.png)

![Consumer Architecture](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/consumer_architecture.png)

## Roadmap
- [x] HTTP Protocol
- [x] Metrics (Prometheus)
- [x] SQLite WAL Mode
- [x] PostgreSQL Support
- [ ] TCP Protocol
- [ ] Go SDK
- [ ] PHP SDK

## Acknowledgment
Inspired by [Redis](https://redis.io), [Kafka](https://kafka.apache.org/), and [Amazon SQS](https://aws.amazon.com/sqs/).

## License
Apache 2.0 License