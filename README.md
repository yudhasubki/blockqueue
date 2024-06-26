# Block Queue

**Block Queue** is a lightweight and cost-effective queue messaging system with pub/sub mechanism for a cheap, robust, reliable, and durable messaging system.

Built on the sturdy foundations of [SQLite3](https://www.sqlite.org/index.html), [NutsDB](https://github.com/nutsdb/nutsdb), and now supporting the [Turso Database](https://turso.tech/), Block Queue prioritizes efficiency by minimizing network latency and ensuring cost-effectiveness.

## Why BlockQueue

While Kafka, Redis, or SQS is an excellent product, it is quite complex and requires a lot of resources. My purpose is to build this BlockQueue for simplicity, low resources, and cheap.

## Features
- 💸 Cost-Effective: Designed with affordability in mind, Block Queue provides a budget-friendly solution for messaging needs.
- 📢 Pub/Sub Mechanism: The inclusion of a publish/subscribe mechanism allows for easy communication and real-time updates.
- 📶 Less Network Latency: Prioritizing efficiency, Block Queue minimizes network latency to persistence to enhance overall performance.

## How to Install
### Binary
You can read it on our wiki page at: https://github.com/yudhasubki/blockqueue/wiki/Welcome-to-BlockQueue

### Driver
BlockQueue supports drivers using SQLite or Turso. You can define the driver in the config.yaml under the http.driver setting (either **sqlite** or **turso**).

### Running on Go
```bash
go get -u github.com/yudhasubki/blockqueue
```

Using SQLite:
```go
    // github.com/yudhasubki/blockqueue/pkg/sqlite or you can define your own
    sqlite, err := sqlite.New(cfg.SQLite.DatabaseName, sqlite.Config{
		BusyTimeout: cfg.SQLite.BusyTimeout,
	})
    if err != nil {
        return err
    }

    // github.com/yudhasubki/blockqueue/pkg/etcd or you can define your own
    etcd, err := etcd.New(
		cfg.Etcd.Path,
		etcd.WithSync(cfg.Etcd.Sync),
	)
    if err != nil {
        return err
    }

    stream := blockqueue.New(sqlite, etcd)
    err = stream.Run(ctx)
    if err != nil {
        return err
    }
```

Using Turso:
```go
    // github.com/yudhasubki/blockqueue/pkg/sqlite or you can define your own
    sqlite, err := turso.New("libsql://dbname-username.turso.io?authToken=[TOKEN]")
    if err != nil {
        return err
    }

    // github.com/yudhasubki/blockqueue/pkg/etcd or you can define your own
    etcd, err := etcd.New(
		cfg.Etcd.Path,
		etcd.WithSync(cfg.Etcd.Sync),
	)
    if err != nil {
        return err
    }

    stream := blockqueue.New(sqlite, etcd)
    err = stream.Run(ctx)
    if err != nil {
        return err
    }
```

## Architecture

![Publish Architecture](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/publisher_architecture.png)

![Consumer Architecture](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/consumer_architecture.png)

![Failed Redelivery Architecture](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/failed_redelivery_architecture.png)

## How it works
### Create Topic

```bash
curl --location 'http://your-host/topics' \
--header 'Content-Type: application/json' \
--data '{
    "name": "cart",
    "subscribers": [
        {
            "name": "counter",
            "option": {
                "max_attempts": 5,
                "visibility_duration": "5m"
            }
        },
        {
            "name": "created",
            "option": {
                "max_attempts": 5,
                "visibility_duration": "5m"
            }
        }
    ]
}'
```

### Subscriber Options
| Key  | Value | Description |
| ------------------------ | ---------- | --------------------- |
| **max_attempts**         | 1, 2, 3    | max redeliver message |
| **visibility_duration**  | 5m, 6m, 1h | if message not ack yet message, will send **now() + visibility_duration** |

### Create New Subscribers
```bash
curl --location 'http://your-host/topics/cart/subscribers' \
--header 'Content-Type: application/json' \
--data '[
    {
        "name": "counter",
        "option": {
            "max_attempts": 5,
            "visibility_duration": "5m"
        }
    }
]
'
```

### Delete Subscriber
```bash
curl --location --request DELETE ''http://your-host/topics/{topic_name}/subscribers/{subscriber_name}'
```

### Publish Message 

```bash
curl --location 'http://your-host/topics/{topic_name}/messages' \
--header 'Content-Type: application/json' \
--data '{
    "message": "hi message from topic {topic_name}"
}'
```

### Read Message

To read a message, you just need to pass the subscriber name into URL Path and with timeout. This ensures horizontal scalability and guarantees that the message is sent once.

```bash
curl --location 'http://your-host/topics/{topic_name}/subscribers/{subscriber_name}?timeout=5s'
```

> Note: A message at-least-once message delivery.

### Delete Message

After reading and successfully processing a message, you must delete it, as the message will persist based on queue retry policy on subscriber option.

```bash
curl --location --request DELETE 'http://your-host/topics/{topic_name}/subscribers/{subscriber_name}/messages/{message_id}'
```

### Subscriber Message Status

If you want to check how many unpublished or unacked message, you can immediately hit this endpoint
```bash
curl --location 'localhost:8080/topics/{your_topic}/subscribers'
```

## Roadmap
- [ ] Protocol
    - [x] HTTP
    - [ ] TCP
- [x] Metrics
- [ ] WAL
- [ ] SDK
    - [ ] Go
    - [ ] PHP
- [ ] Perfomance Test

## Acknowledgment
This package is inspired by the following:
- [Redis](https://redis.io)
- [Kafka](https://kafka.apache.org/)
- [Amazon SQS](https://aws.amazon.com/sqs/)

## License

The BlockQueue is open-sourced software licensed under the Apache 2.0 license.