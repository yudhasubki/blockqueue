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

## Benchmark

Macbook Pro M1, Apple Chip Memory 8GiB, here is some benchmarks:

### Publish 10 Virtual Users
```
k6 run --vus=10 --duration=30s --summary-trend-stats="med,p(95),p(99.9)" publish.js

data_received..............: 187 MB  6.2 MB/s
data_sent..................: 217 MB  7.2 MB/s
http_req_blocked...........: med=0s    p(95)=1µs      p(99.9)=68µs
http_req_connecting........: med=0s    p(95)=0s       p(99.9)=0s
http_req_duration..........: med=135µs p(95)=437µs    p(99.9)=7.63ms
http_req_failed............: 100.00% ✓ 1142146      ✗ 0
http_req_receiving.........: med=4µs   p(95)=21µs     p(99.9)=1.62ms
http_req_sending...........: med=2µs   p(95)=10µs     p(99.9)=344µs
http_req_tls_handshaking...: med=0s    p(95)=0s       p(99.9)=0s
http_req_waiting...........: med=124µs p(95)=407µs    p(99.9)=5.48ms
http_reqs..................: 1142146 38067.366226/s
iteration_duration.........: med=162µs p(95)=507.66µs p(99.9)=11.55ms
iterations.................: 1142146 38067.366226/s
vus........................: 10      min=10         max=10
vus_max....................: 10      min=10         max=10

38K message per seconds.
```

### Publish 100 Virtual Users
```
k6 run --vus=100 --duration=30s --summary-trend-stats="med,p(95),p(99.9)" publish.js

100 Virtual Users

data_received..............: 195 MB  6.5 MB/s
data_sent..................: 226 MB  7.5 MB/s
http_req_blocked...........: med=0s     p(95)=2µs    p(99.9)=111µs
http_req_connecting........: med=0s     p(95)=0s     p(99.9)=0s
http_req_duration..........: med=1.17ms p(95)=7.38ms p(99.9)=59.32ms
http_req_failed............: 100.00% ✓ 1186606      ✗ 0
http_req_receiving.........: med=5µs    p(95)=32µs   p(99.9)=10.6ms
http_req_sending...........: med=2µs    p(95)=14µs   p(99.9)=905.18µs
http_req_tls_handshaking...: med=0s     p(95)=0s     p(99.9)=0s
http_req_waiting...........: med=1.15ms p(95)=7.18ms p(99.9)=55.42ms
http_reqs..................: 1186606 39541.018601/s
iteration_duration.........: med=1.27ms p(95)=8.58ms p(99.9)=66.45ms
iterations.................: 1186606 39541.018601/s
vus........................: 100     min=100        max=100
vus_max....................: 100     min=100        max=100

39,5k message per seconds.
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
- [x] Perfomance Test

## Acknowledgment
This package is inspired by the following:
- [Redis](https://redis.io)
- [Kafka](https://kafka.apache.org/)
- [Amazon SQS](https://aws.amazon.com/sqs/)

## License

The BlockQueue is open-sourced software licensed under the Apache 2.0 license.