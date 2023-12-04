# Block Queue

**Block Queue** is a lightweight and cost-effective queue with pub/sub and consumer groups mechanism for a cheap, robust, reliable, and durable messaging system.

Crafted atop the robust foundations of [SQLite3](https://www.sqlite.org/index.html) and [NutsDB](https://github.com/nutsdb/nutsdb), Block Queue prioritizes efficiency by minimizing network latency and ensuring cost-effectiveness.

## Architecture

![Alt text](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/publisher_architecture.png)

![Alt text](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/consumer_architecture.png)

![Alt text](https://github.com/yudhasubki/blockqueue/blob/main/docs/img/consumer_architecture_partition.png)

## Features
- 💸 Cost-Effective: Designed with affordability in mind, Block Queue provides a budget-friendly solution for messaging needs.
- 📢 Pub/Sub Mechanism: The inclusion of a publish/subscribe mechanism allows for easy communication and real-time updates.
- 📦 Consumer Group: Block Queue facilitates message consumer group, enabling efficient data distribution similar to Kafka or Redis Stream for improved scalability.
- 📶 Less Network Latency: Prioritizing efficiency, Block Queue minimizes network latency to persistence to enhance overall performance.

## How it works
### Create Topic

```bash
curl --location 'http://your-host/topics' \
--header 'Content-Type: application/json' \
--data '{
    "name": "{topic_name}",
    "subscribers": ["{subscriber_name}", "{subscriber_name}"]
}'
```

### Publish Message 

```bash
curl --location 'http://localhost:8080/topics/{topic_name}/messages' \
--header 'Content-Type: application/json' \
--data '{
    "message": "hi message from topic"
}'
```

### Read Message

To read a message, you need to create a unique consumer identifier. This ensures horizontal scalability and guarantees that the message is sent only once.

```bash
curl --location 'http://localhost:8080/topics/{topic_name}/subscribers/{subscriber_name}/{identifier}?timeout={5s/20s/30s}'
```

> Note: A message is only processed by one consumer, claimed by its unique identifier.

### Delete Message

After reading and successfully processing a message, you must delete it, as the message will persist indefinitely if not removed.

```bash
curl --location --request DELETE 'http://localhost:8080/topics/{topic_name}/subscribers/{subscriber_name}/{identifier}/messages/{message_id}'
```

### Check Unclaimed Message

If you want to check how many messages have not been claimed by a consumer

```bash
curl --location 'localhost:8080/topics/cart/subscribers'
```

## Roadmap
- [ ] Protocol
    - [x] HTTP
    - [ ] TCP
- [ ] SDK
    - [ ] Go
    - [ ] PHP
- [ ] Metrics

## Acknowledgment
This package is inspired by the following:
- [Redis](https://redis.io)
- [Kafka](https://kafka.apache.org/)
- [Amazon SQS](https://aws.amazon.com/sqs/)

## License

The BlockQueue is open-sourced software licensed under the Apache 2.0 license.