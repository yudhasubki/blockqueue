package blockqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

// BenchmarkPublish tests single message publish throughput
func BenchmarkPublish(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_publish", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_publish")
	defer os.Remove("bench_publish-shm")
	defer os.Remove("bench_publish-wal")

	runBenchMigrate(b, sqliteDb)

	bq := New(sqliteDb, BlockQueueOption{
		WriteBufferConfig: WriteBufferConfig{
			BatchSize:     100,
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    10000,
		},
		CheckpointInterval: 30 * time.Second,
	})

	ctx := context.Background()
	request := bqio.Topic{
		Name: "bench-topic",
		Subscribers: bqio.Subscribers{
			{Name: "bench-subscriber"},
		},
	}
	topic := request.Topic()
	subscribers := request.Subscriber(topic.Id)

	bq.Run(ctx)
	bq.AddJob(ctx, topic, subscribers)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bq.Publish(ctx, topic, bqio.Publish{
			Message: "benchmark message",
		})
	}

	b.StopTimer()
	bq.Close()
}

// BenchmarkPublishParallel tests concurrent publish throughput
func BenchmarkPublishParallel(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_parallel", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_parallel")
	defer os.Remove("bench_parallel-shm")
	defer os.Remove("bench_parallel-wal")

	runBenchMigrate(b, sqliteDb)

	bq := New(sqliteDb, BlockQueueOption{
		WriteBufferConfig: WriteBufferConfig{
			BatchSize:     100,
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    10000,
		},
		CheckpointInterval: 30 * time.Second,
	})

	ctx := context.Background()
	request := bqio.Topic{
		Name: "bench-parallel-topic",
		Subscribers: bqio.Subscribers{
			{Name: "bench-parallel-subscriber"},
		},
	}
	topic := request.Topic()
	subscribers := request.Subscriber(topic.Id)

	bq.Run(ctx)
	bq.AddJob(ctx, topic, subscribers)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bq.Publish(ctx, topic, bqio.Publish{
				Message: "parallel benchmark message",
			})
		}
	})

	b.StopTimer()
	bq.Close()
}

// BenchmarkBatchEnqueue tests batch insert performance
func BenchmarkBatchEnqueue(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_batch", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_batch")
	defer os.Remove("bench_batch-shm")
	defer os.Remove("bench_batch-wal")

	runBenchMigrate(b, sqliteDb)

	database := newDb(sqliteDb)

	// Create topic and subscriber
	topicId := uuid.New()
	subscriberId := uuid.New()
	sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "bench-batch-topic")
	sqliteDb.Database.Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberId, topicId, "bench-batch-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)

	// Prepare batch
	batchSize := 100
	requests := make([]writeRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		requests[i] = writeRequest{
			TopicId:   topicId,
			MessageId: uuid.NewString(),
			Message:   "batch benchmark message",
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Regenerate message IDs for each iteration
		for j := range requests {
			requests[j].MessageId = uuid.NewString()
		}
		database.batchEnqueueToSubscribers(ctx, requests)
	}
}

// BenchmarkDequeue tests dequeue performance
func BenchmarkDequeue(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_dequeue", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_dequeue")
	defer os.Remove("bench_dequeue-shm")
	defer os.Remove("bench_dequeue-wal")

	runBenchMigrate(b, sqliteDb)

	database := newDb(sqliteDb)

	// Create topic and subscriber
	topicId := uuid.New()
	subscriberId := uuid.New()
	sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "bench-dequeue-topic")
	sqliteDb.Database.Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberId, topicId, "bench-dequeue-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)

	ctx := context.Background()

	// Pre-populate messages
	for i := 0; i < b.N*10; i++ {
		database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "dequeue benchmark message")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		database.dequeueMessages(ctx, subscriberId, 10, 30*time.Second)
	}
}

// BenchmarkAck tests acknowledgment performance
func BenchmarkAck(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_ack", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_ack")
	defer os.Remove("bench_ack-shm")
	defer os.Remove("bench_ack-wal")

	runBenchMigrate(b, sqliteDb)

	database := newDb(sqliteDb)

	// Create topic and subscriber
	topicId := uuid.New()
	subscriberId := uuid.New()
	sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "bench-ack-topic")
	sqliteDb.Database.Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberId, topicId, "bench-ack-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)

	ctx := context.Background()

	// Pre-populate messages
	messageIds := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		messageIds[i] = uuid.NewString()
		database.enqueueToSubscribers(ctx, topicId, messageIds[i], "ack benchmark message")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		database.ackSubscriberMessage(ctx, subscriberId, messageIds[i])
	}
}

// BenchmarkEndToEnd tests full publish→consume→ack cycle
func BenchmarkEndToEnd(b *testing.B) {
	sqliteDb, err := sqlite.New("bench_e2e", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sqliteDb.Database.Close()
	defer os.Remove("bench_e2e")
	defer os.Remove("bench_e2e-shm")
	defer os.Remove("bench_e2e-wal")

	runBenchMigrate(b, sqliteDb)

	database := newDb(sqliteDb)

	// Create topic and subscriber
	topicId := uuid.New()
	subscriberId := uuid.New()
	sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "bench-e2e-topic")
	sqliteDb.Database.Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberId, topicId, "bench-e2e-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		messageId := uuid.NewString()

		// Publish
		database.enqueueToSubscribers(ctx, topicId, messageId, "e2e benchmark message")

		// Consume
		database.dequeueMessages(ctx, subscriberId, 1, 30*time.Second)

		// Ack
		database.ackSubscriberMessage(ctx, subscriberId, messageId)
	}
}

// Helper to run migrations for benchmarks
func runBenchMigrate(b *testing.B, db *sqlite.SQLite) {
	b.Helper()

	migrations := []string{
		`CREATE TABLE IF NOT EXISTS topics (
			id VARCHAR(36) PRIMARY KEY,
			name VARCHAR(255) NOT NULL UNIQUE,
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS topic_subscribers (
			id VARCHAR(36) PRIMARY KEY,
			topic_id VARCHAR(36) NOT NULL,
			name VARCHAR(255) NOT NULL,
			option TEXT DEFAULT '{}',
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS topic_messages (
			id VARCHAR(36) PRIMARY KEY,
			topic_id VARCHAR(36) NOT NULL,
			message TEXT NOT NULL,
			status VARCHAR(15) DEFAULT 'pending',
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS subscriber_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			subscriber_id VARCHAR(36) NOT NULL,
			topic_id VARCHAR(36) NOT NULL,
			message_id VARCHAR(50) NOT NULL,
			message TEXT NOT NULL,
			status VARCHAR(15) DEFAULT 'pending',
			retry_count INTEGER DEFAULT 0,
			visible_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (subscriber_id) REFERENCES topic_subscribers(id) ON DELETE CASCADE,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_poll ON subscriber_messages(subscriber_id, status, visible_at)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_ack ON subscriber_messages(message_id, subscriber_id)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_topic ON subscriber_messages(topic_id)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_pending ON subscriber_messages(subscriber_id, visible_at) WHERE status = 'pending'`,
	}

	for _, migration := range migrations {
		_, err := db.Database.Exec(migration)
		if err != nil {
			b.Fatalf("failed to run migration: %v", err)
		}
	}
}
