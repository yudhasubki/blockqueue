package blockqueue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type benchmarkBackend struct {
	name string
	open func(*testing.B) store.Driver
}

func forEachBenchmarkBackend(b *testing.B, run func(*testing.B, *Queue, Topic)) {
	b.Helper()
	backends := []benchmarkBackend{{
		name: "sqlite",
		open: func(b *testing.B) store.Driver {
			driver, err := sqlite.Open(filepath.Join(b.TempDir(), "benchmark.db"), sqlite.Config{})
			if err != nil {
				b.Fatal(err)
			}
			return driver
		},
	}}
	if postgresURL := os.Getenv("BLOCKQUEUE_BENCH_POSTGRES_URL"); postgresURL != "" {
		backends = append(backends, benchmarkBackend{
			name: "postgres",
			open: func(b *testing.B) store.Driver {
				schema, err := testdb.OpenPostgreSQLSchema(
					context.Background(), postgresURL, "_bench", "blockqueue_benchmark",
				)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := schema.Close(); err != nil {
						b.Error(err)
					}
				})
				return schema.Driver
			},
		})
	}

	for _, backend := range backends {
		backend := backend
		b.Run(backend.name, func(b *testing.B) {
			queue, topic := benchmarkQueue(b, backend.open(b))
			run(b, queue, topic)
		})
	}
}

func benchmarkQueue(b *testing.B, driver store.Driver) (*Queue, Topic) {
	b.Helper()
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 100, FlushInterval: time.Millisecond,
		MaxPendingMessages: 200_000, MaxPendingBytes: 512 << 20,
	}})
	if err := queue.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
	topic := NewTopic("benchmark")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 5, VisibilityDuration: "1m", DequeueBatchSize: 1000,
	})
	if err := queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := queue.Shutdown(ctx); err != nil {
			b.Error(err)
		}
	})
	return queue, topic
}

func BenchmarkPublishDurable(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := queue.Publish(ctx, topic, Message{Message: "benchmark message"}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(b.N))
	})
}

func BenchmarkPublishAsync(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := queue.PublishAsync(ctx, topic, Message{Message: "benchmark message"}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := queue.writer.Barrier(ctx); err != nil {
			b.Fatal(err)
		}
		verifyBenchmarkRows(b, queue, int64(b.N))
	})
}

func BenchmarkBatchPublishDurable100(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		batch := make([]Message, 100)
		for index := range batch {
			batch[index].Message = "batch benchmark message"
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(batch)))
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := queue.BatchPublish(ctx, topic, batch); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(b.N*len(batch)))
	})
}

func BenchmarkClaim100(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		for iteration := 0; iteration < b.N; iteration++ {
			batch := make([]Message, 100)
			for index := range batch {
				batch[index].Message = fmt.Sprintf("claim-%d-%d", iteration, index)
			}
			if _, err := queue.BatchPublish(ctx, topic, batch); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.SetBytes(100)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			messages, err := queue.Claim(ctx, topic, "worker", 100, time.Minute)
			if err != nil {
				b.Fatal(err)
			}
			if len(messages) != 100 {
				b.Fatalf("claimed %d messages, want 100", len(messages))
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(b.N*100))
	})
}

func BenchmarkClaimAndAck(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		batch := make([]Message, b.N)
		for index := range batch {
			batch[index].Message = "ack benchmark"
		}
		for start := 0; start < len(batch); start += 1000 {
			end := min(start+1000, len(batch))
			if _, err := queue.BatchPublish(ctx, topic, batch[start:end]); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			messages, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
			if err != nil || len(messages) != 1 {
				b.Fatalf("claim: count=%d err=%v", len(messages), err)
			}
			message := messages[0]
			if err := queue.AckDelivery(ctx, topic, "worker", message.ID, message.ReceiptToken); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(b.N))
	})
}

func BenchmarkBatchAck100(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		claimedBatches := make([]Deliveries, b.N)
		for iteration := 0; iteration < b.N; iteration++ {
			batch := make([]Message, 100)
			for index := range batch {
				batch[index].Message = "batch ack benchmark"
			}
			if _, err := queue.BatchPublish(ctx, topic, batch); err != nil {
				b.Fatal(err)
			}
			claimed, err := queue.Claim(ctx, topic, "worker", 100, time.Minute)
			if err != nil || len(claimed) != 100 {
				b.Fatalf("claim: count=%d err=%v", len(claimed), err)
			}
			claimedBatches[iteration] = claimed
		}
		b.ReportAllocs()
		b.SetBytes(100)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			items := make([]BatchAckItem, len(claimedBatches[iteration]))
			for index, delivery := range claimedBatches[iteration] {
				items[index] = BatchAckItem{MessageID: delivery.ID, ReceiptToken: delivery.ReceiptToken}
			}
			results := queue.BatchAckDeliveries(ctx, topic, "worker", items)
			for _, result := range results {
				if result.Status != "processed" {
					b.Fatalf("batch ack result: %+v", result)
				}
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(b.N*100))
	})
}

func verifyBenchmarkRows(b *testing.B, queue *Queue, expected int64) {
	b.Helper()
	var messages, deliveries int64
	if err := queue.db.Conn().Get(&messages, "SELECT COUNT(*) FROM messages"); err != nil {
		b.Fatal(err)
	}
	if err := queue.db.Conn().Get(&deliveries, "SELECT COUNT(*) FROM message_deliveries"); err != nil {
		b.Fatal(err)
	}
	if messages != expected || deliveries != expected {
		b.Fatalf("persisted row mismatch: expected=%d messages=%d deliveries=%d", expected, messages, deliveries)
	}
}
