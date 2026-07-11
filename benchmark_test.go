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
	forEachBenchmarkBackendWithWriter(b, WriterOptions{
		BatchSize: 100, FlushInterval: time.Millisecond,
		MaxPendingMessages: 200_000, MaxPendingBytes: 512 << 20,
	}, run)
}

func forEachBenchmarkBackendWithWriter(
	b *testing.B,
	writerOptions WriterOptions,
	run func(*testing.B, *Queue, Topic),
) {
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
			queue, topic := benchmarkQueue(b, backend.open(b), writerOptions)
			run(b, queue, topic)
		})
	}
}

func benchmarkQueue(b *testing.B, driver store.Driver, writerOptions WriterOptions) (*Queue, Topic) {
	b.Helper()
	queue := New(driver, Options{Writer: writerOptions})
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

// BenchmarkPublishDurableIdle isolates the commit latency of a single durable
// admission when no other publisher is available for group commit.
func BenchmarkPublishDurableIdle(b *testing.B) {
	forEachBenchmarkBackendWithWriter(b, WriterOptions{
		BatchSize: 100, FlushInterval: 50 * time.Millisecond,
		MaxPendingMessages: 200_000, MaxPendingBytes: 512 << 20,
	}, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := queue.Publish(ctx, topic, Message{Message: "idle durable benchmark"}); err != nil {
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

const largeScheduleHistoryRows = 1_000_000

// BenchmarkClaim100WithMillionScheduleRuns is opt-in because its fixture is
// intentionally large. It proves claim latency is independent of historical
// schedule_runs growth and guards against reintroducing a global run sweep.
func BenchmarkClaim100WithMillionScheduleRuns(b *testing.B) {
	if os.Getenv("BLOCKQUEUE_BENCH_LARGE_HISTORY") != "1" {
		b.Skip("set BLOCKQUEUE_BENCH_LARGE_HISTORY=1 to seed one million schedule runs")
	}
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		schedule, err := queue.CreateSchedule(context.Background(), topic, ScheduleInput{
			Name: "large-history", CronExpression: "0 0 * * *", Timezone: "UTC", Message: "history",
		})
		if err != nil {
			b.Fatal(err)
		}
		seedScheduleRunHistory(b, queue, schedule.ID, largeScheduleHistoryRows)

		ctx := context.Background()
		const warmupRows = 1000
		warmup := make([]Message, warmupRows)
		for index := range warmup {
			warmup[index].Message = "large-history cache warmup"
		}
		if _, err := queue.BatchPublish(ctx, topic, warmup); err != nil {
			b.Fatal(err)
		}
		for start := 0; start < warmupRows; start += 100 {
			claimed, err := queue.Claim(ctx, topic, "worker", 100, time.Minute)
			if err != nil || len(claimed) != 100 {
				b.Fatalf("warmup claim: count=%d err=%v", len(claimed), err)
			}
			items := make([]BatchAckItem, len(claimed))
			for index, delivery := range claimed {
				items[index] = BatchAckItem{MessageID: delivery.ID, ReceiptToken: delivery.ReceiptToken}
			}
			for _, result := range queue.BatchAckDeliveries(ctx, topic, "worker", items) {
				if result.Status != "processed" {
					b.Fatalf("warmup ACK: %+v", result)
				}
			}
		}
		for iteration := 0; iteration < b.N; iteration++ {
			batch := make([]Message, 100)
			for index := range batch {
				batch[index].Message = "claim with large schedule history"
			}
			if _, err := queue.BatchPublish(ctx, topic, batch); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(largeScheduleHistoryRows, "history_rows")
		b.ReportAllocs()
		b.SetBytes(100)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			messages, err := queue.Claim(ctx, topic, "worker", 100, time.Minute)
			if err != nil {
				b.Fatal(err)
			}
			if len(messages) != 100 {
				b.Fatalf("claim count=%d", len(messages))
			}
		}
		b.StopTimer()
		verifyBenchmarkRows(b, queue, int64(warmupRows+b.N*100))
	})
}

func seedScheduleRunHistory(b *testing.B, queue *Queue, scheduleID string, rows int) {
	b.Helper()
	var query string
	switch queue.db.dialect.kind() {
	case store.DialectSQLite:
		query = queue.db.Conn().Rebind(`
			WITH RECURSIVE sequence(value) AS (
				SELECT 1
				UNION ALL
				SELECT value + 1 FROM sequence WHERE value < ?
			)
			INSERT INTO schedule_runs (id, schedule_id, scheduled_for, status, created_at, finished_at)
			SELECT printf('00000000-0000-4000-8000-%012x', value), ?,
			       datetime('2020-01-01', '+' || value || ' seconds'), 'completed',
			       datetime('2020-01-01', '+' || value || ' seconds'),
			       datetime('2020-01-01', '+' || value || ' seconds')
			FROM sequence`)
	case store.DialectPostgres:
		query = queue.db.Conn().Rebind(`
			INSERT INTO schedule_runs (id, schedule_id, scheduled_for, status, created_at, finished_at)
			SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(value), 12, '0'))::uuid,
			       ?, TIMESTAMPTZ '2020-01-01 00:00:00+00' + value * INTERVAL '1 second',
			       'completed', TIMESTAMPTZ '2020-01-01 00:00:00+00' + value * INTERVAL '1 second',
			       TIMESTAMPTZ '2020-01-01 00:00:00+00' + value * INTERVAL '1 second'
			FROM generate_series(1, CAST(? AS INTEGER)) AS value`)
	default:
		b.Fatalf("unsupported benchmark dialect %q", queue.db.dialect.kind())
	}
	args := []any{rows, scheduleID}
	if queue.db.dialect.kind() == store.DialectPostgres {
		args = []any{scheduleID, rows}
	}
	if _, err := queue.db.Conn().Exec(query, args...); err != nil {
		b.Fatal(err)
	}
	var actual int
	if err := queue.db.Conn().Get(&actual, queue.db.Conn().Rebind(
		"SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?"), scheduleID); err != nil {
		b.Fatal(err)
	}
	if actual != rows {
		b.Fatalf("schedule history rows=%d expected=%d", actual, rows)
	}
	// The fixture represents retained history accumulated over months, not one
	// giant uncheckpointed deployment transaction. Normalize storage state
	// before timing so the benchmark measures index coupling, not fixture WAL.
	if queue.db.dialect.kind() == store.DialectSQLite {
		if _, err := queue.db.checkpointSQLite(context.Background(), sqliteCheckpointTruncate); err != nil {
			b.Fatal(err)
		}
		if _, err := queue.db.Conn().Exec("ANALYZE schedule_runs"); err != nil {
			b.Fatal(err)
		}
	} else {
		if _, err := queue.db.Conn().Exec("VACUUM (ANALYZE) schedule_runs"); err != nil {
			b.Fatal(err)
		}
		if _, err := queue.db.Conn().Exec("CHECKPOINT"); err != nil {
			b.Logf("PostgreSQL CHECKPOINT skipped (grant pg_checkpoint for fully normalized fixtures): %v", err)
		}
	}
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

func BenchmarkBatchNack100(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		claimedBatches := make([]Deliveries, b.N)
		for iteration := 0; iteration < b.N; iteration++ {
			batch := make([]Message, 100)
			for index := range batch {
				batch[index].Message = "batch nack benchmark"
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
			items := make([]BatchNackItem, len(claimedBatches[iteration]))
			for index, delivery := range claimedBatches[iteration] {
				items[index] = BatchNackItem{
					MessageID: delivery.ID, ReceiptToken: delivery.ReceiptToken,
					RetryDelay: time.Hour, Error: "batch benchmark failure",
				}
			}
			results := queue.BatchNackDeliveries(ctx, topic, "worker", items)
			for _, result := range results {
				if result.Status != DeliveryStatusPending {
					b.Fatalf("batch nack result: %+v", result)
				}
			}
		}
		b.StopTimer()
		var errorsCount int
		if err := queue.db.Conn().Get(&errorsCount, "SELECT COUNT(*) FROM delivery_errors"); err != nil {
			b.Fatal(err)
		}
		if errorsCount != b.N*100 {
			b.Fatalf("delivery error mismatch: expected=%d actual=%d", b.N*100, errorsCount)
		}
		verifyBenchmarkRows(b, queue, int64(b.N*100))
	})
}

func BenchmarkNackFailure(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, queue *Queue, topic Topic) {
		ctx := context.Background()
		claimed := make(Deliveries, 0, b.N)
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := queue.Publish(ctx, topic, Message{Message: "nack benchmark"}); err != nil {
				b.Fatal(err)
			}
			deliveries, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
			if err != nil || len(deliveries) != 1 {
				b.Fatalf("claim: count=%d err=%v", len(deliveries), err)
			}
			claimed = append(claimed, deliveries[0])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for _, delivery := range claimed {
			if err := queue.NackDelivery(
				ctx, topic, "worker", delivery.ID, delivery.ReceiptToken, time.Hour, "benchmark failure",
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		var errorsCount int
		if err := queue.db.Conn().Get(&errorsCount, "SELECT COUNT(*) FROM delivery_errors"); err != nil {
			b.Fatal(err)
		}
		if errorsCount != b.N {
			b.Fatalf("delivery error mismatch: expected=%d actual=%d", b.N, errorsCount)
		}
		verifyBenchmarkRows(b, queue, int64(b.N))
	})
}

func BenchmarkSQLiteCallerTransactionContention(b *testing.B) {
	for _, hold := range []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second} {
		b.Run("hold_"+hold.String(), func(b *testing.B) {
			driver, err := sqlite.Open(filepath.Join(b.TempDir(), "caller-transaction.db"), sqlite.Config{
				BusyTimeout: 5000,
			})
			if err != nil {
				b.Fatal(err)
			}
			queue, topic := benchmarkQueue(b, driver, WriterOptions{
				BatchSize: 100, FlushInterval: time.Millisecond,
				MaxPendingMessages: 200_000, MaxPendingBytes: 512 << 20,
			})
			ctx := context.Background()

			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				tx, err := driver.DB().BeginTx(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := queue.PublishTx(ctx, tx, topic, Message{Message: "held caller transaction"}); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				if _, err := queue.PublishAsync(ctx, topic, Message{Message: "queued writer admission"}); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				barrier := make(chan error, 1)
				go func() { barrier <- queue.writer.Barrier(ctx) }()

				b.StartTimer()
				time.Sleep(hold)
				commitErr := tx.Commit()
				barrierErr := <-barrier
				b.StopTimer()
				if commitErr != nil {
					b.Fatal(commitErr)
				}
				if barrierErr != nil {
					b.Fatal(barrierErr)
				}
			}
			verifyBenchmarkRows(b, queue, int64(b.N*2))
		})
	}
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
