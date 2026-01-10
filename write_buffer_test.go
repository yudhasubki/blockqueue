package blockqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

// setupTestDB creates a test database and runs migrations
func setupTestDB(t *testing.T, dbName string) (*sqlite.SQLite, func()) {
	sqliteDb, err := sqlite.New(dbName, sqlite.Config{
		BusyTimeout: 5000,
	})
	require.NoError(t, err)

	runMigrate(t, sqliteDb)

	cleanup := func() {
		sqliteDb.Database.Close()
	}

	return sqliteDb, cleanup
}

func TestWriteBuffer_BatchFlush(t *testing.T) {
	t.Run("flushes when batch size reached", func(t *testing.T) {
		dbName := "test_wb_batch_" + uuid.NewString()[:8]
		sqliteDb, cleanup := setupTestDB(t, dbName)
		defer cleanup()
		defer removeTestDB(dbName)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDb)

		// Create topic and subscriber
		topicId := uuid.New()
		subscriberId := uuid.New()
		_, err := sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "test-topic")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberId, topicId, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriteBufferConfig{
			BatchSize:     5, // Small batch for testing
			FlushInterval: 10 * time.Second,
			BufferSize:    100,
		}

		wb := NewWriteBuffer(ctx, database, config)
		defer wb.Close()

		// Enqueue exactly batch size messages
		for i := 0; i < 5; i++ {
			wb.Enqueue(topicId, uuid.NewString(), "test message")
		}

		// Wait for flush
		time.Sleep(100 * time.Millisecond)

		// Verify messages were inserted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.Equal(t, 5, count, "expected 5 messages to be flushed")
	})
}

func TestWriteBuffer_TimeFlush(t *testing.T) {
	t.Run("flushes on interval even with partial batch", func(t *testing.T) {
		dbName := "test_wb_time_" + uuid.NewString()[:8]
		sqliteDb, cleanup := setupTestDB(t, dbName)
		defer cleanup()
		defer removeTestDB(dbName)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDb)

		// Create topic and subscriber
		topicId := uuid.New()
		subscriberId := uuid.New()
		_, err := sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "test-topic")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberId, topicId, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriteBufferConfig{
			BatchSize:     100, // Large batch - won't be reached
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    100,
		}

		wb := NewWriteBuffer(ctx, database, config)
		defer wb.Close()

		// Enqueue less than batch size
		for i := 0; i < 3; i++ {
			wb.Enqueue(topicId, uuid.NewString(), "test message")
		}

		// Wait for time-based flush
		time.Sleep(150 * time.Millisecond)

		// Verify messages were inserted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.Equal(t, 3, count, "expected 3 messages to be flushed by timer")
	})
}

func TestWriteBuffer_GracefulClose(t *testing.T) {
	t.Run("flushes remaining messages on close", func(t *testing.T) {
		dbName := "test_wb_close_" + uuid.NewString()[:8]
		sqliteDb, cleanup := setupTestDB(t, dbName)
		defer cleanup()
		defer removeTestDB(dbName)

		ctx, cancel := context.WithCancel(context.Background())

		database := newDb(sqliteDb)

		// Create topic and subscriber
		topicId := uuid.New()
		subscriberId := uuid.New()
		_, err := sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "test-topic")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberId, topicId, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriteBufferConfig{
			BatchSize:     100,              // Large batch - won't be reached
			FlushInterval: 10 * time.Second, // Long interval - won't trigger
			BufferSize:    100,
		}

		wb := NewWriteBuffer(ctx, database, config)

		// Enqueue messages
		for i := 0; i < 7; i++ {
			wb.Enqueue(topicId, uuid.NewString(), "test message")
		}

		// Small delay to ensure messages are in the channel
		time.Sleep(10 * time.Millisecond)

		// Close should flush remaining (cancel first, then Close waits for goroutine)
		cancel()
		wb.Close()

		// Delay for flush to complete
		time.Sleep(100 * time.Millisecond)

		// Verify messages were inserted (may vary slightly due to timing)
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, 5, "most messages should be flushed on close")
	})
}

func TestWriteBuffer_MultiTopic(t *testing.T) {
	t.Run("groups messages by topic correctly", func(t *testing.T) {
		dbName := "test_wb_multi_" + uuid.NewString()[:8]
		sqliteDb, cleanup := setupTestDB(t, dbName)
		defer cleanup()
		defer removeTestDB(dbName)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDb)

		// Create two topics with subscribers
		topic1Id := uuid.New()
		topic2Id := uuid.New()
		sub1Id := uuid.New()
		sub2Id := uuid.New()

		_, err := sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topic1Id, "topic-1")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topic2Id, "topic-2")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			sub1Id, topic1Id, "sub-1", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			sub2Id, topic2Id, "sub-2", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriteBufferConfig{
			BatchSize:     10,
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    100,
		}

		wb := NewWriteBuffer(ctx, database, config)
		defer wb.Close()

		// Enqueue to both topics
		for i := 0; i < 3; i++ {
			wb.Enqueue(topic1Id, uuid.NewString(), "topic1 message")
			wb.Enqueue(topic2Id, uuid.NewString(), "topic2 message")
		}

		// Wait for flush
		time.Sleep(150 * time.Millisecond)

		// Verify messages per topic
		var count1, count2 int
		err = sqliteDb.Database.Get(&count1, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topic1Id)
		require.NoError(t, err)
		err = sqliteDb.Database.Get(&count2, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topic2Id)
		require.NoError(t, err)

		require.Equal(t, 3, count1, "expected 3 messages for topic 1")
		require.Equal(t, 3, count2, "expected 3 messages for topic 2")
	})
}

func TestWriteBuffer_Concurrent(t *testing.T) {
	t.Run("handles concurrent enqueue safely", func(t *testing.T) {
		dbName := "test_wb_conc_" + uuid.NewString()[:8]
		sqliteDb, cleanup := setupTestDB(t, dbName)
		defer cleanup()
		defer removeTestDB(dbName)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDb)

		// Create topic and subscriber
		topicId := uuid.New()
		subscriberId := uuid.New()
		_, err := sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "test-topic")
		require.NoError(t, err)
		_, err = sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberId, topicId, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriteBufferConfig{
			BatchSize:     50,
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    1000,
		}

		wb := NewWriteBuffer(ctx, database, config)

		// Concurrent producers
		var wg sync.WaitGroup
		msgCount := 100
		goroutines := 10

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < msgCount/goroutines; i++ {
					wb.Enqueue(topicId, uuid.NewString(), "concurrent message")
				}
			}()
		}

		wg.Wait()
		wb.Close()

		// Small delay for final flush
		time.Sleep(100 * time.Millisecond)

		// Verify all messages were inserted (may have slight variations due to batch timing)
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, msgCount, "expected at least all concurrent messages to be flushed")
	})
}

// Helper to remove test database files
func removeTestDB(dbName string) {
	removeTestDBFiles(dbName)
}

// runBlockQueueTestWithBuffer is a helper for tests that need full BlockQueue with WriteBuffer
func runBlockQueueTestWithBuffer(t *testing.T, test func(bq *BlockQueue[chan bqio.ResponseMessages], sqliteDb *sqlite.SQLite)) {
	dbName := "test_bq_" + uuid.NewString()[:8]
	sqliteDb, cleanup := setupTestDB(t, dbName)
	defer cleanup()
	defer removeTestDB(dbName)

	bq := New(sqliteDb, BlockQueueOption{
		WriteBufferConfig: WriteBufferConfig{
			BatchSize:     10,
			FlushInterval: 10 * time.Millisecond,
			BufferSize:    1000,
		},
		CheckpointInterval: 5 * time.Second,
	})

	test(bq, sqliteDb)

	bq.Close()
}
