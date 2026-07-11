package blockqueue

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

// setupTestDB creates a test database and runs migrations
func setupTestDB(t *testing.T, dbName string) (*sqlite.Driver, func()) {
	t.Helper()
	sqliteDB, err := sqlite.Open(filepath.Join(t.TempDir(), dbName+".db"), sqlite.Config{
		BusyTimeout: 5000,
	})
	require.NoError(t, err)

	require.NoError(t, Migrate(context.Background(), sqliteDB))

	cleanup := func() {
		require.NoError(t, sqliteDB.Close())
	}

	return sqliteDB, cleanup
}

func TestWriter_BatchFlush(t *testing.T) {
	t.Run("flushes when batch size reached", func(t *testing.T) {
		dbName := "test_wb_batch_" + uuid.NewString()[:8]
		sqliteDB, cleanup := setupTestDB(t, dbName)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDB)

		// Create topic and subscriber
		topicID := uuid.New()
		subscriberID := uuid.New()
		_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "test-topic")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberID, topicID, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriterOptions{
			BatchSize:     5, // Small batch for testing
			FlushInterval: 10 * time.Second,
		}

		wb := newWriter(ctx, database, config)
		defer wb.Close()

		// Enqueue exactly batch size messages
		for i := 0; i < 5; i++ {
			wb.Enqueue(topicID, uuid.NewString(), "test message", 0)
		}

		// Wait for flush
		time.Sleep(100 * time.Millisecond)

		// Verify messages were inserted
		var count int
		err = testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID)
		require.NoError(t, err)
		require.Equal(t, 5, count, "expected 5 messages to be flushed")
	})
}

func TestWriter_TimeFlush(t *testing.T) {
	t.Run("flushes on interval even with partial batch", func(t *testing.T) {
		dbName := "test_wb_time_" + uuid.NewString()[:8]
		sqliteDB, cleanup := setupTestDB(t, dbName)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDB)

		// Create topic and subscriber
		topicID := uuid.New()
		subscriberID := uuid.New()
		_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "test-topic")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberID, topicID, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriterOptions{
			BatchSize:     100, // Large batch - won't be reached
			FlushInterval: 50 * time.Millisecond,
		}

		wb := newWriter(ctx, database, config)
		defer wb.Close()

		// Enqueue less than batch size
		for i := 0; i < 3; i++ {
			wb.Enqueue(topicID, uuid.NewString(), "test message", 0)
		}

		// Wait for time-based flush
		time.Sleep(150 * time.Millisecond)

		// Verify messages were inserted
		var count int
		err = testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID)
		require.NoError(t, err)
		require.Equal(t, 3, count, "expected 3 messages to be flushed by timer")
	})
}

func TestWriterKeepsAsyncOnlyAdmissionInGroupCommitWindow(t *testing.T) {
	dbName := "test_wb_async_window_" + uuid.NewString()[:8]
	sqliteDB, cleanup := setupTestDB(t, dbName)
	defer cleanup()
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "async-window")
	require.NoError(t, err)
	_, err = testDB(sqliteDB).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)
	require.NoError(t, err)

	buffer := newWriter(context.Background(), newDb(sqliteDB), WriterOptions{
		BatchSize: 100, FlushInterval: 200 * time.Millisecond,
	})
	defer buffer.Close()
	require.NoError(t, buffer.EnqueueBatchContext(context.Background(), []writeRequest{{
		TopicID: topicID, MessageID: newMessageID(), Message: "async group commit", VisibleAt: time.Now().UTC(),
	}}))
	time.Sleep(30 * time.Millisecond)
	var count int
	require.NoError(t, testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID))
	require.Zero(t, count, "async-only admission must retain the configured group-commit window")
	require.Eventually(t, func() bool {
		return testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID) == nil && count == 1
	}, time.Second, 10*time.Millisecond)
}

func TestWriterFlushesIdleDurableAdmissionBeforeLongInterval(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "idle-flush.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 100, FlushInterval: 5 * time.Second,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("idle-flush")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, queue.Shutdown(ctx))
	})

	started := time.Now()
	receipt, err := queue.PublishDurable(context.Background(), topic, Message{Message: "flush now"})
	require.NoError(t, err)
	require.Equal(t, "persisted", receipt.State)
	require.Less(t, time.Since(started), time.Second,
		"idle durable admission must not wait for the five-second flush interval")
}

func TestWriter_GracefulClose(t *testing.T) {
	t.Run("flushes remaining messages on close", func(t *testing.T) {
		dbName := "test_wb_close_" + uuid.NewString()[:8]
		sqliteDB, cleanup := setupTestDB(t, dbName)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())

		database := newDb(sqliteDB)

		// Create topic and subscriber
		topicID := uuid.New()
		subscriberID := uuid.New()
		_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "test-topic")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberID, topicID, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriterOptions{
			BatchSize:     100,              // Large batch - won't be reached
			FlushInterval: 10 * time.Second, // Long interval - won't trigger
		}

		wb := newWriter(ctx, database, config)

		// Enqueue messages
		for i := 0; i < 7; i++ {
			wb.Enqueue(topicID, uuid.NewString(), "test message", 0)
		}

		// Small delay to ensure messages are in the channel
		time.Sleep(10 * time.Millisecond)

		// Close should flush remaining (cancel first, then Close waits for goroutine)
		cancel()
		wb.Close()

		// Delay for flush to complete
		time.Sleep(100 * time.Millisecond)

		// Verify every accepted message was flushed.
		var count int
		err = testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID)
		require.NoError(t, err)
		require.Equal(t, 7, count, "all messages should be flushed on close")
		err = wb.EnqueueContext(context.Background(), topicID, uuid.NewString(), "after close", 0)
		require.ErrorIs(t, err, ErrWriterClosed)
	})
}

func TestWriter_MultiTopic(t *testing.T) {
	t.Run("groups messages by topic correctly", func(t *testing.T) {
		dbName := "test_wb_multi_" + uuid.NewString()[:8]
		sqliteDB, cleanup := setupTestDB(t, dbName)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDB)

		// Create two topics with subscribers
		topic1ID := uuid.New()
		topic2ID := uuid.New()
		sub1ID := uuid.New()
		sub2ID := uuid.New()

		_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topic1ID, "topic-1")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topic2ID, "topic-2")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			sub1ID, topic1ID, "sub-1", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			sub2ID, topic2ID, "sub-2", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriterOptions{
			BatchSize:     10,
			FlushInterval: 50 * time.Millisecond,
		}

		wb := newWriter(ctx, database, config)
		defer wb.Close()

		// Enqueue to both topics
		for i := 0; i < 3; i++ {
			wb.Enqueue(topic1ID, uuid.NewString(), "topic1 message", 0)
			wb.Enqueue(topic2ID, uuid.NewString(), "topic2 message", 0)
		}

		// Wait for flush
		time.Sleep(150 * time.Millisecond)

		// Verify messages per topic
		var count1, count2 int
		err = testDB(sqliteDB).Get(&count1, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topic1ID)
		require.NoError(t, err)
		err = testDB(sqliteDB).Get(&count2, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topic2ID)
		require.NoError(t, err)

		require.Equal(t, 3, count1, "expected 3 messages for topic 1")
		require.Equal(t, 3, count2, "expected 3 messages for topic 2")
	})
}

func TestWriter_Concurrent(t *testing.T) {
	t.Run("handles concurrent enqueue safely", func(t *testing.T) {
		dbName := "test_wb_conc_" + uuid.NewString()[:8]
		sqliteDB, cleanup := setupTestDB(t, dbName)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		database := newDb(sqliteDB)

		// Create topic and subscriber
		topicID := uuid.New()
		subscriberID := uuid.New()
		_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "test-topic")
		require.NoError(t, err)
		_, err = testDB(sqliteDB).Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			subscriberID, topicID, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		config := WriterOptions{
			BatchSize:     50,
			FlushInterval: 50 * time.Millisecond,
		}

		wb := newWriter(ctx, database, config)

		// Concurrent producers
		var wg sync.WaitGroup
		msgCount := 100
		goroutines := 10

		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < msgCount/goroutines; i++ {
					wb.Enqueue(topicID, uuid.NewString(), "concurrent message", 0)
				}
			}()
		}

		wg.Wait()
		wb.Close()

		// Small delay for final flush
		time.Sleep(100 * time.Millisecond)

		// Verify all messages were inserted.
		var count int
		err = testDB(sqliteDB).Get(&count, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID)
		require.NoError(t, err)
		require.Equal(t, msgCount, count, "expected all concurrent messages to be flushed")
	})
}

func TestWriter_ConcurrentClosePreservesAcceptedMessages(t *testing.T) {
	dbName := "test_wb_atomic_close_" + uuid.NewString()[:8]
	sqliteDB, cleanup := setupTestDB(t, dbName)
	defer cleanup()

	database := newDb(sqliteDB)
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(sqliteDB).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "atomic-close-topic")
	require.NoError(t, err)
	_, err = testDB(sqliteDB).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID,
		topicID,
		"atomic-close-subscriber",
		`{"max_attempts":3,"visibility_duration":"30s"}`,
	)
	require.NoError(t, err)

	wb := newWriter(context.Background(), database, WriterOptions{
		BatchSize:     50,
		FlushInterval: time.Second,
	})

	var accepted atomic.Int64
	for i := 0; i < 10; i++ {
		err := wb.EnqueueContext(context.Background(), topicID, uuid.NewString(), "before-close", 0)
		require.NoError(t, err)
		accepted.Add(1)
	}

	start := make(chan struct{})
	errs := make(chan error, 1000)
	var publishers sync.WaitGroup
	for i := 0; i < 50; i++ {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			<-start
			for j := 0; j < 20; j++ {
				err := wb.EnqueueContext(context.Background(), topicID, uuid.NewString(), "during-close", 0)
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrWriterClosed):
					return
				default:
					errs <- err
					return
				}
			}
		}()
	}

	close(start)
	time.Sleep(time.Millisecond)
	wb.Close()
	publishers.Wait()
	close(errs)
	for publishErr := range errs {
		require.NoError(t, publishErr)
	}

	var stored int64
	err = testDB(sqliteDB).Get(&stored, "SELECT COUNT(*) FROM messages WHERE topic_id = ?", topicID)
	require.NoError(t, err)
	require.Equal(t, accepted.Load(), stored)
}
