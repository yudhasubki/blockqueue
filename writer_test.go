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
	"github.com/mattn/go-sqlite3"
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
		BatchSize:     100,
		FlushInterval: 200 * time.Millisecond,
	})
	defer buffer.Close()
	require.NoError(t, buffer.EnqueueBatchContext(context.Background(), []writeRequest{{
		TopicID:   topicID,
		MessageID: newMessageID(),
		Message:   "async group commit",
		VisibleAt: time.Now().UTC(),
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
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("idle-flush")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "1m",
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

func TestWriterAbortCompletesRemainingIsolatedAdmissions(t *testing.T) {
	permanent := errors.New("injected permanent admission failure")
	blocked := make(chan struct{})
	var blockedOnce sync.Once
	persist := func(ctx context.Context, requests []writeRequest) (persistWriteResult, error) {
		if len(requests) > 1 || requests[0].Message == "permanent" {
			return persistWriteResult{}, permanent
		}
		blockedOnce.Do(func() { close(blocked) })
		<-ctx.Done()
		return persistWriteResult{}, ctx.Err()
	}
	wb := newWriter(context.Background(), &db{disableMetrics: true}, WriterOptions{
		BatchSize:          2,
		FlushInterval:      time.Hour,
		RetryMin:           time.Millisecond,
		RetryMax:           time.Millisecond,
		MaxPendingMessages: 10,
		MaxPendingBytes:    1 << 20,
		persist:            persist,
	})
	now := time.Now().UTC()
	first, err := wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "permanent",
		CreatedAt: now,
		VisibleAt: now,
	}, true)
	require.NoError(t, err)
	second, err := wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "blocked",
		CreatedAt: now,
		VisibleAt: now,
	}, true)
	require.NoError(t, err)
	<-blocked

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, wb.CloseContext(closeCtx), ErrWriterDrainTimeout)
	_, err = wb.waitAdmission(context.Background(), first)
	require.ErrorIs(t, err, permanent)
	_, err = wb.waitAdmission(context.Background(), second)
	require.ErrorIs(t, err, ErrWriterDrainTimeout)
	messages, bytes := wb.Pending()
	require.Zero(t, messages)
	require.Zero(t, bytes)
}

func TestWriterStopsAdmissionAfterUnknownPermanentStorageFailure(t *testing.T) {
	storageFailure := errors.New("injected permanent storage corruption")
	wb := newWriter(context.Background(), &db{disableMetrics: true}, WriterOptions{
		BatchSize:          1,
		FlushInterval:      time.Millisecond,
		MaxPendingMessages: 10,
		MaxPendingBytes:    1 << 20,
		persist: func(context.Context, []writeRequest) (persistWriteResult, error) {
			return persistWriteResult{}, storageFailure
		},
	})
	now := time.Now().UTC()
	admission, err := wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "fatal",
		CreatedAt: now,
		VisibleAt: now,
	}, true)
	require.NoError(t, err)
	_, err = wb.waitAdmission(context.Background(), admission)
	require.ErrorIs(t, err, storageFailure)
	require.Eventually(t, func() bool { return !wb.Healthy() && !wb.accepting.Load() }, time.Second, time.Millisecond)
	_, err = wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "rejected",
		CreatedAt: now,
		VisibleAt: now,
	}, true)
	require.ErrorIs(t, err, ErrWriterClosed)
	require.NoError(t, wb.CloseContext(context.Background()))
}

func TestDomainWriteFailureDoesNotPoisonWriterHealth(t *testing.T) {
	wb := newWriter(context.Background(), &db{disableMetrics: true}, WriterOptions{
		BatchSize:          1,
		FlushInterval:      time.Millisecond,
		MaxPendingMessages: 10,
		MaxPendingBytes:    1 << 20,
		persist: func(context.Context, []writeRequest) (persistWriteResult, error) {
			return persistWriteResult{}, ErrNoActiveSubscriber
		},
	})
	now := time.Now().UTC()
	admission, err := wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "domain failure",
		CreatedAt: now,
		VisibleAt: now,
	}, true)
	require.NoError(t, err)
	_, err = wb.waitAdmission(context.Background(), admission)
	require.ErrorIs(t, err, ErrNoActiveSubscriber)
	require.True(t, wb.Healthy())
	require.True(t, wb.accepting.Load())
	require.NoError(t, wb.CloseContext(context.Background()))
}

func TestTransientWriteErrorClassifiesRecoverableSQLiteCapacityFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{
			name: "database full value",
			err: sqlite3.Error{
				Code:         sqlite3.ErrFull,
				ExtendedCode: sqlite3.ErrFull.Extend(0),
			},
			transient: true,
		},
		{
			name: "database full wrapped pointer",
			err: errors.Join(errors.New("wrapped"), &sqlite3.Error{
				Code:         sqlite3.ErrFull,
				ExtendedCode: sqlite3.ErrFull.Extend(0),
			}),
			transient: true,
		},
		{name: "out of memory primary code", err: sqlite3.ErrNomem, transient: true},
		{name: "io out of memory extended code", err: sqlite3.ErrIoErrNoMem, transient: true},
		{name: "constraint remains permanent", err: sqlite3.ErrConstraint, transient: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.transient, isTransientWriteError(test.err))
		})
	}
}

func TestWriterShutdownUnblocksRegisteredSendersBeforeFinalDrain(t *testing.T) {
	persistStarted := make(chan struct{})
	var startedOnce sync.Once
	wb := newWriter(context.Background(), &db{disableMetrics: true}, WriterOptions{
		BatchSize:          1,
		FlushInterval:      time.Hour,
		MaxPendingMessages: 1,
		MaxPendingBytes:    1 << 20,
		RetryMin:           time.Millisecond,
		RetryMax:           time.Millisecond,
		persist: func(ctx context.Context, _ []writeRequest) (persistWriteResult, error) {
			startedOnce.Do(func() { close(persistStarted) })
			<-ctx.Done()
			return persistWriteResult{}, ctx.Err()
		},
	})
	now := time.Now().UTC()
	_, err := wb.admitOne(context.Background(), writeRequest{
		MessageID: uuid.NewString(),
		Message:   "blocked persistence",
		CreatedAt: now,
		VisibleAt: now,
	}, false)
	require.NoError(t, err)
	<-persistStarted

	firstBarrier := make(chan error, 1)
	go func() { firstBarrier <- wb.Barrier(context.Background()) }()
	require.Eventually(t, func() bool { return len(wb.queue) == 1 }, time.Second, time.Millisecond)
	secondBarrier := make(chan error, 1)
	go func() { secondBarrier <- wb.Barrier(context.Background()) }()
	time.Sleep(5 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = wb.CloseContext(ctx)
	require.ErrorIs(t, err, ErrWriterDrainTimeout)
	require.Less(t, time.Since(started), 250*time.Millisecond,
		"initiateClose must not wait behind a sender blocked on the queue")
	require.ErrorIs(t, <-secondBarrier, ErrWriterClosed)
	require.ErrorIs(t, <-firstBarrier, ErrWriterDrainTimeout)
	require.Eventually(t, func() bool {
		messages, bytes := wb.Pending()
		return messages == 0 && bytes == 0
	}, time.Second, time.Millisecond)
}
