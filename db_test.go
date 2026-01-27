package blockqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

// removeTestDBFiles removes SQLite database files
func removeTestDBFiles(dbName string) {
	os.Remove(dbName)
	os.Remove(dbName + "-shm")
	os.Remove(dbName + "-wal")
}

// setupDBTestEnv creates a test database with topic and subscriber
func setupDBTestEnv(t *testing.T, dbName string) (*db, *sqlite.SQLite, uuid.UUID, uuid.UUID, func()) {
	sqliteDb, err := sqlite.New(dbName, sqlite.Config{
		BusyTimeout: 5000,
	})
	require.NoError(t, err)

	runMigrate(t, sqliteDb)

	database := newDb(sqliteDb)

	// Create topic
	topicId := uuid.New()
	_, err = sqliteDb.Database.Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicId, "test-topic")
	require.NoError(t, err)

	// Create subscriber
	subscriberId := uuid.New()
	_, err = sqliteDb.Database.Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberId, topicId, "test-subscriber", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)
	require.NoError(t, err)

	cleanup := func() {
		sqliteDb.Database.Close()
		removeTestDBFiles(dbName)
	}

	return database, sqliteDb, topicId, subscriberId, cleanup
}

func TestDB_EnqueueToSubscribers(t *testing.T) {
	t.Run("enqueues message to all subscribers", func(t *testing.T) {
		dbName := "test_db_enqueue_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, _, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()
		messageId := uuid.NewString()
		message := "test message content"

		err := database.enqueueToSubscribers(ctx, topicId, messageId, message)
		require.NoError(t, err)

		// Verify message was inserted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("enqueues to multiple subscribers", func(t *testing.T) {
		dbName := "test_db_enqueue_multi_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, _, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		// Add second subscriber
		sub2Id := uuid.New()
		_, err := sqliteDb.Database.Exec(
			"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
			sub2Id, topicId, "test-subscriber-2", `{"max_attempts":3,"visibility_duration":"30s"}`,
		)
		require.NoError(t, err)

		ctx := context.Background()
		messageId := uuid.NewString()

		err = database.enqueueToSubscribers(ctx, topicId, messageId, "test message")
		require.NoError(t, err)

		// Verify message was inserted for both subscribers
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, 2, count, "expected message to be delivered to 2 subscribers")
	})
}

func TestDB_DequeueMessages(t *testing.T) {
	t.Run("dequeues pending messages", func(t *testing.T) {
		dbName := "test_db_dequeue_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue messages
		for i := 0; i < 5; i++ {
			err := database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "message "+string(rune('A'+i)))
			require.NoError(t, err)
		}

		// Dequeue
		messages, err := database.dequeueMessages(ctx, subscriberId, 3, 30*time.Second)
		require.NoError(t, err)
		require.Len(t, messages, 3)

		// Verify status changed to delivered by querying DB
		for _, m := range messages {
			var status string
			err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE id = ?", m.Id)
			require.NoError(t, err)
			require.Equal(t, "delivered", status)
		}
	})

	t.Run("respects visibility timeout", func(t *testing.T) {
		dbName := "test_db_visibility_" + uuid.NewString()[:8]
		database, _, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue message
		err := database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "test message")
		require.NoError(t, err)

		// First dequeue
		messages1, err := database.dequeueMessages(ctx, subscriberId, 10, 30*time.Second)
		require.NoError(t, err)
		require.Len(t, messages1, 1)

		// Second dequeue should return nothing (message is invisible)
		messages2, err := database.dequeueMessages(ctx, subscriberId, 10, 30*time.Second)
		require.NoError(t, err)
		require.Len(t, messages2, 0, "delivered message should not be dequeued again")
	})
}

func TestDB_AckSubscriberMessage(t *testing.T) {
	t.Run("deletes message on ack", func(t *testing.T) {
		dbName := "test_db_ack_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()
		messageId := uuid.NewString()

		// Enqueue
		err := database.enqueueToSubscribers(ctx, topicId, messageId, "test message")
		require.NoError(t, err)

		// Ack
		err = database.ackSubscriberMessage(ctx, subscriberId, messageId)
		require.NoError(t, err)

		// Verify deleted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("returns error for non-existent message", func(t *testing.T) {
		dbName := "test_db_ack_notfound_" + uuid.NewString()[:8]
		database, _, _, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		err := database.ackSubscriberMessage(ctx, subscriberId, "non-existent-id")
		require.Error(t, err)
		require.Equal(t, ErrMessageNotFound, err)
	})
}

func TestDB_RequeueExpiredMessages(t *testing.T) {
	t.Run("requeues expired delivered messages", func(t *testing.T) {
		dbName := "test_db_requeue_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue a message
		messageId := uuid.NewString()
		err := database.enqueueToSubscribers(ctx, topicId, messageId, "test message")
		require.NoError(t, err)

		// Get the message ID from DB
		var msgDbId int64
		err = sqliteDb.Database.Get(&msgDbId, "SELECT id FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)

		// Manually set status to delivered and visible_at to past (expired)
		_, err = sqliteDb.Database.Exec(
			"UPDATE subscriber_messages SET status = 'delivered', visible_at = datetime('now', '-1 minute') WHERE id = ?",
			msgDbId,
		)
		require.NoError(t, err)

		// Requeue expired messages
		err = database.requeueExpiredMessages(ctx, subscriberId, 3, 30*time.Second)
		require.NoError(t, err)

		// Verify message is pending again with incremented retry count
		var status string
		var retryCount int
		err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE id = ?", msgDbId)
		require.NoError(t, err)
		require.Equal(t, "pending", status)

		err = sqliteDb.Database.Get(&retryCount, "SELECT retry_count FROM subscriber_messages WHERE id = ?", msgDbId)
		require.NoError(t, err)
		require.Equal(t, 1, retryCount)
	})

	t.Run("deletes messages exceeding max retries", func(t *testing.T) {
		dbName := "test_db_maxretry_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue
		messageId := uuid.NewString()
		err := database.enqueueToSubscribers(ctx, topicId, messageId, "test message")
		require.NoError(t, err)

		// Manually set high retry count and make it expired
		_, err = sqliteDb.Database.Exec(
			"UPDATE subscriber_messages SET status = 'delivered', retry_count = 5, visible_at = datetime('now', '-1 minute') WHERE message_id = ?",
			messageId,
		)
		require.NoError(t, err)

		// Requeue with max retries = 3
		err = database.requeueExpiredMessages(ctx, subscriberId, 3, 30*time.Second)
		require.NoError(t, err)

		// Verify message was moved to dead_letter
		var status string
		err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, "dead_letter", status, "message exceeding max retries should be moved to dead_letter")
	})
}

func TestDB_GetSubscriberQueueStats(t *testing.T) {
	t.Run("returns correct stats", func(t *testing.T) {
		dbName := "test_db_stats_" + uuid.NewString()[:8]
		database, _, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue 5 messages
		for i := 0; i < 5; i++ {
			err := database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "test message")
			require.NoError(t, err)
		}

		// Dequeue 2 messages
		_, err := database.dequeueMessages(ctx, subscriberId, 2, 30*time.Second)
		require.NoError(t, err)

		// Check stats
		stats, err := database.getSubscriberQueueStats(ctx, subscriberId)
		require.NoError(t, err)
		require.Equal(t, 3, stats.Pending)
		require.Equal(t, 2, stats.Delivered)
	})
}

func TestDB_DeleteSubscriberMessages(t *testing.T) {
	t.Run("deletes all messages for subscriber", func(t *testing.T) {
		dbName := "test_db_delsub_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue messages
		for i := 0; i < 5; i++ {
			err := database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "test message")
			require.NoError(t, err)
		}

		// Delete all
		err := database.deleteSubscriberMessages(ctx, subscriberId)
		require.NoError(t, err)

		// Verify deleted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE subscriber_id = ?", subscriberId)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})
}

func TestDB_DeleteTopicMessages(t *testing.T) {
	t.Run("deletes all messages for topic", func(t *testing.T) {
		dbName := "test_db_deltopic_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, _, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue messages
		for i := 0; i < 5; i++ {
			err := database.enqueueToSubscribers(ctx, topicId, uuid.NewString(), "test message")
			require.NoError(t, err)
		}

		// Delete all for topic
		err := database.deleteTopicMessages(ctx, topicId)
		require.NoError(t, err)

		// Verify deleted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})
}

func TestDB_BatchEnqueueToSubscribers(t *testing.T) {
	t.Run("batch inserts messages correctly", func(t *testing.T) {
		dbName := "test_db_batch_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, _, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Create batch
		requests := make([]writeRequest, 10)
		for i := 0; i < 10; i++ {
			requests[i] = writeRequest{
				TopicId:   topicId,
				MessageId: uuid.NewString(),
				Message:   "batch message",
			}
		}

		// Batch insert
		err := database.batchEnqueueToSubscribers(ctx, requests)
		require.NoError(t, err)

		// Verify all inserted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topicId)
		require.NoError(t, err)
		require.Equal(t, 10, count)
	})
}

func TestDB_BatchAckSubscriberMessages(t *testing.T) {
	t.Run("deletes multiple messages on batch ack", func(t *testing.T) {
		dbName := "test_db_batch_ack_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()

		// Enqueue 3 messages
		msgIds := make([]string, 3)
		for i := 0; i < 3; i++ {
			msgIds[i] = uuid.NewString()
			err := database.enqueueToSubscribers(ctx, topicId, msgIds[i], "test message")
			require.NoError(t, err)
		}

		// Batch Ack
		err := database.ackSubscriberMessages(ctx, subscriberId, msgIds)
		require.NoError(t, err)

		// Verify all deleted
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE subscriber_id = ?", subscriberId)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})
}

func TestDB_DLQ(t *testing.T) {
	t.Run("moves expired max retry messages to DLQ", func(t *testing.T) {
		dbName := "test_db_dlq_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()
		messageId := uuid.NewString()

		// Enqueue
		err := database.enqueueToSubscribers(ctx, topicId, messageId, "test dlq")
		require.NoError(t, err)

		// Set to max retry and expired
		_, err = sqliteDb.Database.Exec(
			"UPDATE subscriber_messages SET status = 'delivered', retry_count = 5, visible_at = datetime('now', '-1 minute') WHERE message_id = ?",
			messageId,
		)
		require.NoError(t, err)

		// Requeue (limit 3)
		err = database.requeueExpiredMessages(ctx, subscriberId, 3, 30*time.Second)
		require.NoError(t, err)

		// Verify is dead letter
		var status string
		err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, "dead_letter", status)
	})

	t.Run("retrieves DLQ messages", func(t *testing.T) {
		dbName := "test_db_getdlq_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()
		messageId := uuid.NewString()

		// Manually insert dead letter
		_, err := sqliteDb.Database.Exec(
			"INSERT INTO subscriber_messages (subscriber_id, topic_id, message_id, message, status, created_at) VALUES (?, ?, ?, ?, 'dead_letter', CURRENT_TIMESTAMP)",
			subscriberId, topicId, messageId, "dead message",
		)
		require.NoError(t, err)

		// Get DLQ
		messages, err := database.getDeadLetterMessages(ctx, subscriberId, 10, 0)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		require.Equal(t, messageId, messages[0].MessageId)
	})

	t.Run("restores DLQ message to pending", func(t *testing.T) {
		dbName := "test_db_restoredlq_" + uuid.NewString()[:8]
		database, sqliteDb, topicId, subscriberId, cleanup := setupDBTestEnv(t, dbName)
		defer cleanup()

		ctx := context.Background()
		messageId := uuid.NewString()

		// Manually insert dead letter
		_, err := sqliteDb.Database.Exec(
			"INSERT INTO subscriber_messages (subscriber_id, topic_id, message_id, message, status, created_at, retry_count) VALUES (?, ?, ?, ?, 'dead_letter', CURRENT_TIMESTAMP, 5)",
			subscriberId, topicId, messageId, "dead message",
		)
		require.NoError(t, err)

		// Restore
		err = database.restoreDeadLetterMessage(ctx, subscriberId, messageId)
		require.NoError(t, err)

		// Verify status pending and retry 0
		var status string
		var retryCount int
		err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, "pending", status)

		err = sqliteDb.Database.Get(&retryCount, "SELECT retry_count FROM subscriber_messages WHERE message_id = ?", messageId)
		require.NoError(t, err)
		require.Equal(t, 0, retryCount)
	})
}
