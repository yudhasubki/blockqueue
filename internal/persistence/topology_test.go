package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestTopologyTopicLifecycle(t *testing.T) {
	ctx := context.Background()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "control.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, driver))

	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })
	topicID := uuid.New()
	subscriberID := uuid.New()
	topic := Topic{ID: topicID, Name: "control-topic", Paused: true}
	subscribers := Subscribers{{
		ID:      subscriberID,
		TopicID: topicID,
		Name:    "control-subscriber",
		Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "30s"},
		Paused:  true,
	}}
	require.NoError(t, database.createTopic(ctx, topic, subscribers))
	var storedPause struct {
		Topic      bool `db:"topic_paused"`
		Subscriber bool `db:"subscriber_paused"`
	}
	require.NoError(t, database.Conn().Get(&storedPause, `
		SELECT topics.paused AS topic_paused, subscribers.paused AS subscriber_paused
		FROM topics JOIN topic_subscribers subscribers ON subscribers.topic_id = topics.id
		WHERE topics.id = ?`, topicID))
	require.True(t, storedPause.Topic)
	require.True(t, storedPause.Subscriber)

	messageID := uuid.NewString()
	scheduleID := uuid.NewString()
	runID := uuid.NewString()
	now := time.Now().UTC()
	_, err = database.Conn().ExecContext(ctx, `
		INSERT INTO messages (id, topic_id, message, scheduled_at) VALUES (?, ?, 'payload', ?);
		INSERT INTO message_deliveries (message_id, subscriber_id, visible_at) VALUES (?, ?, ?);
		INSERT INTO schedules (id, topic_id, name, cron_expression, message, next_run_at) VALUES (?, ?, 'daily', '0 0 * * *', 'payload', ?);
		INSERT INTO schedule_runs (id, schedule_id, message_id, scheduled_for) VALUES (?, ?, ?, ?)
	`, messageID, topicID, now,
		messageID, subscriberID, now,
		scheduleID, topicID, now,
		runID, scheduleID, messageID, now)
	require.NoError(t, err)

	require.NoError(t, database.deleteTopic(ctx, topicID))
	_, _, due, err := database.nextScheduleDue(ctx, now.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, due, "logically deleted topics must immediately leave scheduler reconciliation")
	_, claimed, err := database.claimDueSchedule(ctx, "cleanup-test", now.Add(time.Hour), time.Minute)
	require.NoError(t, err)
	require.False(t, claimed)

	var activeTopics int
	require.NoError(t, database.Conn().GetContext(ctx, &activeTopics,
		"SELECT COUNT(*) FROM topics WHERE id = ? AND deleted_at IS NULL", topicID))
	require.Zero(t, activeTopics)

	var activeSubscribers int
	require.NoError(t, database.Conn().GetContext(ctx, &activeSubscribers,
		"SELECT COUNT(*) FROM topic_subscribers WHERE topic_id = ? AND deleted_at IS NULL", topicID))
	require.Zero(t, activeSubscribers)

	for _, table := range []string{
		"message_deliveries",
		"messages",
		"schedule_runs",
		"schedules",
	} {
		var count int
		require.NoError(t, database.Conn().GetContext(ctx, &count, "SELECT COUNT(*) FROM "+table))
		require.Equal(t, 1, count, table+" must remain until bounded cleanup")
	}

	cleaned, _, _, err := database.pruneDeletedTopology(ctx, time.Second)
	require.NoError(t, err)
	require.GreaterOrEqual(t, cleaned, int64(6))
	for _, table := range []string{
		"topics",
		"topic_subscribers",
		"message_deliveries",
		"messages",
		"schedule_runs",
		"schedules",
	} {
		var count int
		require.NoError(t, database.Conn().GetContext(ctx, &count, "SELECT COUNT(*) FROM "+table))
		require.Zero(t, count, table)
	}
}

func TestHasDeletedTopologyDistinguishesCleanDatabase(t *testing.T) {
	ctx := context.Background()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "deleted-topology-check.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, driver))

	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })

	hasDeleted, err := database.hasDeletedTopology(ctx)
	require.NoError(t, err)
	require.False(t, hasDeleted, "a clean startup must not schedule a competing cleanup pass")

	topicID := uuid.New()
	require.NoError(t, database.createTopic(ctx, Topic{ID: topicID, Name: "deleted-check"}, nil))
	require.NoError(t, database.deleteTopic(ctx, topicID))
	hasDeleted, err = database.hasDeletedTopology(ctx)
	require.NoError(t, err)
	require.True(t, hasDeleted, "durable tombstones must resume cleanup after restart")
}

func TestTopologyDeleteSubscriber(t *testing.T) {
	ctx := context.Background()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "subscriber-control.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, driver))

	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })
	topicID := uuid.New()
	subscriberID := uuid.New()
	subscriberName := "removed-subscriber"
	require.NoError(t, database.createTopic(ctx,
		Topic{ID: topicID, Name: "subscriber-control-topic"},
		Subscribers{{
			ID: subscriberID, TopicID: topicID, Name: subscriberName,
			Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "30s"},
		}},
	))

	messageID := uuid.NewString()
	now := time.Now().UTC()
	_, err = database.Conn().ExecContext(ctx, `
		INSERT INTO messages (id, topic_id, message, scheduled_at) VALUES (?, ?, 'payload', ?);
		INSERT INTO message_deliveries (message_id, subscriber_id, visible_at) VALUES (?, ?, ?)
	`, messageID, topicID, now, messageID, subscriberID, now)
	require.NoError(t, err)

	require.NoError(t, database.deleteSubscriber(ctx, topicID, subscriberID, subscriberName))
	require.ErrorIs(t, database.deleteSubscriber(ctx, topicID, subscriberID, subscriberName), ErrSubscriberNotFound)

	var activeSubscribers int
	require.NoError(t, database.Conn().GetContext(ctx, &activeSubscribers,
		"SELECT COUNT(*) FROM topic_subscribers WHERE id = ? AND deleted_at IS NULL", subscriberID))
	require.Zero(t, activeSubscribers)

	var deliveries int
	require.NoError(t, database.Conn().GetContext(ctx, &deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.Equal(t, 1, deliveries, "logical deletion must return before physical cleanup")

	cleaned, _, _, err := database.pruneDeletedTopology(ctx, time.Second)
	require.NoError(t, err)
	require.GreaterOrEqual(t, cleaned, int64(2))
	require.NoError(t, database.Conn().GetContext(ctx, &deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.Zero(t, deliveries)

	var canonicalMessages int
	require.NoError(t, database.Conn().GetContext(ctx, &canonicalMessages, "SELECT COUNT(*) FROM messages WHERE id = ?", messageID))
	require.Equal(t, 1, canonicalMessages, "subscriber deletion must not remove the canonical message")
}

func TestTopologyCleanupYieldsBetweenChunks(t *testing.T) {
	ctx := context.Background()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "topology-cleanup-chunks.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, driver))
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })

	topicID := uuid.New()
	subscriberID := uuid.New()
	require.NoError(t, database.createTopic(ctx,
		Topic{ID: topicID, Name: "chunked-cleanup"},
		Subscribers{{
			ID: subscriberID, TopicID: topicID, Name: "worker",
			Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "30s"},
		}},
	))
	now := time.Now().UTC()
	rows := topologyCleanupBatchSize + 1
	_, err = database.Conn().ExecContext(ctx, `
		WITH RECURSIVE sequence(value) AS (
			SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < ?
		)
		INSERT INTO messages (id, topic_id, message, scheduled_at)
		SELECT printf('cleanup-%04d', value), ?, 'payload', ? FROM sequence`, rows, topicID, now)
	require.NoError(t, err)
	_, err = database.Conn().ExecContext(ctx, `
		WITH RECURSIVE sequence(value) AS (
			SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < ?
		)
		INSERT INTO message_deliveries (message_id, subscriber_id, visible_at)
		SELECT printf('cleanup-%04d', value), ?, ? FROM sequence`, rows, subscriberID, now)
	require.NoError(t, err)
	require.NoError(t, database.deleteSubscriber(ctx, topicID, subscriberID, "worker"))

	cleaned, yielded, _, err := database.pruneDeletedTopology(ctx, time.Second)
	require.NoError(t, err)
	require.True(t, yielded, "a full chunk must yield before cleanup continues")
	require.EqualValues(t, rows+1, cleaned)
	var deliveries, subscribers int
	require.NoError(t, database.Conn().Get(&deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.NoError(t, database.Conn().Get(&subscribers, "SELECT COUNT(*) FROM topic_subscribers"))
	require.Zero(t, deliveries)
	require.Zero(t, subscribers)
}

func TestSubscriberCleanupCompletesScheduleRunWithoutActiveDeliveries(t *testing.T) {
	ctx := context.Background()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "subscriber-run-completion.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(ctx, driver))
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })

	topicID := uuid.New()
	subscriberID := uuid.New()
	require.NoError(t, database.createTopic(ctx,
		Topic{ID: topicID, Name: "subscriber-run-completion"},
		Subscribers{{
			ID: subscriberID, TopicID: topicID, Name: "worker",
			Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "30s"},
		}},
	))
	now := time.Now().UTC()
	messageID := uuid.NewString()
	scheduleID := uuid.NewString()
	runID := uuid.NewString()
	_, err = database.Conn().ExecContext(ctx, `
		INSERT INTO messages (id, topic_id, message, scheduled_at) VALUES (?, ?, 'payload', ?);
		INSERT INTO message_deliveries (message_id, subscriber_id, visible_at) VALUES (?, ?, ?);
		INSERT INTO schedules (id, topic_id, name, cron_expression, message, next_run_at)
			VALUES (?, ?, 'daily', '0 0 * * *', 'payload', ?);
		INSERT INTO schedule_runs (id, schedule_id, message_id, scheduled_for, status)
			VALUES (?, ?, ?, ?, 'running')
	`, messageID, topicID, now,
		messageID, subscriberID, now,
		scheduleID, topicID, now.Add(time.Hour),
		runID, scheduleID, messageID, now)
	require.NoError(t, err)
	require.NoError(t, database.deleteSubscriber(ctx, topicID, subscriberID, "worker"))
	_, _, _, err = database.pruneDeletedTopology(ctx, time.Second)
	require.NoError(t, err)

	var status string
	require.NoError(t, database.Conn().GetContext(ctx, &status,
		"SELECT status FROM schedule_runs WHERE id = ?", runID))
	require.Equal(t, ScheduleRunStatusCompleted, status)
}

func TestPruneDeletedTopologyReportsRemainingWorkWhenBudgetExpires(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "topology-cleanup-budget.db"), sqlite.Config{})
	require.NoError(t, err)
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })
	require.NoError(t, Migrate(context.Background(), driver))

	rows, yielded, more, err := database.pruneDeletedTopology(context.Background(), time.Nanosecond)
	require.NoError(t, err)
	require.Zero(t, rows)
	require.False(t, yielded)
	require.True(t, more)
}
