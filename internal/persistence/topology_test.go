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
		require.Zero(t, count, table)
	}
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

	for _, table := range []string{"message_deliveries"} {
		var count int
		require.NoError(t, database.Conn().GetContext(ctx, &count, "SELECT COUNT(*) FROM "+table))
		require.Zero(t, count, table)
	}

	var canonicalMessages int
	require.NoError(t, database.Conn().GetContext(ctx, &canonicalMessages, "SELECT COUNT(*) FROM messages WHERE id = ?", messageID))
	require.Equal(t, 1, canonicalMessages, "subscriber deletion must not remove the canonical message")
}
