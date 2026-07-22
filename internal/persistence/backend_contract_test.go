package persistence

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func forEachPersistenceContractBackend(
	t *testing.T,
	run func(*testing.T, store.Driver),
) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		driver, err := sqlite.Open(filepath.Join(t.TempDir(), "persistence-contract.db"), sqlite.Config{
			BusyTimeout: 5000,
		})
		require.NoError(t, err)
		run(t, driver)
	})

	t.Run("postgres", func(t *testing.T) {
		postgresURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
		if postgresURL == "" {
			t.Skip("set BLOCKQUEUE_TEST_POSTGRES_URL to run the PostgreSQL persistence contract")
		}
		schema, err := testdb.OpenPostgreSQLSchema(
			context.Background(), postgresURL, "_test", "bq_persistence_contract",
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, schema.Close()) })
		run(t, schema.Driver)
	})
}

func persistenceContractDatabase(t *testing.T, driver store.Driver) *db {
	t.Helper()
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })
	require.NoError(t, Migrate(context.Background(), driver))
	return database
}

func createPersistenceContractTopic(t *testing.T, database *db, name string) (Topic, Subscriber) {
	t.Helper()
	topic := Topic{ID: uuid.New(), Name: name}
	subscriber := Subscriber{
		ID:      uuid.New(),
		TopicID: topic.ID,
		Name:    "worker",
		Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1},
	}
	require.NoError(t, database.createTopic(
		context.Background(), topic, Subscribers{subscriber},
	))
	return topic, subscriber
}

func TestScheduleRunRetentionContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		topic, _ := createPersistenceContractTopic(t, database, "retention-contract")
		ctx := context.Background()
		scheduleID := uuid.NewString()
		completedRunID := uuid.NewString()
		runningRunID := uuid.NewString()
		cutoff := time.Now().UTC().Add(-31 * 24 * time.Hour)

		_, err := database.Conn().ExecContext(ctx, database.Conn().Rebind(`
			INSERT INTO schedules (id, topic_id, name, cron_expression, message, next_run_at)
			VALUES (?, ?, 'retention', '0 0 * * *', 'payload', ?)`),
			scheduleID, topic.ID, cutoff)
		require.NoError(t, err)
		_, err = database.Conn().ExecContext(ctx, database.Conn().Rebind(`
			INSERT INTO schedule_runs (id, schedule_id, scheduled_for, status, created_at, finished_at)
			VALUES (?, ?, ?, 'completed', ?, ?), (?, ?, ?, 'running', ?, NULL)`),
			completedRunID, scheduleID, cutoff, cutoff, cutoff,
			runningRunID, scheduleID, cutoff.Add(-time.Hour), cutoff)
		require.NoError(t, err)

		require.NoError(t, database.pruneScheduleRuns(ctx, 30*24*time.Hour))
		var completedRows, runningRows int
		require.NoError(t, database.Conn().Get(&completedRows, database.Conn().Rebind(
			"SELECT COUNT(*) FROM schedule_runs WHERE id = ?"), completedRunID))
		require.NoError(t, database.Conn().Get(&runningRows, database.Conn().Rebind(
			"SELECT COUNT(*) FROM schedule_runs WHERE id = ?"), runningRunID))
		require.Zero(t, completedRows)
		require.Equal(t, 1, runningRows, "retention must never delete an active run")
	})
}

func TestMaintenanceLeadershipContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		var leader bool
		var release func() error
		var leadershipErr error
		require.Eventually(t, func() bool {
			leader, release, leadershipErr = database.tryMaintenanceLeadership(context.Background())
			return leader
		}, 3*time.Second, 10*time.Millisecond)
		require.NoError(t, leadershipErr)
		defer func() { require.NoError(t, release()) }()

		secondLeader, secondRelease, err := database.tryMaintenanceLeadership(context.Background())
		require.NoError(t, err)
		if driver.Dialect() == store.DialectPostgres {
			require.False(t, secondLeader)
			require.Nil(t, secondRelease)
		} else {
			require.True(t, secondLeader)
			require.NoError(t, secondRelease())
		}
	})
}

func TestAmbiguousCommitRetryContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		topic, _ := createPersistenceContractTopic(t, database, "ambiguous-commit-contract")
		now := time.Now().UTC().Truncate(time.Millisecond)
		request := WriteRequest{
			TopicID:   topic.ID,
			MessageID: uuid.NewString(),
			Message:   "commit-once",
			Headers:   []byte("{}"),
			VisibleAt: now,
			CreatedAt: now,
		}

		result, err := database.persistWriteRequests(context.Background(), []WriteRequest{request})
		require.NoError(t, err)
		require.Equal(t, []bool{false}, result.Duplicates)
		require.Equal(t, []time.Time{now}, result.ScheduledAt)
		result, err = database.persistWriteRequests(context.Background(), []WriteRequest{request})
		require.NoError(t, err)
		require.Equal(t, []bool{true}, result.Duplicates)
		require.Equal(t, []time.Time{now}, result.ScheduledAt)

		var canonicalRows, deliveryRows int
		require.NoError(t, database.Conn().Get(&canonicalRows, database.Conn().Rebind(
			"SELECT COUNT(*) FROM messages WHERE id = ?"), request.MessageID))
		require.NoError(t, database.Conn().Get(&deliveryRows, database.Conn().Rebind(
			"SELECT COUNT(*) FROM message_deliveries WHERE message_id = ?"), request.MessageID))
		require.Equal(t, 1, canonicalRows)
		require.Equal(t, 1, deliveryRows)

		conflict := request
		conflict.Message = "different-payload"
		_, err = database.persistWriteRequests(context.Background(), []WriteRequest{conflict})
		require.ErrorIs(t, err, ErrIdempotencyConflict)
	})
}

func TestPublishSchedulingUsesDatabaseClockContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		topic, _ := createPersistenceContractTopic(t, database, "database-clock-contract")
		ctx := context.Background()
		before, err := database.databaseNow(ctx)
		require.NoError(t, err)
		requests := []WriteRequest{
			{
				TopicID:      topic.ID,
				MessageID:    uuid.NewString(),
				Message:      "immediate",
				Headers:      []byte("{}"),
				ScheduleMode: scheduleModeImmediate,
				CreatedAt:    time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				VisibleAt:    time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				TopicID:       topic.ID,
				MessageID:     uuid.NewString(),
				Message:       "delayed",
				Headers:       []byte("{}"),
				ScheduleMode:  scheduleModeDelay,
				ScheduleDelay: 2 * time.Second,
				CreatedAt:     time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				VisibleAt:     time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}
		result, err := database.persistWriteRequests(ctx, requests)
		require.NoError(t, err)
		require.Len(t, result.ScheduledAt, len(requests))
		after, err := database.databaseNow(ctx)
		require.NoError(t, err)

		type timestampRow struct {
			ID          string    `db:"id"`
			CreatedAt   time.Time `db:"created_at"`
			ScheduledAt time.Time `db:"scheduled_at"`
		}
		rows := make([]timestampRow, 0, 2)
		query, args, err := sqlx.In(
			"SELECT id, created_at, scheduled_at FROM messages WHERE id IN (?)", []string{requests[0].MessageID, requests[1].MessageID},
		)
		require.NoError(t, err)
		require.NoError(t, database.Conn().SelectContext(ctx, &rows, database.Conn().Rebind(query), args...))
		require.Len(t, rows, 2)
		byID := make(map[string]timestampRow, len(rows))
		for _, row := range rows {
			byID[row.ID] = row
			require.False(t, row.CreatedAt.Before(before.Add(-time.Millisecond)))
			require.False(t, row.CreatedAt.After(after.Add(time.Millisecond)))
		}
		require.WithinDuration(t, byID[requests[0].MessageID].ScheduledAt, result.ScheduledAt[0], time.Millisecond)
		require.WithinDuration(t, byID[requests[1].MessageID].ScheduledAt, result.ScheduledAt[1], time.Millisecond)
		require.WithinDuration(t, byID[requests[0].MessageID].CreatedAt, byID[requests[0].MessageID].ScheduledAt, time.Millisecond)
		require.WithinDuration(t, byID[requests[1].MessageID].CreatedAt.Add(2*time.Second), byID[requests[1].MessageID].ScheduledAt, time.Millisecond)
	})
}

func TestLogicalTopicDeletionContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		topic, subscriber := createPersistenceContractTopic(t, database, "logical-delete-contract")
		ctx := context.Background()
		now := time.Now().UTC()
		messageID := uuid.NewString()
		scheduleID := uuid.NewString()
		runID := uuid.NewString()

		_, err := database.Conn().ExecContext(ctx, database.Conn().Rebind(
			"INSERT INTO messages (id, topic_id, message, scheduled_at) VALUES (?, ?, 'payload', ?)"),
			messageID, topic.ID, now)
		require.NoError(t, err)
		_, err = database.Conn().ExecContext(ctx, database.Conn().Rebind(
			"INSERT INTO message_deliveries (message_id, subscriber_id, visible_at) VALUES (?, ?, ?)"),
			messageID, subscriber.ID, now)
		require.NoError(t, err)
		_, err = database.Conn().ExecContext(ctx, database.Conn().Rebind(`
			INSERT INTO schedules (id, topic_id, name, cron_expression, message, next_run_at)
			VALUES (?, ?, 'due', '* * * * *', 'payload', ?)`), scheduleID, topic.ID, now)
		require.NoError(t, err)
		_, err = database.Conn().ExecContext(ctx, database.Conn().Rebind(`
			INSERT INTO schedule_runs (id, schedule_id, message_id, scheduled_for, status)
			VALUES (?, ?, ?, ?, 'running')`), runID, scheduleID, messageID, now)
		require.NoError(t, err)

		require.NoError(t, database.deleteTopic(ctx, topic.ID))
		var activeTopics, activeSubscribers int
		require.NoError(t, database.Conn().Get(&activeTopics, database.Conn().Rebind(
			"SELECT COUNT(*) FROM topics WHERE id = ? AND deleted_at IS NULL"), topic.ID))
		require.NoError(t, database.Conn().Get(&activeSubscribers, database.Conn().Rebind(
			"SELECT COUNT(*) FROM topic_subscribers WHERE id = ? AND deleted_at IS NULL"), subscriber.ID))
		require.Zero(t, activeTopics)
		require.Zero(t, activeSubscribers)
		_, _, due, err := database.nextScheduleDue(ctx, now.Add(time.Hour))
		require.NoError(t, err)
		require.False(t, due)

		for _, table := range []string{"messages", "message_deliveries", "schedules", "schedule_runs"} {
			var rows int
			require.NoError(t, database.Conn().Get(&rows, "SELECT COUNT(*) FROM "+table))
			require.Equal(t, 1, rows, table+" must remain until physical cleanup")
		}
		cleaned, _, _, err := database.pruneDeletedTopology(ctx, time.Second)
		require.NoError(t, err)
		require.GreaterOrEqual(t, cleaned, int64(6))
		for _, table := range []string{
			"topics", "topic_subscribers", "messages", "message_deliveries", "schedules", "schedule_runs",
		} {
			var rows int
			require.NoError(t, database.Conn().Get(&rows, "SELECT COUNT(*) FROM "+table))
			require.Zero(t, rows, table)
		}
	})
}
