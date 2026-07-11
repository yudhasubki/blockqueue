package persistence

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
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
		ID: uuid.New(), TopicID: topic.ID, Name: "worker",
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

func TestAmbiguousCommitRetryContract(t *testing.T) {
	forEachPersistenceContractBackend(t, func(t *testing.T, driver store.Driver) {
		database := persistenceContractDatabase(t, driver)
		topic, _ := createPersistenceContractTopic(t, database, "ambiguous-commit-contract")
		now := time.Now().UTC().Truncate(time.Millisecond)
		request := WriteRequest{
			TopicID: topic.ID, MessageID: uuid.NewString(), Message: "commit-once",
			Headers: []byte("{}"), VisibleAt: now, CreatedAt: now,
		}

		duplicates, err := database.persistWriteRequests(context.Background(), []WriteRequest{request})
		require.NoError(t, err)
		require.Equal(t, []bool{false}, duplicates)
		duplicates, err = database.persistWriteRequests(context.Background(), []WriteRequest{request})
		require.NoError(t, err)
		require.Equal(t, []bool{true}, duplicates)

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
