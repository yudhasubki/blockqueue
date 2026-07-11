package blockqueue

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestMigrationChecksumMismatchFailsLoudly(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "checksum.db"), sqlite.Config{})
	require.NoError(t, err)
	defer func() { require.NoError(t, driver.Close()) }()
	require.NoError(t, Migrate(context.Background(), driver))

	_, err = testDB(driver).Exec(`
		UPDATE schema_migrations SET checksum = 'tampered'
		WHERE version = (SELECT MIN(version) FROM schema_migrations)`)
	require.NoError(t, err)
	err = Migrate(context.Background(), driver)
	require.ErrorIs(t, err, ErrMigrationChecksum)
}

func TestFreshSchemaContainsCanonicalTables(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "fresh-schema.db"), sqlite.Config{})
	require.NoError(t, err)
	defer func() { require.NoError(t, driver.Close()) }()
	require.NoError(t, Migrate(context.Background(), driver))

	connection := testDB(driver)
	for _, table := range []string{"topics", "topic_subscribers", "messages", "message_deliveries", "schedules", "schedule_runs"} {
		var count int
		require.NoError(t, connection.Get(&count,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table))
		require.Equal(t, 1, count, table)
	}
}

func TestStartupFailureClosesOwnedDriver(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "startup-failure.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(context.Background(), driver))
	_, err = testDB(driver).Exec(`
		UPDATE schema_migrations SET checksum = 'tampered'
		WHERE version = (SELECT MIN(version) FROM schema_migrations)`)
	require.NoError(t, err)

	queue := New(driver, Options{})
	err = queue.Run(context.Background())
	require.ErrorIs(t, err, ErrMigrationChecksum)
	require.Equal(t, LifecycleStopped, queue.State())
	require.Error(t, driver.DB().Ping(), "failed startup must release the owned database")
	require.NoError(t, queue.Shutdown(context.Background()), "shutdown after failed startup must be idempotent")
}

func TestConcurrentSQLiteMigrationsAreSerialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-migrate.db")
	drivers := make([]*sqlite.Driver, 2)
	for index := range drivers {
		driver, err := sqlite.Open(path, sqlite.Config{BusyTimeout: 5000})
		require.NoError(t, err)
		drivers[index] = driver
		current := driver
		t.Cleanup(func() { _ = current.Close() })
	}

	start := make(chan struct{})
	errorsByWorker := make([]error, len(drivers))
	var workers sync.WaitGroup
	for index, driver := range drivers {
		workers.Add(1)
		go func(index int, driver *sqlite.Driver) {
			defer workers.Done()
			<-start
			errorsByWorker[index] = Migrate(context.Background(), driver)
		}(index, driver)
	}
	close(start)
	workers.Wait()
	for _, err := range errorsByWorker {
		require.NoError(t, err)
	}

	var applied int
	require.NoError(t, testDB(drivers[0]).Get(&applied, "SELECT COUNT(*) FROM schema_migrations"))
	require.Equal(t, 2, applied)
}

func TestConcurrentPostgresMigrationsAreSerialized(t *testing.T) {
	postgresURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
	if postgresURL == "" {
		t.Skip("set BLOCKQUEUE_TEST_POSTGRES_URL to run the PostgreSQL migration lock test")
	}
	schema, err := testdb.OpenPostgreSQLSchema(
		context.Background(), postgresURL, "_test", "blockqueue_migration",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, schema.Close()) })

	start := make(chan struct{})
	errorsByWorker := make([]error, 2)
	var workers sync.WaitGroup
	for index := range errorsByWorker {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			errorsByWorker[index] = Migrate(context.Background(), schema.Driver)
		}(index)
	}
	close(start)
	workers.Wait()
	for _, err := range errorsByWorker {
		require.NoError(t, err)
	}

	var applied int
	require.NoError(t, testDB(schema.Driver).Get(&applied, "SELECT COUNT(*) FROM schema_migrations"))
	require.Equal(t, 2, applied)
}
