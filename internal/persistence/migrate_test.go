package persistence

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestMigrationStatementAndLedgerRollbackTogether(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "rollback.db"), sqlite.Config{})
	require.NoError(t, err)
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })
	connection := database.Conn()
	require.NoError(t, ensureMigrationTable(context.Background(), connection, "sqlite"))

	migration := embeddedMigration{
		version: "9999999999_transaction_rollback",
		path:    "test migration",
		contents: `
			CREATE TABLE migration_must_rollback (id INTEGER PRIMARY KEY);
			INSERT INTO missing_table (id) VALUES (1);`,
		checksum: "test-checksum",
	}
	dialect, err := newSQLDialect(store.DialectSQLite)
	require.NoError(t, err)
	err = applyMigrations(context.Background(), connection, dialect, []embeddedMigration{migration})
	require.Error(t, err)

	var exists int
	require.NoError(t, connection.Get(&exists,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", "migration_must_rollback"))
	require.Zero(t, exists)
	var applied int
	require.NoError(t, connection.Get(&applied,
		"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", migration.version))
	require.Zero(t, applied)
}

func TestEmbeddedSchemasContainCanonicalStateValues(t *testing.T) {
	values := []string{
		DeliveryStatusPending,
		DeliveryStatusDelivered,
		DeliveryStatusProcessed,
		DeliveryStatusDeadLetter,
		DeliveryStatusCancelled,
		ScheduleRunStatusRunning,
		ScheduleRunStatusCompleted,
		ScheduleRunStatusSkipped,
		ScheduleRunStatusFailed,
		ScheduleMisfirePolicyFireOnce,
		ScheduleOverlapPolicySkip,
	}
	for _, path := range []string{
		"migration/sqlite/0001_v0_2_0_schema.sql",
		"migration/pgsql/0001_v0_2_0_schema.sql",
	} {
		contents, err := migrationFiles.ReadFile(path)
		require.NoError(t, err)
		for _, value := range values {
			require.Contains(t, string(contents), "'"+value+"'", "%s must contain %q", path, value)
		}
	}
}
