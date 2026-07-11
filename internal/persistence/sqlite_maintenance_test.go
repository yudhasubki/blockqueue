package persistence

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestSQLiteCheckpointRejectsUnknownMode(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "checkpoint-mode.db"), sqlite.Config{})
	require.NoError(t, err)
	database := newDb(driver)
	t.Cleanup(func() { require.NoError(t, database.close()) })

	_, err = database.checkpointSQLite(context.Background(), sqliteCheckpointMode("INVALID"))
	require.ErrorContains(t, err, "unsupported SQLite checkpoint mode")
}
