package persistence

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestPreparedStatementCacheIsBoundedAndClosedWithDatabase(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "statements.db"), sqlite.Config{})
	require.NoError(t, err)
	database := newDb(driver)
	for index := 0; index < defaultStatementCacheSize+20; index++ {
		_, err := database.statements.get(context.Background(), database.Conn(),
			fmt.Sprintf("SELECT 1 /* statement-%d */", index))
		require.NoError(t, err)
	}
	require.Equal(t, defaultStatementCacheSize, database.statements.len())
	require.NoError(t, database.close())
	require.Zero(t, database.statements.len())
	require.NoError(t, database.close(), "database close must be idempotent")
}
