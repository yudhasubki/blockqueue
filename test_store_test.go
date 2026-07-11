package blockqueue

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
)

func testDB(driver store.Driver) *sqlx.DB {
	return sqlx.NewDb(driver.DB(), driver.DriverName())
}

// requireSQLiteWriteLock tolerates a maintenance transaction that was already
// in flight when a fault-injection test tries to install its artificial lock.
// Once acquired, the caller owns a raw transaction and must issue ROLLBACK.
func requireSQLiteWriteLock(t *testing.T, connection *sqlx.Conn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := connection.ExecContext(context.Background(), "BEGIN IMMEDIATE")
		if err == nil {
			return
		}
		if !isTransientWriteError(err) || time.Now().After(deadline) {
			require.NoError(t, err)
			return
		}
		time.Sleep(time.Millisecond)
	}
}
