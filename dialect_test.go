package blockqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
)

func TestSQLDialectStrategies(t *testing.T) {
	sqliteBackend, err := newSQLDialect(store.DialectSQLite)
	require.NoError(t, err)
	require.Equal(t, "0", sqliteBackend.boolLiteral(false))
	require.Equal(t, "1", sqliteBackend.boolLiteral(true))
	require.Empty(t, sqliteBackend.lockClause("deliveries", true))
	require.Equal(t, "?", sqliteBackend.timestampBind())
	require.Equal(t, 400, sqliteBackend.claimChunkSize(1000))

	postgresBackend, err := newSQLDialect(store.DialectPostgres)
	require.NoError(t, err)
	require.Equal(t, "FALSE", postgresBackend.boolLiteral(false))
	require.Equal(t, "TRUE", postgresBackend.boolLiteral(true))
	require.Equal(t, " FOR UPDATE OF deliveries SKIP LOCKED", postgresBackend.lockClause("deliveries", true))
	require.Equal(t, "CAST(? AS TIMESTAMPTZ)", postgresBackend.timestampBind())
	require.Equal(t, 1000, postgresBackend.claimChunkSize(1000))

	_, err = newSQLDialect(store.Dialect("unsupported"))
	require.ErrorIs(t, err, ErrUnsupportedDialect)
}
