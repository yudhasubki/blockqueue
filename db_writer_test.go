package blockqueue

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
)

type postgresQueryDriver struct {
	database *sql.DB
}

func (driver *postgresQueryDriver) DB() *sql.DB            { return driver.database }
func (driver *postgresQueryDriver) Dialect() store.Dialect { return store.DialectPostgres }
func (driver *postgresQueryDriver) DriverName() string     { return "pgx" }
func (driver *postgresQueryDriver) Close() error           { return driver.database.Close() }

func TestPostgresDeliveryBatchCTEUsesCanonicalTypes(t *testing.T) {
	raw, err := sql.Open("pgx", "postgres://query-generation-only")
	require.NoError(t, err)
	database := newDb(&postgresQueryDriver{database: raw})
	t.Cleanup(func() { require.NoError(t, database.close()) })

	query := cachedDeliveryInsertQuery(database, 2)
	require.Contains(t, query, "CAST($1 AS UUID)")
	require.Contains(t, query, "CAST($2 AS TIMESTAMPTZ)")
	require.Contains(t, query, "CAST($3 AS INTEGER)")
	require.Contains(t, query, "CAST($4 AS TIMESTAMPTZ)")
	require.Contains(t, query, "CAST($5 AS UUID)")
	require.Contains(t, query, "CAST($8 AS TIMESTAMPTZ)")
}
