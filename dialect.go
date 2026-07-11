package blockqueue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

var ErrUnsupportedDialect = errors.New("unsupported database dialect")

// sqlDialect owns the small set of SQL and concurrency differences that are
// genuinely backend-specific. Durable queue transactions remain in db so
// SQLite and PostgreSQL cannot drift into separate repository implementations.
type sqlDialect interface {
	kind() store.Dialect
	migrationDirectory() string
	boolLiteral(bool) string
	currentTimeQuery() string
	lockClause(target string, skipLocked bool) string
	topologyReadLockClause(target string) string
	timestampBind() string
	deliveryBatchRow() string
	deliveryIdentityRow() string
	deliveryFailureRow() string
	messageChunkSize() int
	claimChunkSize(int) int
	usesDatabaseEvents() bool
	lockMigrations(context.Context, *sqlx.Tx) error
}

type sqliteDialect struct{}

func (sqliteDialect) kind() store.Dialect        { return store.DialectSQLite }
func (sqliteDialect) migrationDirectory() string { return "sqlite" }
func (sqliteDialect) boolLiteral(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
func (sqliteDialect) currentTimeQuery() string                       { return "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')" }
func (sqliteDialect) lockClause(string, bool) string                 { return "" }
func (sqliteDialect) topologyReadLockClause(string) string           { return "" }
func (sqliteDialect) timestampBind() string                          { return "?" }
func (sqliteDialect) deliveryBatchRow() string                       { return "(?, ?, ?, ?)" }
func (sqliteDialect) deliveryIdentityRow() string                    { return "(?, ?)" }
func (sqliteDialect) deliveryFailureRow() string                     { return "(?, ?, ?, ?, ?, ?)" }
func (sqliteDialect) messageChunkSize() int                          { return 100 }
func (sqliteDialect) claimChunkSize(requested int) int               { return min(requested, 400) }
func (sqliteDialect) usesDatabaseEvents() bool                       { return false }
func (sqliteDialect) lockMigrations(context.Context, *sqlx.Tx) error { return nil }

type postgresDialect struct{}

func (postgresDialect) kind() store.Dialect        { return store.DialectPostgres }
func (postgresDialect) migrationDirectory() string { return "pgsql" }
func (postgresDialect) boolLiteral(value bool) string {
	if value {
		return "TRUE"
	}
	return "FALSE"
}
func (postgresDialect) currentTimeQuery() string { return "SELECT clock_timestamp()" }
func (postgresDialect) timestampBind() string    { return "CAST(? AS TIMESTAMPTZ)" }
func (postgresDialect) deliveryBatchRow() string {
	return "(CAST(? AS UUID), CAST(? AS TIMESTAMPTZ), CAST(? AS INTEGER), CAST(? AS TIMESTAMPTZ))"
}
func (postgresDialect) deliveryIdentityRow() string {
	return "(CAST(? AS UUID), CAST(? AS UUID))"
}
func (postgresDialect) deliveryFailureRow() string {
	return "(CAST(? AS UUID), CAST(? AS UUID), CAST(? AS INTEGER), CAST(? AS VARCHAR), CAST(? AS TIMESTAMPTZ), CAST(? AS TIMESTAMPTZ))"
}
func (postgresDialect) messageChunkSize() int            { return 1000 }
func (postgresDialect) claimChunkSize(requested int) int { return requested }
func (postgresDialect) usesDatabaseEvents() bool         { return true }

func (postgresDialect) topologyReadLockClause(target string) string {
	clause := " FOR SHARE"
	if target != "" {
		clause += " OF " + target
	}
	return clause
}

func (postgresDialect) lockClause(target string, skipLocked bool) string {
	clause := " FOR UPDATE"
	if target != "" {
		clause += " OF " + target
	}
	if skipLocked {
		clause += " SKIP LOCKED"
	}
	return clause
}

const migrationAdvisoryLockID int64 = 0x424C4F434B515545 // "BLOCKQUE"

func (postgresDialect) lockMigrations(ctx context.Context, tx *sqlx.Tx) error {
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	return nil
}

func newSQLDialect(kind store.Dialect) (sqlDialect, error) {
	switch kind {
	case store.DialectSQLite:
		return sqliteDialect{}, nil
	case store.DialectPostgres:
		return postgresDialect{}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDialect, kind)
	}
}
