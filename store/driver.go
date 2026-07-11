// Package store defines the small database boundary used by BlockQueue.
package store

import (
	"context"
	"database/sql"
)

type Dialect string

const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
)

type Durability string

const (
	DurabilityStrict   Durability = "strict"
	DurabilityBalanced Durability = "balanced"
)

// Driver intentionally exposes only database/sql. SQL builders, rebinding,
// migrations, and sqlx are implementation details of the queue engine.
type Driver interface {
	DB() *sql.DB
	Dialect() Dialect
	DriverName() string
	Close() error
}

// NotificationSource is an optional capability. Queue correctness never
// depends on notifications; supported drivers use them to avoid polling
// latency while bounded reconciliation remains the fallback.
type NotificationSource interface {
	Listen(context.Context, string) (<-chan string, error)
}

// NotificationHealthSource optionally exposes the state of the driver's
// dedicated listener connection. It is operational health only; polling keeps
// queue correctness independent from this signal.
type NotificationHealthSource interface {
	NotificationHealthy() bool
}

// SQLiteMaintenanceSource marks a local SQLite driver that supports WAL
// checkpoint and incremental-vacuum pragmas. Remote SQLite-compatible drivers
// such as Turso intentionally do not implement this capability.
type SQLiteMaintenanceSource interface {
	SQLiteMaintenanceEnabled() bool
}
