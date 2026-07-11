// Package turso provides experimental libSQL/Turso storage support.
package turso

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	"github.com/yudhasubki/blockqueue/store"
)

type Driver struct{ db *sqlx.DB }

func Open(databaseURL string) (*Driver, error) {
	db, err := sqlx.Open("libsql", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Driver{db: db}, nil
}

func (driver *Driver) DB() *sql.DB            { return driver.db.DB }
func (driver *Driver) Dialect() store.Dialect { return store.DialectSQLite }
func (driver *Driver) DriverName() string     { return "libsql" }
func (driver *Driver) Close() error           { return driver.db.Close() }
