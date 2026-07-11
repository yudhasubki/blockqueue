// Package turso provides experimental libSQL/Turso storage support.
package turso

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	"github.com/yudhasubki/blockqueue/store"
)

const driverName = "libsql"

var ErrEmptyURL = errors.New("turso database URL is required")

type Driver struct{ db *sqlx.DB }

func Open(databaseURL string) (*Driver, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return nil, ErrEmptyURL
	}
	db, err := sqlx.Open(driverName, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return &Driver{db: db}, nil
}

func (driver *Driver) DB() *sql.DB            { return driver.db.DB }
func (driver *Driver) Dialect() store.Dialect { return store.DialectSQLite }
func (driver *Driver) DriverName() string     { return driverName }
func (driver *Driver) Close() error           { return driver.db.Close() }
