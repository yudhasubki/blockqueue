// Package sqlite provides the production SQLite storage driver.
package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/yudhasubki/blockqueue/store"
)

const (
	driverName          = "blockqueue_sqlite3"
	synchronousStrict   = "full"
	synchronousBalanced = "normal"
)

var ErrEmptyPath = errors.New("sqlite database path is required")

func init() {
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		_, err := conn.Exec("PRAGMA wal_autocheckpoint = 0", nil)
		return err
	}})
}

type Driver struct{ db *sqlx.DB }

type Config struct {
	BusyTimeout     int
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	CacheSize       int
	MmapSize        int64
	Durability      store.Durability
}

func Open(path string, config Config) (*Driver, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrEmptyPath
	}
	if config.BusyTimeout < 0 || config.MaxOpenConns < 0 || config.MaxIdleConns < 0 ||
		config.ConnMaxIdleTime < 0 || config.MmapSize < 0 {
		return nil, errors.New("sqlite connection settings cannot be negative")
	}
	cacheSize := config.CacheSize
	if cacheSize == 0 {
		cacheSize = -64000
	}
	mmapSize := config.MmapSize
	if mmapSize == 0 {
		mmapSize = 256 << 20
	}
	busyTimeout := config.BusyTimeout
	if busyTimeout <= 0 {
		busyTimeout = 5000
	}
	synchronous := synchronousStrict
	switch config.Durability {
	case "", store.DurabilityStrict:
	case store.DurabilityBalanced:
		synchronous = synchronousBalanced
	default:
		return nil, fmt.Errorf("unsupported sqlite durability mode %q", config.Durability)
	}

	escapedPath := (&url.URL{Path: path}).EscapedPath()
	parameters := url.Values{
		"_synchronous":  []string{synchronous},
		"_journal_mode": []string{"wal"},
		"_cache_size":   []string{fmt.Sprint(cacheSize)},
		"_mmap_size":    []string{fmt.Sprint(mmapSize)},
		"_temp_store":   []string{"memory"},
		"_foreign_keys": []string{"on"},
		"_busy_timeout": []string{fmt.Sprint(busyTimeout)},
		"_txlock":       []string{"immediate"},
		"_auto_vacuum":  []string{"2"},
	}
	db, err := sqlx.Connect(driverName, "file:"+escapedPath+"?"+parameters.Encode())
	if err != nil {
		return nil, err
	}
	maxOpen := config.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 10
	}
	if path == ":memory:" {
		maxOpen = 1
	}
	maxIdle := config.MaxIdleConns
	if maxIdle <= 0 || maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	idleTime := config.ConnMaxIdleTime
	if idleTime <= 0 {
		idleTime = 5 * time.Minute
	}
	db.SetConnMaxIdleTime(idleTime)

	var autoVacuum int
	if err := db.Get(&autoVacuum, "PRAGMA auto_vacuum"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("verify auto_vacuum: %w", err)
	}
	if autoVacuum != 2 {
		var tableCount int
		if err := db.Get(&tableCount, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"); err != nil {
			_ = db.Close()
			return nil, err
		}
		if tableCount == 0 {
			if _, err := db.Exec("PRAGMA auto_vacuum = incremental"); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("set auto_vacuum: %w", err)
			}
			if _, err := db.Exec("VACUUM"); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("bootstrap incremental auto_vacuum: %w", err)
			}
		}
	}
	return &Driver{db: db}, nil
}

func (driver *Driver) DB() *sql.DB                    { return driver.db.DB }
func (driver *Driver) Dialect() store.Dialect         { return store.DialectSQLite }
func (driver *Driver) DriverName() string             { return driverName }
func (driver *Driver) Close() error                   { return driver.db.Close() }
func (driver *Driver) SQLiteMaintenanceEnabled() bool { return true }
