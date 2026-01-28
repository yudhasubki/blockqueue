package sqlite

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	Database *sqlx.DB
}

type Config struct {
	BusyTimeout     int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	CacheSize       int   // KB (negative = KB, positive = pages), default -64000 (64MB)
	MmapSize        int64 // Memory-mapped I/O size in bytes, default 268435456 (256MB)
}

func New(dbName string, config Config) (*SQLite, error) {
	// Apply defaults
	cacheSize := config.CacheSize
	if cacheSize == 0 {
		cacheSize = -64000 // 64MB
	}

	mmapSize := config.MmapSize
	if mmapSize == 0 {
		mmapSize = 268435456 // 256MB
	}

	db, err := sqlx.Connect("sqlite3",
		fmt.Sprintf(
			"file:%s?_synchronous=normal&_journal_mode=wal&_cache_size=%d&_mmap_size=%d&_temp_store=memory&_foreign_keys=on&busy_timeout=%d&_txlock=immediate",
			dbName,
			cacheSize,
			mmapSize,
			config.BusyTimeout,
		),
	)
	if err != nil {
		return &SQLite{}, err
	}

	maxIdleConns := config.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = 10
	}
	db.SetMaxIdleConns(maxIdleConns)

	connMaxIdleTime := config.ConnMaxIdleTime
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 5 * time.Minute
	}
	db.SetConnMaxIdleTime(connMaxIdleTime)

	return &SQLite{
		Database: db,
	}, nil
}

func (sqlite *SQLite) Conn() *sqlx.DB {
	return sqlite.Database
}

func (sqlite *SQLite) Close() error {
	return sqlite.Database.Close()
}
