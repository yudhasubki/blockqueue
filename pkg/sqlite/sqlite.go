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

	// SQLite connection string with performance optimizations:
	// - _synchronous=normal: balanced durability/performance (safe with WAL)
	// - _journal_mode=wal: enables concurrent reads while writing
	// - _cache_size: page cache size
	// - _mmap_size: memory-mapped I/O for faster reads
	// - _temp_store=memory: temp tables in memory
	// - _foreign_keys=on: enforce referential integrity
	db, err := sqlx.Connect("sqlite3",
		fmt.Sprintf(
			"file:%s?_synchronous=normal&_journal_mode=wal&_cache_size=%d&_mmap_size=%d&_temp_store=memory&_foreign_keys=on&busy_timeout=%d",
			dbName,
			cacheSize,
			mmapSize,
			config.BusyTimeout,
		),
	)
	if err != nil {
		return &SQLite{}, err
	}

	// Connection pool tuning for SQLite:
	// Note: In WAL mode, SQLite handles write serialization internally via busy_timeout.
	// We don't limit MaxOpenConns because doing so can cause deadlocks when multiple
	// goroutines wait for the single connection. The busy_timeout handles write contention.
	// - MaxIdleConns: keep connections warm for reads
	// - ConnMaxLifetime(0): don't expire connections
	// - ConnMaxIdleTime: release idle connections after timeout

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
