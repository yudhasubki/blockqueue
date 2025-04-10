package sqlite

import (
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	Database *sqlx.DB
}

type Config struct {
	BusyTimeout int
}

func New(filename string, cfg Config) (*SQLite, error) {
	conn, err := sqlx.Connect("sqlite3", filename+"?_journal=WAL&_busy_timeout="+strconv.Itoa(cfg.BusyTimeout)+"&cache=shared")
	if err != nil {
		return nil, err
	}

	// Optimized connection pool settings - scale better with both low and high concurrency
	conn.SetMaxOpenConns(35) // Increased from 25 for better low concurrency performance
	conn.SetMaxIdleConns(15) // Increased from 10 for better reuse at low concurrency
	conn.SetConnMaxLifetime(time.Hour)
	conn.SetConnMaxIdleTime(10 * time.Minute) // Add explicit idle timeout

	// Execute pragmas for better performance with adaptive settings
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL", // Keep NORMAL for reliability
		"PRAGMA cache_size = 20000",   // Increased to 20MB for better caching
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 134217728",     // 128MB mmap (increased for better read performance)
		"PRAGMA page_size = 8192",          // Larger page size for better sequential reads
		"PRAGMA busy_timeout = 5000",       // Consistent timeout
		"PRAGMA wal_autocheckpoint = 1000", // Checkpoint WAL after 1000 pages
	}

	for _, pragma := range pragmas {
		_, err = conn.Exec(pragma)
		if err != nil {
			return nil, err
		}
	}

	return &SQLite{Database: conn}, nil
}

func (sqlite *SQLite) Conn() *sqlx.DB {
	return sqlite.Database
}

func (sqlite *SQLite) Close() error {
	return sqlite.Database.Close()
}
