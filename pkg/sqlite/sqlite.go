package sqlite

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	Database *sqlx.DB
}

type Config struct {
	BusyTimeout int
}

func New(dbName string, config Config) (*SQLite, error) {
	db, err := sqlx.Connect("sqlite3",
		fmt.Sprintf(
			"file:%s?_synchronous=normal&_journal_mode=wal&busy_timeout=%d",
			dbName,
			config.BusyTimeout,
		),
	)
	if err != nil {
		return &SQLite{}, err
	}

	return &SQLite{
		Database: db,
	}, nil
}

func (sqlite *SQLite) Close() error {
	return sqlite.Database.Close()
}
