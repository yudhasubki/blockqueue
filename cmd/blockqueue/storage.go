package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	blockqueue "github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/postgres"
	"github.com/yudhasubki/blockqueue/store/sqlite"
	"github.com/yudhasubki/blockqueue/store/turso"
)

// openConfiguredDriver is the composition root for storage backends. Runtime
// queue code depends only on store.Driver and its optional capabilities.
func openConfiguredDriver(config Config) (store.Driver, error) {
	switch strings.ToLower(strings.TrimSpace(config.Http.Driver)) {
	case storageDriverTurso:
		if strings.TrimSpace(config.Turso.URL) == "" {
			return nil, errors.New("turso.url is required")
		}
		driver, err := turso.Open(config.Turso.URL)
		if err != nil {
			return nil, fmt.Errorf("open turso: %w", err)
		}
		return driver, nil
	case storageDriverPGSQL, storageDriverPostgres, storageDriverPostgreSQL:
		if strings.TrimSpace(config.PgSQL.Host) == "" || strings.TrimSpace(config.PgSQL.Username) == "" || strings.TrimSpace(config.PgSQL.Name) == "" {
			return nil, errors.New("pgsql.host, pgsql.username, and pgsql.name are required")
		}
		driver, err := postgres.Open(postgres.Config{
			Host:         config.PgSQL.Host,
			Username:     config.PgSQL.Username,
			Password:     config.PgSQL.Password,
			Name:         config.PgSQL.Name,
			Port:         config.PgSQL.Port,
			Timezone:     config.PgSQL.Timezone,
			MaxOpenConns: config.PgSQL.MaxOpenConns,
			MaxIdleConns: config.PgSQL.MaxIdleConns,
			SSLMode:      config.PgSQL.SSLMode,
			Durability:   store.Durability(config.PgSQL.Durability),
		})
		if err != nil {
			return nil, fmt.Errorf("open postgres: %w", err)
		}
		return driver, nil
	case storageDriverSQLite, "":
		if strings.TrimSpace(config.SQLite.DatabaseName) == "" {
			return nil, errors.New("sqlite.db_name is required")
		}
		driver, err := sqlite.Open(config.SQLite.DatabaseName, sqlite.Config{
			BusyTimeout:  config.SQLite.BusyTimeout,
			MaxOpenConns: config.SQLite.MaxOpenConns,
			MaxIdleConns: config.SQLite.MaxIdleConns,
			CacheSize:    config.SQLite.CacheSize,
			MmapSize:     config.SQLite.MmapSize,
			Durability:   store.Durability(config.SQLite.Durability),
		})
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		return driver, nil
	default:
		return nil, fmt.Errorf("unsupported driver %q", config.Http.Driver)
	}
}

func configuredCheckpointInterval(config Config) (time.Duration, error) {
	driver := strings.ToLower(strings.TrimSpace(config.Http.Driver))
	if (driver != "" && driver != storageDriverSQLite) || config.SQLite.CheckpointInterval == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(config.SQLite.CheckpointInterval)
	if err != nil || interval < blockqueue.MinimumCheckpointInterval {
		return 0, fmt.Errorf("sqlite.checkpoint_interval must be at least %s", blockqueue.MinimumCheckpointInterval)
	}
	return interval, nil
}
