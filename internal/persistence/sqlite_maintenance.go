package persistence

import (
	"context"
	"fmt"
)

type sqliteCheckpointMode string

const (
	sqliteCheckpointPassive  sqliteCheckpointMode = "PASSIVE"
	sqliteCheckpointTruncate sqliteCheckpointMode = "TRUNCATE"
)

type sqliteCheckpointResult struct {
	Busy         int
	LogFrames    int
	Checkpointed int
}

func (d *db) checkpointSQLite(ctx context.Context, mode sqliteCheckpointMode) (sqliteCheckpointResult, error) {
	if mode != sqliteCheckpointPassive && mode != sqliteCheckpointTruncate {
		return sqliteCheckpointResult{}, fmt.Errorf("unsupported SQLite checkpoint mode %q", mode)
	}

	var result sqliteCheckpointResult
	row := d.Conn().QueryRowxContext(ctx, "PRAGMA wal_checkpoint("+string(mode)+")")
	err := row.Scan(&result.Busy, &result.LogFrames, &result.Checkpointed)
	return result, err
}

func (d *db) incrementalVacuum(ctx context.Context) error {
	_, err := d.Conn().ExecContext(ctx, "PRAGMA incremental_vacuum")
	return err
}
