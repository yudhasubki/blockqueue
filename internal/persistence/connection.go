package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

type db struct {
	Database   store.Driver
	connection *sqlx.DB
	dialect    sqlDialect
	dialectErr error
	statements *statementCache
	closeOnce  sync.Once
	closeErr   error
}

func newDb(driver store.Driver) *db {
	dialect, dialectErr := newSQLDialect(driver.Dialect())
	return &db{
		Database: driver, connection: sqlx.NewDb(driver.DB(), driver.DriverName()), dialect: dialect, dialectErr: dialectErr,
		statements: newStatementCache(defaultStatementCacheSize),
	}
}

func (d *db) Conn() *sqlx.DB { return d.connection }

func (d *db) close() error {
	d.closeOnce.Do(func() {
		d.closeErr = errors.Join(d.statements.close(), d.Database.Close())
	})
	return d.closeErr
}

func boolLiteral(d *db, value bool) string {
	return d.dialect.boolLiteral(value)
}

func (d *db) supportsSQLiteMaintenance() bool {
	source, ok := d.Database.(store.SQLiteMaintenanceSource)
	return ok && source.SQLiteMaintenanceEnabled()
}

func databaseTime(value any) (time.Time, bool, error) {
	if value == nil {
		return time.Time{}, false, nil
	}
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), true, nil
	case []byte:
		value = string(typed)
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, false, fmt.Errorf("unsupported database time type %T", value)
	}
	text = strings.TrimSpace(text)
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid database time %q", text)
}

func (d *db) tx(ctx context.Context, fn func(context.Context, *sqlx.Tx) error) error {
	return d.withTx(ctx, nil, fn)
}

func (d *db) withTx(ctx context.Context, external *sql.Tx, fn func(context.Context, *sqlx.Tx) error) error {
	if external != nil {
		return fn(ctx, &sqlx.Tx{Tx: external, Mapper: d.Conn().Mapper})
	}
	tx, err := d.Conn().BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (d *db) notifyTx(ctx context.Context, tx *sqlx.Tx, payload string) error {
	if !d.dialect.usesDatabaseEvents() {
		return nil
	}
	_, err := tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", EventChannel, payload)
	return err
}

func (d *db) notifyDatabase(ctx context.Context, payload string) error {
	if !d.dialect.usesDatabaseEvents() {
		return nil
	}
	_, err := d.Conn().ExecContext(ctx, "SELECT pg_notify($1, $2)", EventChannel, payload)
	return err
}
