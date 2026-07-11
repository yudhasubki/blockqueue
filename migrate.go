package blockqueue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

var ErrMigrationChecksum = errors.New("migration checksum mismatch")

//go:embed migration/sqlite/*.sql migration/pgsql/*.sql
var migrationFiles embed.FS

type embeddedMigration struct {
	version  string
	path     string
	contents string
	checksum string
}

// Migrate installs the v0.2 schema and applies future ordered migrations.
// v0.2 is a clean schema break: callers must provide a new database rather
// than a database created by v0.1.
func Migrate(ctx context.Context, driver store.Driver) error {
	connection := sqlx.NewDb(driver.DB(), driver.DriverName())
	dialect, err := newSQLDialect(driver.Dialect())
	if err != nil {
		return err
	}

	migrations, err := loadEmbeddedMigrations(dialect.migrationDirectory())
	if err != nil {
		return err
	}
	return applyMigrations(ctx, connection, dialect, migrations)
}

func applyMigrations(ctx context.Context, connection *sqlx.DB, dialect sqlDialect, migrations []embeddedMigration) error {
	tx, err := connection.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := dialect.lockMigrations(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, migrationTableSQL(dialect.migrationDirectory())); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	for _, migration := range migrations {
		var checksum string
		err := tx.GetContext(ctx, &checksum,
			tx.Rebind("SELECT checksum FROM schema_migrations WHERE version = ?"),
			migration.version,
		)
		switch {
		case err == nil:
			if checksum != migration.checksum {
				return fmt.Errorf("%w: version %s", ErrMigrationChecksum, migration.version)
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read migration %s: %w", migration.version, err)
		}
		if _, err := tx.ExecContext(ctx, migration.contents); err != nil {
			return fmt.Errorf("apply migration %s (%s): %w", migration.version, migration.path, err)
		}
		if _, err := tx.ExecContext(ctx,
			tx.Rebind("INSERT INTO schema_migrations (version, checksum) VALUES (?, ?)"),
			migration.version,
			migration.checksum,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", migration.version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func ensureMigrationTable(ctx context.Context, connection *sqlx.DB, backend string) error {
	_, err := connection.ExecContext(ctx, migrationTableSQL(backend))
	return err
}

func migrationTableSQL(backend string) string {
	appliedType := "DATETIME"
	if backend == "pgsql" {
		appliedType = "TIMESTAMPTZ"
	}
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			checksum VARCHAR(64) NOT NULL,
			applied_at %s NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`, appliedType)
}

func loadEmbeddedMigrations(backend string) ([]embeddedMigration, error) {
	directory := "migration/" + backend
	entries, err := fs.ReadDir(migrationFiles, directory)
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	migrations := make([]embeddedMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		path := directory + "/" + entry.Name()
		body, err := migrationFiles.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", path, err)
		}
		sum := sha256.Sum256(body)
		migrations = append(migrations, embeddedMigration{
			version:  strings.TrimSuffix(entry.Name(), ".sql"),
			path:     path,
			contents: string(body),
			checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	return migrations, nil
}
