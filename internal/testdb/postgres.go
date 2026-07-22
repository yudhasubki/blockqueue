// Package testdb provides guarded, schema-isolated PostgreSQL databases for
// integration tests and benchmarks. It is internal so applications cannot
// accidentally depend on test provisioning behavior.
package testdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

const (
	postgresDriverName = "pgx"
	postgresScheme     = "postgres"
	postgresqlScheme   = "postgresql"
)

type Schema struct {
	Driver       store.Driver
	DatabaseName string
	Name         string

	admin     *sqlx.DB
	closeOnce sync.Once
	closeErr  error
}

type schemaDriver struct {
	database  *sqlx.DB
	closeOnce sync.Once
	closeErr  error
}

func (driver *schemaDriver) DB() *sql.DB            { return driver.database.DB }
func (driver *schemaDriver) Dialect() store.Dialect { return store.DialectPostgres }
func (driver *schemaDriver) DriverName() string     { return postgresDriverName }
func (driver *schemaDriver) Close() error {
	driver.closeOnce.Do(func() { driver.closeErr = driver.database.Close() })
	return driver.closeErr
}

// OpenPostgreSQLSchema creates a random schema after verifying that the target
// database name has the required test/benchmark suffix. This prevents an
// accidental environment variable from targeting a production database.
func OpenPostgreSQLSchema(ctx context.Context, rawURL, requiredDatabaseSuffix, prefix string) (*Schema, error) {
	parsed, databaseName, err := guardedURL(rawURL, requiredDatabaseSuffix)
	if err != nil {
		return nil, err
	}
	if !identifierPattern.MatchString(prefix) {
		return nil, fmt.Errorf("invalid schema prefix %q", prefix)
	}
	schemaName := prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin, err := sqlx.ConnectContext(ctx, postgresDriverName, parsed.String())
	if err != nil {
		return nil, fmt.Errorf("connect PostgreSQL test database: %w", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("create PostgreSQL test schema: %w", err)
	}

	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	connection, err := sqlx.ConnectContext(ctx, postgresDriverName, parsed.String())
	if err != nil {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
		_ = admin.Close()
		return nil, fmt.Errorf("connect isolated PostgreSQL schema: %w", err)
	}
	connection.SetMaxOpenConns(25)
	connection.SetMaxIdleConns(10)

	var actualDatabase, actualSchema string
	if err := connection.QueryRowxContext(ctx, "SELECT current_database(), current_schema()").Scan(&actualDatabase, &actualSchema); err != nil {
		_ = connection.Close()
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
		_ = admin.Close()
		return nil, fmt.Errorf("verify PostgreSQL test isolation: %w", err)
	}
	if actualDatabase != databaseName || actualSchema != schemaName {
		_ = connection.Close()
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
		_ = admin.Close()
		return nil, fmt.Errorf("PostgreSQL isolation mismatch: database=%q schema=%q", actualDatabase, actualSchema)
	}

	driver := &schemaDriver{database: connection}
	return &Schema{
		Driver:       driver,
		DatabaseName: databaseName,
		Name:         schemaName,
		admin:        admin,
	}, nil
}

func (schema *Schema) Close() error {
	schema.closeOnce.Do(func() {
		driverErr := schema.Driver.Close()
		_, dropErr := schema.admin.ExecContext(context.Background(),
			"DROP SCHEMA "+pgx.Identifier{schema.Name}.Sanitize()+" CASCADE")
		adminErr := schema.admin.Close()
		schema.closeErr = errors.Join(driverErr, dropErr, adminErr)
	})
	return schema.closeErr
}

func guardedURL(rawURL, requiredDatabaseSuffix string) (*url.URL, string, error) {
	if rawURL == "" {
		return nil, "", errors.New("PostgreSQL URL is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != postgresScheme && parsed.Scheme != postgresqlScheme) {
		return nil, "", fmt.Errorf("invalid PostgreSQL URL")
	}
	databaseName, err := url.PathUnescape(strings.TrimPrefix(parsed.EscapedPath(), "/"))
	if err != nil || databaseName == "" || strings.Contains(databaseName, "/") {
		return nil, "", errors.New("PostgreSQL URL must name one database")
	}
	if requiredDatabaseSuffix == "" || !strings.HasSuffix(databaseName, requiredDatabaseSuffix) {
		return nil, "", fmt.Errorf("refusing PostgreSQL database %q: name must end with %q", databaseName, requiredDatabaseSuffix)
	}
	return parsed, databaseName, nil
}
