// Package postgres provides the production PostgreSQL storage driver.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

const (
	driverName                = "pgx"
	connectionScheme          = "postgres"
	defaultSSLMode            = "require"
	synchronousCommitStrict   = "on"
	synchronousCommitBalanced = "local"
)

type Driver struct {
	db            *sqlx.DB
	connectionURL string
	listenerOK    atomic.Bool
}

type Config struct {
	Host     string
	Username string
	Password string
	Name     string
	Port     int
	Timezone string
	// MaxOpenConns defaults to 25. A positive value must be at least 2 because
	// the maintenance leader pins one pooled connection while running queries on
	// another.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	SSLMode         string
	Durability      store.Durability
}

func Open(config Config) (*Driver, error) {
	if strings.TrimSpace(config.Host) == "" || strings.TrimSpace(config.Username) == "" || strings.TrimSpace(config.Name) == "" {
		return nil, errors.New("postgres host, username, and database name are required")
	}
	if config.Port < 0 || config.Port > 65535 || config.MaxOpenConns < 0 || config.MaxIdleConns < 0 ||
		config.ConnMaxLifetime < 0 || config.ConnMaxIdleTime < 0 {
		return nil, errors.New("postgres connection settings are invalid")
	}
	if config.MaxOpenConns == 1 {
		return nil, errors.New("postgres max open connections must be 0 (default) or at least 2")
	}
	connectionString, err := buildConnectionURL(config)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Connect(driverName, connectionString)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	maxOpen := config.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 25
	}
	maxIdle := config.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 10
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	lifetime := config.ConnMaxLifetime
	if lifetime <= 0 {
		lifetime = 5 * time.Minute
	}
	db.SetConnMaxLifetime(lifetime)
	idleTime := config.ConnMaxIdleTime
	if idleTime <= 0 {
		idleTime = time.Minute
	}
	db.SetConnMaxIdleTime(idleTime)
	return &Driver{db: db, connectionURL: connectionString}, nil
}

func buildConnectionURL(config Config) (string, error) {
	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = defaultSSLMode
	}
	switch sslMode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		return "", fmt.Errorf("unsupported postgres ssl mode %q", sslMode)
	}
	user := url.User(config.Username)
	if config.Password != "" {
		user = url.UserPassword(config.Username, config.Password)
	}
	connectionURL := &url.URL{Scheme: connectionScheme, User: user, Host: config.Host, Path: config.Name}
	if config.Port > 0 {
		connectionURL.Host = net.JoinHostPort(strings.Trim(config.Host, "[]"), strconv.Itoa(config.Port))
	}
	synchronousCommit := synchronousCommitStrict
	switch config.Durability {
	case "", store.DurabilityStrict:
	case store.DurabilityBalanced:
		synchronousCommit = synchronousCommitBalanced
	default:
		return "", fmt.Errorf("unsupported postgres durability mode %q", config.Durability)
	}
	params := url.Values{
		"sslmode": []string{sslMode},
		"options": []string{"-c synchronous_commit=" + synchronousCommit},
	}
	if config.Timezone != "" {
		params.Set("TimeZone", config.Timezone)
	}
	connectionURL.RawQuery = params.Encode()
	return connectionURL.String(), nil
}

func (driver *Driver) DB() *sql.DB               { return driver.db.DB }
func (driver *Driver) Dialect() store.Dialect    { return store.DialectPostgres }
func (driver *Driver) DriverName() string        { return driverName }
func (driver *Driver) Close() error              { return driver.db.Close() }
func (driver *Driver) NotificationHealthy() bool { return driver.listenerOK.Load() }

func (driver *Driver) Listen(ctx context.Context, channel string) (<-chan string, error) {
	connection, err := openListener(ctx, driver.connectionURL, channel)
	if err != nil {
		driver.listenerOK.Store(false)
		return nil, err
	}
	driver.listenerOK.Store(true)
	output := make(chan string, 64)
	go func() {
		defer close(output)
		defer driver.listenerOK.Store(false)
		defer func() {
			if connection != nil {
				_ = connection.Close(context.Background())
			}
		}()
		backoff := time.Second
		for {
			if connection == nil {
				driver.listenerOK.Store(false)
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
				}
				var connectErr error
				connection, connectErr = openListener(ctx, driver.connectionURL, channel)
				if connectErr != nil {
					backoff = min(backoff*2, 30*time.Second)
					continue
				}
				driver.listenerOK.Store(true)
				backoff = time.Second
			}
			notification, waitErr := connection.WaitForNotification(ctx)
			if waitErr == nil {
				backoff = time.Second
				select {
				case output <- notification.Payload:
				default:
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			_ = connection.Close(context.Background())
			driver.listenerOK.Store(false)
			connection = nil
			backoff = min(backoff*2, 30*time.Second)
		}
	}()
	return output, nil
}

func openListener(ctx context.Context, connectionURL, channel string) (*pgx.Conn, error) {
	connection, err := pgx.Connect(ctx, connectionURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres listener: %w", err)
	}
	if _, err := connection.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = connection.Close(context.Background())
		return nil, fmt.Errorf("listen postgres channel: %w", err)
	}
	return connection, nil
}
