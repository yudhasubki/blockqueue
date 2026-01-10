package postgre

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Postgre struct {
	Database *sqlx.DB
}

type Config struct {
	Host            string
	Username        string
	Password        string
	Name            string
	Port            int
	Timezone        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func New(config Config) (*Postgre, error) {
	// Build connection string parts, skipping empty values
	parts := []string{
		fmt.Sprintf("host=%s", config.Host),
		fmt.Sprintf("user=%s", config.Username),
		fmt.Sprintf("dbname=%s", config.Name),
		fmt.Sprintf("port=%d", config.Port),
		"sslmode=disable",
	}

	// Only add password if not empty
	if config.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", config.Password))
	}

	// Only add timezone if not empty
	if config.Timezone != "" {
		parts = append(parts, fmt.Sprintf("TimeZone=%s", url.QueryEscape(config.Timezone)))
	}

	connStr := strings.Join(parts, " ")

	slog.Debug("PostgreSQL connection", "connStr", connStr)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		slog.Error("[pgx.NewConnPool] failed to connect to database", "error", err)
		return &Postgre{}, err
	}

	// Connection pool settings for better performance
	maxOpenConns := config.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = 25 // Default for PostgreSQL
	}
	db.SetMaxOpenConns(maxOpenConns)

	maxIdleConns := config.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = 10
	}
	db.SetMaxIdleConns(maxIdleConns)

	connMaxLifetime := config.ConnMaxLifetime
	if connMaxLifetime <= 0 {
		connMaxLifetime = 5 * time.Minute
	}
	db.SetConnMaxLifetime(connMaxLifetime)

	connMaxIdleTime := config.ConnMaxIdleTime
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 1 * time.Minute
	}
	db.SetConnMaxIdleTime(connMaxIdleTime)

	return &Postgre{
		Database: db,
	}, nil
}

func (pg *Postgre) Conn() *sqlx.DB {
	return pg.Database
}

func (pg *Postgre) Close() error {
	return pg.Database.Close()
}
