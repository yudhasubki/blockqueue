package postgre

import (
	"fmt"
	"log/slog"
	"net/url"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Postgre struct {
	Database *sqlx.DB
}

type Config struct {
	Host         string
	Username     string
	Password     string
	Name         string
	Port         int
	Timezone     string
	MaxOpenConns int
	MaxIdleConns int
}

func New(config Config) (*Postgre, error) {
	db, err := sqlx.Connect("postgres", fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=%s",
		config.Host,
		config.Username,
		config.Password,
		config.Name,
		config.Port,
		url.QueryEscape(config.Timezone)))
	if err != nil {
		slog.Error("[pgx.NewConnPool] failed to connect to database", "error", err)
		return &Postgre{}, nil
	}

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
