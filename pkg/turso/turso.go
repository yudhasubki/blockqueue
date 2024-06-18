package turso

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

type Turso struct {
	Database *sqlx.DB
}

func New(url string) (*Turso, error) {
	db, err := sqlx.Open("libsql", url)
	if err != nil {
		return nil, err
	}
	return &Turso{
		Database: db,
	}, nil
}

func (t *Turso) Conn() *sqlx.DB {
	return t.Database
}

func (t *Turso) Close() error {
	return t.Database.Close()
}
