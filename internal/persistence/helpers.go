package persistence

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"
)

func equalJSON(left, right string) bool {
	var leftValue, rightValue any
	if json.Unmarshal([]byte(left), &leftValue) != nil || json.Unmarshal([]byte(right), &rightValue) != nil {
		return left == right
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func normalizeResourceConflict(err error) error {
	if err == nil {
		return nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == postgresUniqueViolationCode {
		return ErrResourceConflict
	}
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) && (sqliteError.ExtendedCode == sqlite3.ErrConstraintUnique ||
		sqliteError.ExtendedCode == sqlite3.ErrConstraintPrimaryKey) {
		return ErrResourceConflict
	}
	return err
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
