package blockqueue

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"
)

const (
	postgresClassConnectionException  = "08"
	postgresClassTransactionRollback  = "40"
	postgresClassInsufficientResource = "53"
	postgresClassSystemError          = "58"
	postgresLockNotAvailable          = "55P03"
	postgresAdminShutdown             = "57P01"
	postgresCrashShutdown             = "57P02"
	postgresCannotConnectNow          = "57P03"
)

var transientWriteErrorFragments = []string{
	"database is locked",
	"database table is locked",
	"connection reset",
	"broken pipe",
	"connection refused",
	"server closed",
}

func isDomainWriteError(err error) bool {
	return errors.Is(err, ErrInvalidPublish) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrNoActiveSubscriber) ||
		errors.Is(err, ErrTopicNotFound)
}

func isTransientWriteError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, ErrNoActiveSubscriber) ||
		errors.Is(err, ErrTopicNotFound) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return strings.HasPrefix(code, postgresClassConnectionException) ||
			strings.HasPrefix(code, postgresClassTransactionRollback) ||
			strings.HasPrefix(code, postgresClassInsufficientResource) ||
			code == postgresLockNotAvailable || code == postgresAdminShutdown ||
			code == postgresCrashShutdown || code == postgresCannotConnectNow ||
			strings.HasPrefix(code, postgresClassSystemError)
	}
	if sqliteCode, ok := sqlitePrimaryErrorCode(err); ok {
		switch sqliteCode {
		case sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrIoErr, sqlite3.ErrInterrupt,
			sqlite3.ErrProtocol, sqlite3.ErrFull, sqlite3.ErrNomem:
			return true
		default:
			return false
		}
	}
	lower := strings.ToLower(err.Error())
	for _, fragment := range transientWriteErrorFragments {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func isAmbiguousCommitError(err error) bool {
	// Once Commit has been invoked, a context cancellation or deadline cannot
	// prove whether the server committed before the client stopped waiting.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return strings.HasPrefix(code, postgresClassConnectionException) ||
			strings.HasPrefix(code, postgresClassSystemError) ||
			code == postgresAdminShutdown || code == postgresCrashShutdown || code == postgresCannotConnectNow
	}
	if sqliteCode, ok := sqlitePrimaryErrorCode(err); ok {
		return sqliteCode == sqlite3.ErrIoErr || sqliteCode == sqlite3.ErrProtocol
	}
	lower := strings.ToLower(err.Error())
	for _, fragment := range transientWriteErrorFragments {
		if fragment != "database is locked" && fragment != "database table is locked" &&
			strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

// sqlitePrimaryErrorCode accepts every error shape exposed by go-sqlite3:
// value or pointer Error structs as well as bare primary/extended result codes.
// Extended result codes retain the primary code in their low byte.
func sqlitePrimaryErrorCode(err error) (sqlite3.ErrNo, bool) {
	var value sqlite3.Error
	if errors.As(err, &value) {
		return value.Code, true
	}
	var pointer *sqlite3.Error
	if errors.As(err, &pointer) && pointer != nil {
		return pointer.Code, true
	}
	var primary sqlite3.ErrNo
	if errors.As(err, &primary) {
		return primary, true
	}
	var extended sqlite3.ErrNoExtended
	if errors.As(err, &extended) {
		return sqlite3.ErrNo(int(extended) & 0xff), true
	}
	return 0, false
}
