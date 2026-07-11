package persistence

import "errors"

var (
	ErrTopicNotFound      = errors.New("topic not found")
	ErrNoActiveSubscriber = errors.New("topic has no active subscriber")
	ErrInvalidPublish     = errors.New("invalid publish request")
	ErrResourceConflict   = errors.New("resource already exists")
	ErrSubscriberNotFound = errors.New("subscriber not found")

	ErrLeaseLost        = errors.New("delivery lease lost")
	ErrDeliveryNotFound = errors.New("delivery not found")
	ErrInvalidReceipt   = errors.New("receipt_token is required")
	ErrDeliveryTerminal = errors.New("delivery is already terminal")

	ErrIdempotencyConflict = errors.New("idempotency key conflicts with a different message")
	ErrWriterClosed        = errors.New("writer closed")

	ErrScheduleNotFound  = errors.New("schedule not found")
	ErrScheduleVersion   = errors.New("stale schedule version")
	ErrScheduleOverlap   = errors.New("previous schedule run is still active")
	ErrScheduleLeaseLost = errors.New("schedule lease lost")

	ErrUnsupportedDialect = errors.New("unsupported database dialect")
	ErrMigrationChecksum  = errors.New("migration checksum mismatch")
)
