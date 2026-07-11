package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrDeliveryTerminal reports an attempted cancellation of a delivery that
// was already processed or dead-lettered.
var ErrDeliveryTerminal = errors.New("delivery is already terminal")

// SnoozeDelivery returns an active lease to pending without recording a
// failure or consuming the subscriber's failure budget.
func (q *Queue) SnoozeDelivery(
	ctx context.Context,
	topic Topic,
	subscriber, messageID, receipt string,
	delay time.Duration,
) (time.Time, error) {
	return q.snoozeDeliveryTx(ctx, nil, topic, subscriber, messageID, receipt, delay)
}

// SnoozeDeliveryTx is SnoozeDelivery within a caller-owned transaction.
func (q *Queue) SnoozeDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	topic Topic,
	subscriber, messageID, receipt string,
	delay time.Duration,
) (time.Time, error) {
	if tx == nil {
		return time.Time{}, fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	return q.snoozeDeliveryTx(ctx, tx, topic, subscriber, messageID, receipt, delay)
}

func (q *Queue) snoozeDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	topic Topic,
	subscriber, messageID, receipt string,
	delay time.Duration,
) (time.Time, error) {
	if err := q.requireTransactionAllowed(tx); err != nil {
		return time.Time{}, err
	}
	if delay < 0 {
		return time.Time{}, fmt.Errorf("%w: snooze delay cannot be negative", ErrInvalidPublish)
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return time.Time{}, err
	}
	if err := validateDeliveryIdentity(messageID, receipt); err != nil {
		return time.Time{}, err
	}
	visibleAt, err := q.db.snoozeDeliveryWithTx(ctx, tx, runtime.id, messageID, receipt, delay)
	if err == nil && tx == nil {
		runtime.notify()
	}
	return visibleAt, err
}

// CancelDelivery terminally cancels one subscriber delivery. Repeating a
// successful cancellation is idempotent.
func (q *Queue) CancelDelivery(ctx context.Context, topic Topic, subscriber, messageID, reason string) error {
	return q.cancelDeliveryTx(ctx, nil, topic, subscriber, messageID, reason)
}

// CancelDeliveryTx is CancelDelivery within a caller-owned transaction.
func (q *Queue) CancelDeliveryTx(ctx context.Context, tx *sql.Tx, topic Topic, subscriber, messageID, reason string) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	return q.cancelDeliveryTx(ctx, tx, topic, subscriber, messageID, reason)
}

// CancelClaimedDelivery terminally cancels only the delivery lease identified
// by receipt. A stale worker cannot cancel a newer redelivery.
func (q *Queue) CancelClaimedDelivery(
	ctx context.Context,
	topic Topic,
	subscriber, messageID, receipt, reason string,
) error {
	return q.cancelClaimedDeliveryTx(ctx, nil, topic, subscriber, messageID, receipt, reason)
}

// CancelClaimedDeliveryTx is CancelClaimedDelivery within a caller-owned
// transaction.
func (q *Queue) CancelClaimedDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	topic Topic,
	subscriber, messageID, receipt, reason string,
) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	return q.cancelClaimedDeliveryTx(ctx, tx, topic, subscriber, messageID, receipt, reason)
}

func (q *Queue) cancelClaimedDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	topic Topic,
	subscriber, messageID, receipt, reason string,
) error {
	if err := q.requireTransactionAllowed(tx); err != nil {
		return err
	}
	if err := validateDeliveryIdentity(messageID, receipt); err != nil {
		return err
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
	}
	err = q.db.cancelClaimedDeliveryWithTx(ctx, tx, runtime.id, messageID, receipt, reason)
	if err == nil && tx == nil {
		runtime.notify()
	}
	return err
}

func (q *Queue) cancelDeliveryTx(ctx context.Context, tx *sql.Tx, topic Topic, subscriber, messageID, reason string) error {
	if err := q.requireTransactionAllowed(tx); err != nil {
		return err
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return ErrDeliveryNotFound
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
	}
	_, err = q.db.cancelDeliveryWithTx(ctx, tx, runtime.id, messageID, reason)
	if err == nil && tx == nil {
		runtime.notify()
	}
	return err
}

// CancelMessage cancels every pending or delivered subscriber delivery for a
// canonical message and returns the resulting state per subscriber.
func (q *Queue) CancelMessage(ctx context.Context, topic Topic, messageID, reason string) ([]DeliveryResult, error) {
	return q.cancelMessageTx(ctx, nil, topic, messageID, reason)
}

// CancelMessageTx is CancelMessage within a caller-owned transaction.
func (q *Queue) CancelMessageTx(ctx context.Context, tx *sql.Tx, topic Topic, messageID, reason string) ([]DeliveryResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	return q.cancelMessageTx(ctx, tx, topic, messageID, reason)
}

func (q *Queue) cancelMessageTx(ctx context.Context, tx *sql.Tx, topic Topic, messageID, reason string) ([]DeliveryResult, error) {
	if err := q.requireTransactionAllowed(tx); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return nil, ErrDeliveryNotFound
	}
	runtime, exists := q.getTopicRuntime(topic)
	if !exists || runtime.deleted.Load() {
		return nil, ErrTopicNotFound
	}
	rows, err := q.db.cancelMessageWithTx(ctx, tx, runtime.id, messageID, reason)
	if err != nil {
		return nil, err
	}
	results := make([]DeliveryResult, len(rows))
	for index, row := range rows {
		results[index] = DeliveryResult{
			MessageID: row.MessageID, SubscriberID: row.SubscriberID, Status: row.Status,
		}
	}
	if tx == nil {
		runtime.notify()
	}
	return results, nil
}

// DeliveryErrors returns append-only NACK and lease-expiry history newest
// first. The cursor is opaque and scoped to the selected delivery.
func (q *Queue) DeliveryErrors(
	ctx context.Context,
	topic Topic,
	subscriber, messageID string,
	limit int,
	cursor string,
) (DeliveryErrorPage, error) {
	if err := q.requireRunning(); err != nil {
		return DeliveryErrorPage{}, err
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return DeliveryErrorPage{}, ErrDeliveryNotFound
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return DeliveryErrorPage{}, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := q.db.listDeliveryErrors(ctx, runtime.id, messageID, limit+1, cursor)
	if err != nil {
		return DeliveryErrorPage{}, err
	}
	page := DeliveryErrorPage{Errors: rows}
	if len(rows) > limit {
		page.Errors = rows[:limit]
		last := rows[limit-1]
		page.NextCursor = encodeDeliveryCursor(last.FailedAt, last.ID)
	}
	return page, nil
}

// GetMessageStatus returns a canonical message and all of its delivery states.
func (q *Queue) GetMessageStatus(ctx context.Context, topic Topic, messageID string) (MessageStatus, error) {
	if err := q.requireRunning(); err != nil {
		return MessageStatus{}, err
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return MessageStatus{}, ErrDeliveryNotFound
	}
	runtime, exists := q.getTopicRuntime(topic)
	if !exists || runtime.deleted.Load() {
		return MessageStatus{}, ErrTopicNotFound
	}
	return q.db.getMessageStatus(ctx, runtime.id, messageID)
}

// AckDeliveryTx atomically acknowledges a lease in a caller-owned transaction.
func (q *Queue) AckDeliveryTx(ctx context.Context, tx *sql.Tx, topic Topic, subscriber, messageID, receipt string) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	if err := q.requireTransactionAllowed(tx); err != nil {
		return err
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
	}
	if err := validateDeliveryIdentity(messageID, receipt); err != nil {
		return err
	}
	return q.db.ackDeliveryWithTx(ctx, tx, runtime.id, messageID, receipt)
}

// NackDeliveryTx atomically records a failed lease in a caller-owned
// transaction. A zero retryDelay selects the subscriber retry policy.
func (q *Queue) NackDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	topic Topic,
	subscriber, messageID, receipt string,
	retryDelay time.Duration,
	errorText string,
) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	if err := q.requireTransactionAllowed(tx); err != nil {
		return err
	}
	if retryDelay < 0 {
		return fmt.Errorf("%w: retry delay cannot be negative", ErrInvalidPublish)
	}
	_, runtime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
	}
	if err := validateDeliveryIdentity(messageID, receipt); err != nil {
		return err
	}
	_, err = q.db.nackDeliveryWithTx(ctx, tx, runtime.id, messageID, receipt, retryDelay, errorText)
	return err
}

func validateDeliveryIdentity(messageID, receipt string) error {
	if receipt == "" {
		return ErrInvalidReceipt
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return ErrDeliveryNotFound
	}
	if _, err := uuid.Parse(receipt); err != nil {
		return ErrLeaseLost
	}
	return nil
}
