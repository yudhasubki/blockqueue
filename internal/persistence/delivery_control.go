package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/internal/textlimit"
)

type messageStatusRow struct {
	ID             string         `db:"id"`
	TopicID        string         `db:"topic_id"`
	Message        string         `db:"message"`
	Headers        string         `db:"headers"`
	CorrelationID  sql.NullString `db:"correlation_id"`
	IdempotencyKey sql.NullString `db:"idempotency_key"`
	Priority       int            `db:"priority"`
	ScheduledAt    time.Time      `db:"scheduled_at"`
	CreatedAt      time.Time      `db:"created_at"`
}

type cancellationRow struct {
	MessageID    string `db:"message_id"`
	SubscriberID string `db:"subscriber_id"`
	Status       string `db:"status"`
}

func (d *db) snoozeDeliveryWithTx(
	ctx context.Context,
	external *sql.Tx,
	subscriberID uuid.UUID,
	messageID, receipt string,
	delay time.Duration,
) (time.Time, error) {
	var visibleAt time.Time
	err := d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		visibleAt = now.Add(delay)
		result, err := tx.ExecContext(ctx, d.Conn().Rebind(`
			UPDATE message_deliveries
			SET status = 'pending', visible_at = ?, receipt_token = NULL,
			    lease_expires_at = NULL, processed_at = NULL, last_error = NULL
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`),
			visibleAt, messageID, subscriberID, receipt, now)
		if err != nil {
			return err
		}
		updated, _ := result.RowsAffected()
		if updated > 0 {
			return nil
		}
		return d.deliveryMutationErrorTx(ctx, tx, subscriberID, messageID)
	})
	return visibleAt, err
}

func (d *db) cancelDeliveryWithTx(
	ctx context.Context,
	external *sql.Tx,
	subscriberID uuid.UUID,
	messageID, reason string,
) (string, error) {
	reason = textlimit.UTF8(reason, MaxDeliveryTextBytes)
	status := ""
	err := d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, d.Conn().Rebind(`
			UPDATE message_deliveries
			SET status = 'cancelled', processed_at = ?, cancelled_at = ?, cancel_reason = ?,
			    receipt_token = NULL, lease_expires_at = NULL
			WHERE message_id = ? AND subscriber_id = ? AND status IN ('pending', 'delivered')`),
			now, now, nullString(reason), messageID, subscriberID)
		if err != nil {
			return err
		}
		updated, _ := result.RowsAffected()
		if updated > 0 {
			status = DeliveryStatusCancelled
			return d.completeScheduleRunsTx(ctx, tx, messageID)
		}
		if err := tx.GetContext(ctx, &status, d.Conn().Rebind(
			"SELECT status FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"),
			messageID, subscriberID); errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryNotFound
		} else if err != nil {
			return err
		}
		if status == DeliveryStatusCancelled {
			return nil
		}
		return fmt.Errorf("%w: delivery is %s", ErrDeliveryTerminal, status)
	})
	return status, err
}

func (d *db) cancelClaimedDeliveryWithTx(
	ctx context.Context,
	external *sql.Tx,
	subscriberID uuid.UUID,
	messageID, receipt, reason string,
) error {
	reason = textlimit.UTF8(reason, MaxDeliveryTextBytes)
	return d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, d.Conn().Rebind(`
			UPDATE message_deliveries
			SET status = 'cancelled', processed_at = ?, cancelled_at = ?, cancel_reason = ?,
			    lease_expires_at = NULL
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`),
			now, now, nullString(reason), messageID, subscriberID, receipt, now)
		if err != nil {
			return err
		}
		updated, _ := result.RowsAffected()
		if updated > 0 {
			return d.completeScheduleRunsTx(ctx, tx, messageID)
		}
		var row struct {
			Status       string         `db:"status"`
			ReceiptToken sql.NullString `db:"receipt_token"`
		}
		if err := tx.GetContext(ctx, &row, d.Conn().Rebind(`
			SELECT status, receipt_token FROM message_deliveries
			WHERE message_id = ? AND subscriber_id = ?`),
			messageID, subscriberID); errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryNotFound
		} else if err != nil {
			return err
		}
		if row.Status == DeliveryStatusCancelled && row.ReceiptToken.Valid && row.ReceiptToken.String == receipt {
			return nil
		}
		return ErrLeaseLost
	})
}

func (d *db) cancelMessageWithTx(
	ctx context.Context,
	external *sql.Tx,
	topicID uuid.UUID,
	messageID, reason string,
) ([]cancellationRow, error) {
	reason = textlimit.UTF8(reason, MaxDeliveryTextBytes)
	rows := make([]cancellationRow, 0)
	err := d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		var exists int
		if err := tx.GetContext(ctx, &exists, d.Conn().Rebind(
			"SELECT COUNT(*) FROM messages WHERE id = ? AND topic_id = ?"), messageID, topicID); err != nil {
			return err
		}
		if exists == 0 {
			return ErrDeliveryNotFound
		}
		if _, err := tx.ExecContext(ctx, d.Conn().Rebind(`
			UPDATE message_deliveries
			SET status = 'cancelled', processed_at = ?, cancelled_at = ?, cancel_reason = ?,
			    receipt_token = NULL, lease_expires_at = NULL
			WHERE message_id = ? AND status IN ('pending', 'delivered')`),
			now, now, nullString(reason), messageID); err != nil {
			return err
		}
		if err := tx.SelectContext(ctx, &rows, d.Conn().Rebind(`
			SELECT message_id, subscriber_id, status FROM message_deliveries
			WHERE message_id = ? ORDER BY subscriber_id`), messageID); err != nil {
			return err
		}
		return d.completeScheduleRunsTx(ctx, tx, messageID)
	})
	return rows, err
}

func (d *db) deliveryMutationErrorTx(ctx context.Context, tx *sqlx.Tx, subscriberID uuid.UUID, messageID string) error {
	var status string
	if err := tx.GetContext(ctx, &status, d.Conn().Rebind(
		"SELECT status FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"),
		messageID, subscriberID); errors.Is(err, sql.ErrNoRows) {
		return ErrDeliveryNotFound
	} else if err != nil {
		return err
	}
	return ErrLeaseLost
}

func (d *db) listDeliveryErrors(
	ctx context.Context,
	subscriberID uuid.UUID,
	messageID string,
	limit int,
	cursor string,
) ([]DeliveryError, error) {
	var deliveryExists int
	if err := d.Conn().GetContext(ctx, &deliveryExists, d.Conn().Rebind(
		"SELECT COUNT(*) FROM message_deliveries WHERE subscriber_id = ? AND message_id = ?"),
		subscriberID, messageID); err != nil {
		return nil, err
	}
	if deliveryExists == 0 {
		return nil, ErrDeliveryNotFound
	}
	args := []any{subscriberID, messageID}
	query := `SELECT id, message_id, subscriber_id, failure_count, error, failed_at
		FROM delivery_errors WHERE subscriber_id = ? AND message_id = ?`
	if cursor != "" {
		failedAt, errorID, err := decodeDeliveryCursor(cursor)
		if err != nil {
			return nil, ErrInvalidCursor
		}
		query += " AND (failed_at < ? OR (failed_at = ? AND id < ?))"
		args = append(args, failedAt, failedAt, errorID)
	}
	query += " ORDER BY failed_at DESC, id DESC LIMIT ?"
	args = append(args, limit)
	rows := make([]DeliveryError, 0, limit)
	if err := d.Conn().SelectContext(ctx, &rows, d.Conn().Rebind(query), args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (d *db) getMessageStatus(ctx context.Context, topicID uuid.UUID, messageID string) (MessageStatus, error) {
	var row messageStatusRow
	if err := d.Conn().GetContext(ctx, &row, d.Conn().Rebind(`
		SELECT id, topic_id, message, headers, correlation_id, idempotency_key,
		       priority, scheduled_at, created_at
		FROM messages WHERE id = ? AND topic_id = ?`), messageID, topicID); errors.Is(err, sql.ErrNoRows) {
		return MessageStatus{}, ErrDeliveryNotFound
	} else if err != nil {
		return MessageStatus{}, err
	}
	deliveries := make([]MessageDeliveryStatus, 0)
	if err := d.Conn().SelectContext(ctx, &deliveries, d.Conn().Rebind(`
		SELECT deliveries.subscriber_id, subscribers.name AS subscriber,
		       deliveries.status, deliveries.delivery_count, deliveries.failure_count,
		       deliveries.visible_at, deliveries.processed_at, deliveries.cancelled_at,
		       COALESCE(deliveries.cancel_reason, '') AS cancel_reason
		FROM message_deliveries deliveries
		JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
		WHERE deliveries.message_id = ? ORDER BY subscribers.name, deliveries.subscriber_id`), messageID); err != nil {
		return MessageStatus{}, err
	}
	headers := make(map[string]string)
	if err := json.Unmarshal([]byte(row.Headers), &headers); err != nil {
		return MessageStatus{}, fmt.Errorf("decode message headers: %w", err)
	}
	return MessageStatus{
		ID: row.ID, TopicID: row.TopicID, Message: row.Message, Headers: headers,
		CorrelationID: row.CorrelationID.String, IdempotencyKey: row.IdempotencyKey.String,
		Priority: row.Priority, ScheduledAt: row.ScheduledAt, CreatedAt: row.CreatedAt,
		Deliveries: deliveries,
	}, nil
}
