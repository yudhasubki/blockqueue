package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const (
	softDeleteTopicQuery            = "UPDATE topics SET deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL"
	softDeleteTopicSubscribersQuery = "UPDATE topic_subscribers SET deleted_at = CURRENT_TIMESTAMP " +
		"WHERE topic_id = ? AND deleted_at IS NULL"
	deleteTopicSchedulesQuery = "DELETE FROM schedules WHERE topic_id = ?"
	deleteTopicMessagesQuery  = "DELETE FROM messages WHERE topic_id = ?"
	softDeleteSubscriberQuery = "UPDATE topic_subscribers SET deleted_at = CURRENT_TIMESTAMP " +
		"WHERE topic_id = ? AND id = ? AND name = ? AND deleted_at IS NULL"
)

// createTopic persists a topic and its initial subscribers atomically. Queue
// orchestration should use this method instead of depending on transaction or
// query details.
func (d *db) createTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		if err := d.createTxTopic(ctx, tx, topic); err != nil {
			return err
		}
		if err := d.createTxSubscribers(ctx, tx, subscribers); err != nil {
			return err
		}
		return d.notifyTx(ctx, tx, "topology")
	})
	return normalizeResourceConflict(err)
}

// createSubscribers keeps the transaction boundary inside the storage layer.
func (d *db) createSubscribers(ctx context.Context, subscribers Subscribers) error {
	if len(subscribers) == 0 {
		return nil
	}
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		topicID := subscribers[0].TopicID
		for _, subscriber := range subscribers[1:] {
			if subscriber.TopicID != topicID {
				return fmt.Errorf("%w: subscribers must belong to one topic", ErrInvalidPublish)
			}
		}
		if err := d.lockTopicTx(ctx, tx, topicID); err != nil {
			return err
		}
		if err := d.createTxSubscribers(ctx, tx, subscribers); err != nil {
			return err
		}
		return d.notifyTx(ctx, tx, "topology")
	})
	return normalizeResourceConflict(err)
}

func (d *db) lockTopicTx(ctx context.Context, tx *sqlx.Tx, topicID uuid.UUID) error {
	query := "SELECT id FROM topics WHERE id = ? AND deleted_at IS NULL"
	query += d.dialect.lockClause("", false)
	var id string
	if err := tx.GetContext(ctx, &id, tx.Rebind(query), topicID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrTopicNotFound
		}
		return err
	}
	return nil
}

func (d *db) setTopicPaused(ctx context.Context, topicID uuid.UUID, paused bool) error {
	result, err := d.Conn().ExecContext(ctx,
		d.Conn().Rebind("UPDATE topics SET paused = ? WHERE id = ? AND deleted_at IS NULL"),
		paused, topicID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrTopicNotFound
	}
	_ = d.notifyDatabase(ctx, "topology")
	return nil
}

func (d *db) setSubscriberPaused(ctx context.Context, subscriberID uuid.UUID, paused bool) error {
	result, err := d.Conn().ExecContext(ctx,
		d.Conn().Rebind("UPDATE topic_subscribers SET paused = ? WHERE id = ? AND deleted_at IS NULL"),
		paused, subscriberID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrSubscriberNotFound
	}
	_ = d.notifyDatabase(ctx, "topology")
	return nil
}

// deleteSubscriber removes canonical delivery state while retaining the
// soft-deleted subscriber metadata for restart consistency.
func (d *db) deleteSubscriber(ctx context.Context, topicID, subscriberID uuid.UUID, subscriberName string) error {
	return d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		if err := d.lockTopicTx(ctx, tx, topicID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, tx.Rebind(softDeleteSubscriberQuery), topicID, subscriberID, subscriberName)
		if err != nil {
			return fmt.Errorf("soft-delete subscriber: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return ErrSubscriberNotFound
		}
		if err := d.deleteSubscriberDeliveriesTx(ctx, tx, subscriberID, 2*time.Second); err != nil {
			return fmt.Errorf("delete subscriber deliveries: %w", err)
		}
		return d.notifyTx(ctx, tx, "topology")
	})
}

func (d *db) deleteSubscriberDeliveriesTx(
	ctx context.Context,
	tx *sqlx.Tx,
	subscriberID uuid.UUID,
	budget time.Duration,
) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		result, err := tx.ExecContext(ctx, tx.Rebind(`
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT message_id, subscriber_id FROM message_deliveries
				WHERE subscriber_id = ? LIMIT 1000
			)`), subscriberID)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows < 1000 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (d *db) pruneDeletedSubscriberDeliveries(ctx context.Context, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, `
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT deliveries.message_id, deliveries.subscriber_id
				FROM message_deliveries deliveries
				JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
				WHERE subscribers.deleted_at IS NOT NULL LIMIT 1000
			)`)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows < 1000 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// deleteTopic removes all queue data owned by a topic in one transaction.
func (d *db) deleteTopic(ctx context.Context, topicID uuid.UUID) error {
	return d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		if err := d.lockTopicTx(ctx, tx, topicID); err != nil {
			return err
		}
		statements := []struct {
			operation string
			query     string
		}{
			{"soft-delete topic", softDeleteTopicQuery},
			{"soft-delete subscribers", softDeleteTopicSubscribersQuery},
			{"delete schedules", deleteTopicSchedulesQuery},
			{"delete canonical messages", deleteTopicMessagesQuery},
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, tx.Rebind(statement.query), topicID); err != nil {
				return fmt.Errorf("%s: %w", statement.operation, err)
			}
		}
		return d.notifyTx(ctx, tx, "topology")
	})
}
