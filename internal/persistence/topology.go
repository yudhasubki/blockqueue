package persistence

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

func (d *db) getTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	topics := make(Topics, 0)
	query := "SELECT id, name, paused, created_at, deleted_at FROM topics"
	clause, namedArgs := filter.filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}
	query, args, err := sqlx.Named(query, namedArgs)
	if err != nil {
		return nil, err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return nil, err
	}
	err = d.Conn().SelectContext(ctx, &topics, d.Conn().Rebind(query), args...)
	return topics, err
}

func (d *db) getSubscribers(ctx context.Context, filter SubscriberFilter) (Subscribers, error) {
	subscribers := make(Subscribers, 0)
	query := `SELECT topic_subscribers.id, topic_subscribers.topic_id,
		       topics.name AS topic_name, topic_subscribers.name,
		       topic_subscribers.option, topic_subscribers.paused,
		       topic_subscribers.created_at, topic_subscribers.deleted_at
		FROM topic_subscribers JOIN topics ON topics.id = topic_subscribers.topic_id`
	clause, namedArgs := filter.filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}
	query, args, err := sqlx.Named(query, namedArgs)
	if err != nil {
		return nil, err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return nil, err
	}
	err = d.Conn().SelectContext(ctx, &subscribers, d.Conn().Rebind(query), args...)
	return subscribers, err
}

func (d *db) createTxTopic(ctx context.Context, tx *sqlx.Tx, topic Topic) error {
	_, err := tx.NamedExecContext(ctx, "INSERT INTO topics (id, name, paused) VALUES (:id, :name, :paused)", topic)
	return err
}

func (d *db) createTxSubscribers(ctx context.Context, tx *sqlx.Tx, subscribers Subscribers) error {
	for _, subscriber := range subscribers {
		options, err := parseSubscriberOptions(subscriber)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, tx.Rebind(`
			INSERT INTO topic_subscribers
				(id, topic_id, name, option, paused, max_attempts, visibility_timeout_ms, dequeue_batch_size,
				 retry_initial_delay_ms, retry_max_delay_ms, retry_multiplier, retry_jitter)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			subscriber.ID, subscriber.TopicID, subscriber.Name, subscriber.Options, subscriber.Paused,
			options.MaxAttempts, options.VisibilityDuration.Milliseconds(), options.DequeueBatchSize,
			options.RetryInitialDelay.Milliseconds(), options.RetryMaxDelay.Milliseconds(),
			options.RetryMultiplier, options.RetryJitter,
		); err != nil {
			return err
		}
	}
	return nil
}

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
		return d.notifyTx(ctx, tx, EventTopology)
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
		return d.notifyTx(ctx, tx, EventTopology)
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
	_ = d.notifyDatabase(ctx, EventTopology)
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
	_ = d.notifyDatabase(ctx, EventTopology)
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
		return d.notifyTx(ctx, tx, EventTopology)
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

func (d *db) getTopicSubscriberQueueStats(ctx context.Context, topicID uuid.UUID) (map[uuid.UUID]SubscriberQueueStats, error) {
	type row struct {
		SubscriberID uuid.UUID `db:"subscriber_id"`
		Pending      int       `db:"pending"`
		Delivered    int       `db:"delivered"`
	}
	rows := make([]row, 0)
	err := d.Conn().SelectContext(ctx, &rows, d.Conn().Rebind(`
		SELECT deliveries.subscriber_id,
			SUM(CASE WHEN deliveries.status = 'pending' THEN 1 ELSE 0 END) AS pending,
			SUM(CASE WHEN deliveries.status = 'delivered' THEN 1 ELSE 0 END) AS delivered
		FROM message_deliveries deliveries
		JOIN messages ON messages.id = deliveries.message_id
		WHERE messages.topic_id = ? AND deliveries.status IN ('pending', 'delivered')
		GROUP BY deliveries.subscriber_id`), topicID)
	if err != nil {
		return nil, err
	}
	result := make(map[uuid.UUID]SubscriberQueueStats, len(rows))
	for _, item := range rows {
		result[item.SubscriberID] = SubscriberQueueStats{Pending: item.Pending, Delivered: item.Delivered}
	}
	return result, nil
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
		return d.notifyTx(ctx, tx, EventTopology)
	})
}
