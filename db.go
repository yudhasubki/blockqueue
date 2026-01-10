package blockqueue

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/pkg/core"
)

var (
	ErrMessageNotFound = errors.New("message not found")
)

type Driver interface {
	Conn() *sqlx.DB
	Close() error
}

type db struct {
	Database Driver
}

func newDb(driver Driver) *db {
	return &db{
		Database: driver,
	}
}

func (d *db) getTopics(ctx context.Context, filter core.FilterTopic) (core.Topics, error) {
	var (
		topics = make(core.Topics, 0)
		query  = "SELECT * FROM topics"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return topics, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return topics, err
	}
	query = d.Database.Conn().Rebind(query)

	err = d.Database.Conn().SelectContext(ctx, &topics, query, args...)
	if err != nil {
		return topics, err
	}

	return topics, nil
}

func (d *db) getSubscribers(ctx context.Context, filter core.FilterSubscriber) (core.Subscribers, error) {
	var (
		subscribers = make(core.Subscribers, 0)
		query       = "SELECT topic_subscribers.*, t.name as topic_name FROM topic_subscribers INNER JOIN topics t ON topic_subscribers.topic_id = t.id"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return subscribers, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return subscribers, err
	}
	query = d.Database.Conn().Rebind(query)

	err = d.Database.Conn().SelectContext(ctx, &subscribers, query, args...)
	if err != nil {
		return subscribers, err
	}

	return subscribers, nil
}

func (d *db) createTxTopic(ctx context.Context, tx *sqlx.Tx, topic core.Topic) error {
	stmt, err := tx.PrepareNamedContext(ctx, "INSERT INTO topics (id, name) VALUES (:id, :name)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) deleteTxTopic(ctx context.Context, tx *sqlx.Tx, topic core.Topic) error {
	stmt, err := tx.PrepareNamedContext(ctx, "UPDATE topics SET deleted_at = :deleted_at WHERE id = :id")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) deleteTxSubscribers(ctx context.Context, tx *sqlx.Tx, topic core.Subscriber) error {
	stmt, err := tx.PrepareNamedContext(ctx, "UPDATE topic_subscribers SET deleted_at = :deleted_at WHERE name = :name AND topic_id = :topic_id")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) createTxSubscribers(ctx context.Context, tx *sqlx.Tx, subscribers core.Subscribers) error {
	_, err := tx.NamedExecContext(ctx, "INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (:id, :topic_id, :name, :option)", subscribers)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) createMessages(ctx context.Context, message core.Message) error {
	stmt, err := d.Database.Conn().PrepareNamedContext(ctx, "INSERT INTO topic_messages (id, topic_id, message, status) VALUES (:id, :topic_id, :message, :status)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, message)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) updateStatusMessage(ctx context.Context, status core.MessageStatus, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	query, args, err := sqlx.In("UPDATE topic_messages SET status = ? WHERE id IN (?)", status, ids)
	if err != nil {
		return err
	}

	_, err = d.Database.Conn().ExecContext(ctx, d.Database.Conn().Rebind(query), args...)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) getMessages(ctx context.Context, filter core.FilterMessage) (core.Messages, error) {
	var (
		messages = make(core.Messages, 0)
		query    = "SELECT * FROM topic_messages"
	)

	clause, arg := filter.Filter("AND")
	if clause != "" {
		query += " WHERE " + clause
	}
	query += " " + filter.Sort() + " " + filter.Page()

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return messages, err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return messages, err
	}
	query = d.Database.Conn().Rebind(query)

	err = d.Database.Conn().SelectContext(ctx, &messages, query, args...)
	if err != nil {
		return messages, err
	}

	return messages, nil
}

func (d *db) tx(ctx context.Context, fn func(ctx context.Context, tx *sqlx.Tx) error) error {
	tx, err := d.Database.Conn().Beginx()
	if err != nil {
		return err
	}

	err = fn(ctx, tx)
	if err != nil {
		if errTx := tx.Rollback(); errTx == nil {
			return err
		}

		return err
	}

	return tx.Commit()
}

// SubscriberMessage represents a message in the subscriber queue
type SubscriberMessage struct {
	Id           int64     `db:"id"`
	SubscriberId uuid.UUID `db:"subscriber_id"`
	TopicId      uuid.UUID `db:"topic_id"`
	MessageId    string    `db:"message_id"`
	Message      string    `db:"message"`
	Status       string    `db:"status"`
	RetryCount   int       `db:"retry_count"`
	VisibleAt    time.Time `db:"visible_at"`
	CreatedAt    time.Time `db:"created_at"`
}

type SubscriberMessages []SubscriberMessage

// SubscriberQueueStats holds queue statistics
type SubscriberQueueStats struct {
	Pending   int
	Delivered int
}

// enqueueToSubscribers inserts messages to all active subscribers for a topic
func (d *db) enqueueToSubscribers(ctx context.Context, topicId uuid.UUID, messageId string, message string) error {
	query := `
		INSERT INTO subscriber_messages (subscriber_id, topic_id, message_id, message, status, visible_at)
		SELECT ts.id, ts.topic_id, ?, ?, 'pending', CURRENT_TIMESTAMP
		FROM topic_subscribers ts
		WHERE ts.topic_id = ? AND ts.deleted_at IS NULL
	`
	_, err := d.Database.Conn().ExecContext(ctx, d.Database.Conn().Rebind(query), messageId, message, topicId)
	return err
}

// dequeueMessages fetches and marks messages as delivered atomically
func (d *db) dequeueMessages(ctx context.Context, subscriberId uuid.UUID, limit int, visibilityDuration time.Duration) (SubscriberMessages, error) {
	messages := make(SubscriberMessages, 0)

	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		// Select pending messages that are visible
		selectQuery := `
			SELECT id, subscriber_id, topic_id, message_id, message, status, retry_count, visible_at, created_at
			FROM subscriber_messages
			WHERE subscriber_id = ? AND status = 'pending' AND visible_at <= CURRENT_TIMESTAMP
			ORDER BY created_at ASC
			LIMIT ?
		`
		err := tx.SelectContext(ctx, &messages, tx.Rebind(selectQuery), subscriberId, limit)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			return nil
		}

		// Collect IDs and update to delivered with visibility timeout
		ids := make([]int64, 0, len(messages))
		for _, m := range messages {
			ids = append(ids, m.Id)
		}

		visibleAt := time.Now().Add(visibilityDuration)
		updateQuery, args, err := sqlx.In(
			"UPDATE subscriber_messages SET status = 'delivered', visible_at = ? WHERE id IN (?)",
			visibleAt, ids,
		)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, tx.Rebind(updateQuery), args...)
		return err
	})

	return messages, err
}

// ackSubscriberMessage deletes a message after successful processing
func (d *db) ackSubscriberMessage(ctx context.Context, subscriberId uuid.UUID, messageId string) error {
	result, err := d.Database.Conn().ExecContext(ctx,
		"DELETE FROM subscriber_messages WHERE subscriber_id = ? AND message_id = ?",
		subscriberId, messageId,
	)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrMessageNotFound
	}
	return nil
}

// getSubscriberQueueStats returns pending and delivered message counts
func (d *db) getSubscriberQueueStats(ctx context.Context, subscriberId uuid.UUID) (SubscriberQueueStats, error) {
	var stats SubscriberQueueStats

	err := d.Database.Conn().GetContext(ctx, &stats.Pending,
		"SELECT COUNT(*) FROM subscriber_messages WHERE subscriber_id = ? AND status = 'pending'",
		subscriberId,
	)
	if err != nil {
		return stats, err
	}

	err = d.Database.Conn().GetContext(ctx, &stats.Delivered,
		"SELECT COUNT(*) FROM subscriber_messages WHERE subscriber_id = ? AND status = 'delivered'",
		subscriberId,
	)
	return stats, err
}

// requeueExpiredMessages moves expired delivered messages back to pending, or deletes if max retries exceeded
func (d *db) requeueExpiredMessages(ctx context.Context, subscriberId uuid.UUID, maxRetries int, visibilityDuration time.Duration) error {
	// Delete messages that exceeded max retries
	_, err := d.Database.Conn().ExecContext(ctx,
		d.Database.Conn().Rebind("DELETE FROM subscriber_messages WHERE subscriber_id = ? AND status = 'delivered' AND visible_at <= CURRENT_TIMESTAMP AND retry_count >= ?"),
		subscriberId, maxRetries,
	)
	if err != nil {
		return err
	}

	// Requeue messages that are still under retry limit
	visibleAt := time.Now().Add(visibilityDuration)
	_, err = d.Database.Conn().ExecContext(ctx,
		d.Database.Conn().Rebind("UPDATE subscriber_messages SET status = 'pending', retry_count = retry_count + 1, visible_at = ? WHERE subscriber_id = ? AND status = 'delivered' AND visible_at <= CURRENT_TIMESTAMP"),
		visibleAt, subscriberId,
	)
	return err
}

// deleteSubscriberMessages deletes all messages for a subscriber
func (d *db) deleteSubscriberMessages(ctx context.Context, subscriberId uuid.UUID) error {
	_, err := d.Database.Conn().ExecContext(ctx,
		d.Database.Conn().Rebind("DELETE FROM subscriber_messages WHERE subscriber_id = ?"),
		subscriberId,
	)
	return err
}

// deleteTopicMessages deletes all messages for a topic
func (d *db) deleteTopicMessages(ctx context.Context, topicId uuid.UUID) error {
	_, err := d.Database.Conn().ExecContext(ctx,
		d.Database.Conn().Rebind("DELETE FROM subscriber_messages WHERE topic_id = ?"),
		topicId,
	)
	return err
}
