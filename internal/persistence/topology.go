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
	topologyCleanupBatchSize = 1000
	topologyCleanupYield     = 10 * time.Millisecond

	softDeleteTopicQuery            = "UPDATE topics SET deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL"
	softDeleteTopicSubscribersQuery = "UPDATE topic_subscribers SET deleted_at = CURRENT_TIMESTAMP " +
		"WHERE topic_id = ? AND deleted_at IS NULL"
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

func (d *db) listTopics(ctx context.Context, limit int, afterName, afterID string) (Topics, error) {
	query := "SELECT id, name, paused, created_at, deleted_at FROM topics WHERE deleted_at IS NULL"
	args := make([]any, 0, 4)
	if afterID != "" {
		query += " AND (name > ? OR (name = ? AND id > ?))"
		args = append(args, afterName, afterName, afterID)
	}
	query += " ORDER BY name, id LIMIT ?"
	args = append(args, limit)
	items := make(Topics, 0, limit)
	err := d.Conn().SelectContext(ctx, &items, d.Conn().Rebind(query), args...)
	return items, err
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

// deleteSubscriber commits only the logical deletion. Physical delivery and
// metadata cleanup is performed in bounded maintenance transactions so a
// large subscriber cannot monopolize SQLite's single writer.
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
		return d.notifyTx(ctx, tx, EventTopology)
	})
}

// hasDeletedTopology keeps startup recovery cheap for the common case. A
// maintenance pass is only queued when a previous process left durable
// tombstones behind; fresh databases do not pay a competing cleanup cycle
// immediately after Run returns.
func (d *db) hasDeletedTopology(ctx context.Context) (bool, error) {
	var exists bool
	err := d.Conn().GetContext(ctx, &exists, `
		SELECT EXISTS (
			SELECT 1 FROM topics WHERE deleted_at IS NOT NULL
			UNION ALL
			SELECT 1 FROM topic_subscribers WHERE deleted_at IS NOT NULL
		)`)
	return exists, err
}

// pruneDeletedTopology physically removes logically deleted queue topology in
// dependency order. Every statement is independently committed and bounded;
// the shared deadline prevents one maintenance cycle from monopolizing the
// writer when a deleted topic owns millions of rows.
func (d *db) pruneDeletedTopology(ctx context.Context, budget time.Duration) (int64, bool, bool, error) {
	if budget <= 0 {
		return 0, false, false, nil
	}
	deadline := time.Now().Add(budget)
	type cleanupPhase struct {
		query string
		args  []any
	}
	limitArgs := func() []any { return []any{topologyCleanupBatchSize} }
	phases := []cleanupPhase{
		{query: `DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT deliveries.message_id, deliveries.subscriber_id
				FROM message_deliveries deliveries
				JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
				WHERE subscribers.deleted_at IS NOT NULL
				LIMIT ?
			)`,
			args: limitArgs()},
		{query: `UPDATE schedule_runs
			SET status = ?, finished_at = CURRENT_TIMESTAMP
			WHERE id IN (
				SELECT runs.id FROM schedule_runs runs
				WHERE runs.status = ? AND runs.message_id IS NOT NULL
				  AND NOT EXISTS (
					SELECT 1 FROM message_deliveries deliveries
					JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
					WHERE deliveries.message_id = runs.message_id
					  AND subscribers.deleted_at IS NULL
					  AND deliveries.status NOT IN (?, ?, ?)
				  )
				LIMIT ?
			)`,
			args: []any{
				ScheduleRunStatusCompleted, ScheduleRunStatusRunning,
				DeliveryStatusProcessed, DeliveryStatusDeadLetter, DeliveryStatusCancelled,
				topologyCleanupBatchSize,
			}},
		{query: `DELETE FROM schedule_runs
			WHERE id IN (
				SELECT runs.id FROM schedule_runs runs
				JOIN schedules ON schedules.id = runs.schedule_id
				JOIN topics ON topics.id = schedules.topic_id
				WHERE topics.deleted_at IS NOT NULL
				LIMIT ?
			)`,
			args: limitArgs()},
		{query: `DELETE FROM schedules
			WHERE id IN (
				SELECT schedules.id FROM schedules
				JOIN topics ON topics.id = schedules.topic_id
				WHERE topics.deleted_at IS NOT NULL
				  AND NOT EXISTS (
					SELECT 1 FROM schedule_runs
					WHERE schedule_runs.schedule_id = schedules.id
				  )
				LIMIT ?
			)`,
			args: limitArgs()},
		{query: `DELETE FROM messages
			WHERE id IN (
				SELECT messages.id FROM messages
				JOIN topics ON topics.id = messages.topic_id
				WHERE topics.deleted_at IS NOT NULL
				  AND NOT EXISTS (
					SELECT 1 FROM message_deliveries
					WHERE message_deliveries.message_id = messages.id
				  )
				LIMIT ?
			)`,
			args: limitArgs()},
		{query: `DELETE FROM topic_subscribers
			WHERE id IN (
				SELECT subscribers.id FROM topic_subscribers subscribers
				WHERE subscribers.deleted_at IS NOT NULL
				  AND NOT EXISTS (
					SELECT 1 FROM message_deliveries
					WHERE message_deliveries.subscriber_id = subscribers.id
				  )
				LIMIT ?
			)`,
			args: limitArgs()},
		{query: `DELETE FROM topics
			WHERE id IN (
				SELECT topics.id FROM topics
				WHERE topics.deleted_at IS NOT NULL
				  AND NOT EXISTS (SELECT 1 FROM topic_subscribers WHERE topic_subscribers.topic_id = topics.id)
				  AND NOT EXISTS (SELECT 1 FROM schedules WHERE schedules.topic_id = topics.id)
				  AND NOT EXISTS (SELECT 1 FROM messages WHERE messages.topic_id = topics.id)
				LIMIT ?
			)`,
			args: limitArgs()},
	}

	var total int64
	yielded := false
	for _, phase := range phases {
		for {
			if !time.Now().Before(deadline) {
				return total, yielded, true, nil
			}
			result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(phase.query), phase.args...)
			if err != nil {
				return total, yielded, true, err
			}
			rows, _ := result.RowsAffected()
			total += rows
			if rows < topologyCleanupBatchSize {
				break
			}
			yielded = true
			timer := time.NewTimer(topologyCleanupYield)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return total, yielded, true, ctx.Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return total, yielded, true, err
		}
	}
	return total, yielded, false, nil
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

func (d *db) listSubscriberStatuses(
	ctx context.Context,
	topicID uuid.UUID,
	limit int,
	afterName, afterID string,
) ([]SubscriberStatusRow, error) {
	query := `WITH selected AS (
		SELECT id, name FROM topic_subscribers
		WHERE topic_id = ? AND deleted_at IS NULL`
	args := []any{topicID}
	if afterID != "" {
		query += " AND (name > ? OR (name = ? AND id > ?))"
		args = append(args, afterName, afterName, afterID)
	}
	query += ` ORDER BY name, id LIMIT ?
	)
	SELECT selected.id, selected.name,
		COALESCE(SUM(CASE WHEN deliveries.status = 'pending' THEN 1 ELSE 0 END), 0) AS pending,
		COALESCE(SUM(CASE WHEN deliveries.status = 'delivered' THEN 1 ELSE 0 END), 0) AS delivered
	FROM selected
	LEFT JOIN message_deliveries deliveries ON deliveries.subscriber_id = selected.id
	GROUP BY selected.id, selected.name
	ORDER BY selected.name, selected.id`
	args = append(args, limit)
	rows := make([]SubscriberStatusRow, 0, limit)
	err := d.Conn().SelectContext(ctx, &rows, d.Conn().Rebind(query), args...)
	return rows, err
}

// deleteTopic commits only logical deletion. Physical rows remain inaccessible
// and are removed later by pruneDeletedTopology in bounded transactions.
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
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, tx.Rebind(statement.query), topicID); err != nil {
				return fmt.Errorf("%s: %w", statement.operation, err)
			}
		}
		return d.notifyTx(ctx, tx, EventTopology)
	})
}
