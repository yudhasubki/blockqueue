package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

type db struct {
	Database       store.Driver
	connection     *sqlx.DB
	dialect        sqlDialect
	dialectErr     error
	statements     *statementCache
	disableMetrics bool
	closeOnce      sync.Once
	closeErr       error
}

const databaseEventChannel = "blockqueue_events"

func newDb(driver store.Driver) *db {
	dialect, dialectErr := newSQLDialect(driver.Dialect())
	return &db{
		Database: driver, connection: sqlx.NewDb(driver.DB(), driver.DriverName()), dialect: dialect, dialectErr: dialectErr,
		statements: newStatementCache(defaultStatementCacheSize),
	}
}

func (d *db) Conn() *sqlx.DB { return d.connection }

func (d *db) close() error {
	d.closeOnce.Do(func() {
		d.closeErr = errors.Join(d.statements.close(), d.Database.Close())
	})
	return d.closeErr
}

func boolLiteral(d *db, value bool) string {
	return d.dialect.boolLiteral(value)
}

func (d *db) supportsSQLiteMaintenance() bool {
	source, ok := d.Database.(store.SQLiteMaintenanceSource)
	return ok && source.SQLiteMaintenanceEnabled()
}

func databaseTime(value any) (time.Time, bool, error) {
	if value == nil {
		return time.Time{}, false, nil
	}
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), true, nil
	case []byte:
		value = string(typed)
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, false, fmt.Errorf("unsupported database time type %T", value)
	}
	text = strings.TrimSpace(text)
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid database time %q", text)
}

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

func (d *db) getSubscribers(ctx context.Context, filter subscriberFilter) (Subscribers, error) {
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

func (d *db) tx(ctx context.Context, fn func(context.Context, *sqlx.Tx) error) error {
	return d.withTx(ctx, nil, fn)
}

func (d *db) withTx(ctx context.Context, external *sql.Tx, fn func(context.Context, *sqlx.Tx) error) error {
	if external != nil {
		return fn(ctx, &sqlx.Tx{Tx: external, Mapper: d.Conn().Mapper})
	}
	tx, err := d.Conn().BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (d *db) notifyTx(ctx context.Context, tx *sqlx.Tx, payload string) error {
	if !d.dialect.usesDatabaseEvents() {
		return nil
	}
	_, err := tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", databaseEventChannel, payload)
	return err
}

func (d *db) notifyDatabase(ctx context.Context, payload string) error {
	if !d.dialect.usesDatabaseEvents() {
		return nil
	}
	_, err := d.Conn().ExecContext(ctx, "SELECT pg_notify($1, $2)", databaseEventChannel, payload)
	return err
}

type SubscriberQueueStats struct {
	Pending   int `db:"pending"`
	Delivered int `db:"delivered"`
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

// pruneProcessedMessages uses processed_at as the retention clock and works in
// bounded chunks so maintenance cannot monopolize the writer.
func (d *db) pruneProcessedMessages(ctx context.Context, retention time.Duration) error {
	threshold := time.Now().UTC().Add(-retention)
	deadline := time.Now().Add(2 * time.Second)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT message_id, subscriber_id FROM message_deliveries
				WHERE status IN ('processed', 'cancelled') AND processed_at < ?
				ORDER BY processed_at LIMIT 1000
			)`), threshold)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < 1000 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	var orphaned int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, `
			DELETE FROM messages
			WHERE id IN (
				SELECT candidate.id FROM messages candidate
				WHERE NOT EXISTS (
					SELECT 1 FROM message_deliveries
					WHERE message_deliveries.message_id = candidate.id
				)
				ORDER BY candidate.id LIMIT 1000
			)`)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		orphaned += rows
		if rows < 1000 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 || orphaned > 0 {
		slog.Info("pruned processed queue data", "deliveries", total, "messages", orphaned, "threshold", threshold)
	}
	return nil
}

func (d *db) pruneScheduleRuns(ctx context.Context, retention time.Duration) error {
	threshold := time.Now().UTC().Add(-retention)
	deadline := time.Now().Add(2 * time.Second)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM schedule_runs
			WHERE id IN (
				SELECT id FROM schedule_runs
				WHERE status <> 'running' AND COALESCE(finished_at, created_at) < ?
				ORDER BY COALESCE(finished_at, created_at), id LIMIT 1000
			)`), threshold)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < 1000 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 {
		slog.Info("pruned schedule runs", "count", total, "threshold", threshold)
	}
	return nil
}

func (d *db) pruneDeadLetters(ctx context.Context, retention time.Duration) error {
	threshold := time.Now().UTC().Add(-retention)
	deadline := time.Now().Add(2 * time.Second)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT message_id, subscriber_id FROM message_deliveries
				WHERE status = 'dead_letter' AND processed_at < ?
				ORDER BY processed_at LIMIT 1000
			)`), threshold)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < 1000 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 {
		slog.Info("pruned dead letters", "count", total, "threshold", threshold)
	}
	return nil
}
