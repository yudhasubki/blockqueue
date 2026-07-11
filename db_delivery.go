package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type deliveryRow struct {
	MessageID      string         `db:"message_id"`
	Message        string         `db:"message"`
	Headers        string         `db:"headers"`
	CorrelationID  sql.NullString `db:"correlation_id"`
	Priority       int            `db:"priority"`
	Status         string         `db:"status"`
	DeliveryCount  int            `db:"delivery_count"`
	FailureCount   int            `db:"failure_count"`
	VisibleAt      time.Time      `db:"visible_at"`
	ReceiptToken   sql.NullString `db:"receipt_token"`
	LeaseExpiresAt sql.NullTime   `db:"lease_expires_at"`
	CreatedAt      time.Time      `db:"created_at"`
}

func (d *db) nowTx(ctx context.Context, tx *sqlx.Tx) (time.Time, error) {
	query := d.dialect.currentTimeQuery()
	var raw any
	if err := tx.QueryRowxContext(ctx, query).Scan(&raw); err != nil {
		return time.Time{}, err
	}
	now, exists, err := databaseTime(raw)
	if err != nil {
		return time.Time{}, err
	}
	if !exists {
		return time.Time{}, errors.New("database returned no current time")
	}
	return now, nil
}

// claimDeliveries uses two set-based statements inside one transaction: a
// locking candidate read and a single UPDATE ... RETURNING. PostgreSQL workers
// coordinate with SKIP LOCKED; SQLite transactions are BEGIN IMMEDIATE through
// the driver DSN.
func (d *db) claimDeliveries(ctx context.Context, subscriberID uuid.UUID, limit int, lease time.Duration) ([]deliveryRow, error) {
	claimed := make([]deliveryRow, 0, limit)
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := d.requeueExpiredDeliveriesTx(ctx, tx, &subscriberID, 1000, now); err != nil {
			return err
		}
		query := `
			SELECT deliveries.message_id, messages.message, messages.headers,
			       messages.correlation_id, deliveries.priority, deliveries.status,
			       deliveries.delivery_count, deliveries.failure_count,
			       deliveries.visible_at, deliveries.receipt_token,
			       deliveries.lease_expires_at, messages.created_at
			FROM message_deliveries deliveries
			JOIN messages ON messages.id = deliveries.message_id
			JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
			JOIN topics ON topics.id = messages.topic_id
			WHERE deliveries.subscriber_id = ? AND deliveries.status = 'pending'
			  AND deliveries.visible_at <= ?
			  AND subscribers.deleted_at IS NULL AND subscribers.paused = ` + boolLiteral(d, false) + `
			  AND topics.deleted_at IS NULL AND topics.paused = ` + boolLiteral(d, false) + `
			ORDER BY deliveries.priority DESC, deliveries.visible_at ASC,
			         deliveries.message_created_at ASC, deliveries.message_id ASC
			LIMIT ?`
		query += d.dialect.lockClause("deliveries", true)
		candidates := make([]deliveryRow, 0, limit)
		if err := tx.SelectContext(ctx, &candidates, tx.Rebind(query), subscriberID, now, limit); err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}

		leaseExpires := now.Add(lease)
		tokens := make(map[string]string, len(candidates))
		updated := make(map[string]struct{}, len(candidates))
		chunkSize := d.dialect.claimChunkSize(len(candidates))
		for start := 0; start < len(candidates); start += chunkSize {
			end := min(start+chunkSize, len(candidates))
			chunk := candidates[start:end]
			var statement strings.Builder
			statement.WriteString("WITH claimed(message_id, receipt_token) AS (VALUES ")
			args := make([]any, 0, len(chunk)*2+3)
			for index, candidate := range chunk {
				if index > 0 {
					statement.WriteByte(',')
				}
				statement.WriteString(d.dialect.deliveryIdentityRow())
				token := uuid.NewString()
				tokens[candidate.MessageID] = token
				args = append(args, candidate.MessageID, token)
			}
			statement.WriteString(`)
				UPDATE message_deliveries
				SET status = 'delivered', delivery_count = delivery_count + 1,
				    receipt_token = (SELECT receipt_token FROM claimed
				        WHERE claimed.message_id = message_deliveries.message_id),
				    lease_expires_at = ?, delivered_at = ?, processed_at = NULL, last_error = NULL
				WHERE subscriber_id = ? AND status = 'pending'
				  AND message_id IN (SELECT message_id FROM claimed)
				RETURNING message_id`)
			args = append(args, leaseExpires, now, subscriberID)
			updatedIDs := make([]string, 0, len(chunk))
			if err := tx.SelectContext(ctx, &updatedIDs, tx.Rebind(statement.String()), args...); err != nil {
				return err
			}
			for _, messageID := range updatedIDs {
				updated[messageID] = struct{}{}
			}
		}
		for _, candidate := range candidates {
			if _, ok := updated[candidate.MessageID]; !ok {
				continue
			}
			candidate.Status = "delivered"
			candidate.DeliveryCount++
			candidate.ReceiptToken = sql.NullString{String: tokens[candidate.MessageID], Valid: true}
			candidate.LeaseExpiresAt = sql.NullTime{Time: leaseExpires, Valid: true}
			claimed = append(claimed, candidate)
		}
		return nil
	})
	return claimed, err
}

type expiredDelivery struct {
	MessageID          string  `db:"message_id"`
	SubscriberID       string  `db:"subscriber_id"`
	FailureCount       int     `db:"failure_count"`
	MaxAttempts        int     `db:"max_attempts"`
	RetryInitialMillis int64   `db:"retry_initial_delay_ms"`
	RetryMaxMillis     int64   `db:"retry_max_delay_ms"`
	RetryMultiplier    float64 `db:"retry_multiplier"`
	RetryJitter        float64 `db:"retry_jitter"`
}

func (d *db) requeueExpiredDeliveriesTx(
	ctx context.Context,
	tx *sqlx.Tx,
	subscriberID *uuid.UUID,
	limit int,
	now time.Time,
) (int64, error) {
	query := `
		SELECT deliveries.message_id, deliveries.subscriber_id, deliveries.failure_count,
		       subscribers.max_attempts, subscribers.retry_initial_delay_ms,
		       subscribers.retry_max_delay_ms, subscribers.retry_multiplier, subscribers.retry_jitter
		FROM message_deliveries deliveries
		JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
		WHERE deliveries.status = 'delivered' AND deliveries.lease_expires_at <= ?`
	args := []any{now}
	if subscriberID != nil {
		query += " AND deliveries.subscriber_id = ?"
		args = append(args, *subscriberID)
	}
	query += " ORDER BY deliveries.lease_expires_at, deliveries.message_id, deliveries.subscriber_id LIMIT ?"
	args = append(args, limit)
	query += d.dialect.lockClause("deliveries", true)
	rows := make([]expiredDelivery, 0, limit)
	if err := tx.SelectContext(ctx, &rows, d.Conn().Rebind(query), args...); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	terminalMessageIDs := make([]string, 0)
	terminalSeen := make(map[string]struct{})
	for start := 0; start < len(rows); start += 100 {
		end := min(start+100, len(rows))
		chunk := rows[start:end]
		var update strings.Builder
		update.WriteString("WITH failed(message_id, subscriber_id, failure_count, next_status, visible_at, processed_at) AS (VALUES ")
		updateArgs := make([]any, 0, len(chunk)*6+1)
		for index, row := range chunk {
			if index > 0 {
				update.WriteByte(',')
			}
			failureCount := row.FailureCount + 1
			status := "pending"
			visibleAt := now.Add(retryDelayFor(subscriberOptions{
				RetryInitialDelay: time.Duration(row.RetryInitialMillis) * time.Millisecond,
				RetryMaxDelay:     time.Duration(row.RetryMaxMillis) * time.Millisecond,
				RetryMultiplier:   row.RetryMultiplier,
				RetryJitter:       row.RetryJitter,
			}, failureCount, row.MessageID))
			var processedAt any
			if failureCount >= row.MaxAttempts {
				status = "dead_letter"
				visibleAt = now
				processedAt = now
				if _, exists := terminalSeen[row.MessageID]; !exists {
					terminalSeen[row.MessageID] = struct{}{}
					terminalMessageIDs = append(terminalMessageIDs, row.MessageID)
				}
			}
			update.WriteString(d.dialect.deliveryFailureRow())
			updateArgs = append(updateArgs, row.MessageID, row.SubscriberID, failureCount, status, visibleAt, processedAt)
		}
		update.WriteString(`)
			UPDATE message_deliveries
			SET failure_count = (SELECT failure_count FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    status = (SELECT next_status FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    visible_at = (SELECT visible_at FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    receipt_token = NULL, lease_expires_at = NULL,
			    processed_at = (SELECT processed_at FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    last_error = ?
			WHERE status = 'delivered' AND EXISTS (
			    SELECT 1 FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id
			)`)
		updateArgs = append(updateArgs, leaseExpiredError)
		if _, err := tx.ExecContext(ctx, d.Conn().Rebind(update.String()), updateArgs...); err != nil {
			return 0, err
		}
	}
	if err := d.insertDeliveryErrorsTx(ctx, tx, rows, leaseExpiredError, now); err != nil {
		return 0, err
	}
	if err := d.completeScheduleRunsForMessagesTx(ctx, tx, terminalMessageIDs); err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

func (d *db) insertDeliveryErrorsTx(ctx context.Context, tx *sqlx.Tx, rows []expiredDelivery, errorText string, failedAt time.Time) error {
	for start := 0; start < len(rows); start += 150 {
		end := min(start+150, len(rows))
		var query strings.Builder
		query.WriteString("INSERT INTO delivery_errors (id, message_id, subscriber_id, failure_count, error, failed_at) VALUES ")
		args := make([]any, 0, (end-start)*6)
		for index, row := range rows[start:end] {
			if index > 0 {
				query.WriteByte(',')
			}
			query.WriteString("(?, ?, ?, ?, ?, ?)")
			args = append(args, uuid.NewString(), row.MessageID, row.SubscriberID, row.FailureCount+1, errorText, failedAt)
		}
		if _, err := tx.ExecContext(ctx, d.Conn().Rebind(query.String()), args...); err != nil {
			return err
		}
	}
	return nil
}

// reapExpiredDeliveries advances leases even when no consumer performs another
// claim. Run completion happens in the same transaction.
func (d *db) reapExpiredDeliveries(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	var affected int64
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		affected, err = d.requeueExpiredDeliveriesTx(ctx, tx, nil, limit, now)
		return err
	})
	return affected, err
}

func (d *db) nextLeaseExpiry(ctx context.Context) (time.Time, bool, error) {
	var value any
	if err := d.Conn().QueryRowxContext(ctx,
		"SELECT MIN(lease_expires_at) FROM message_deliveries WHERE status = 'delivered'").Scan(&value); err != nil {
		return time.Time{}, false, err
	}
	return databaseTime(value)
}

func (d *db) ackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string) error {
	return d.ackDeliveryWithTx(ctx, nil, subscriberID, messageID, receipt)
}

func (d *db) ackDeliveryWithTx(ctx context.Context, external *sql.Tx, subscriberID uuid.UUID, messageID, receipt string) error {
	return d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, d.Conn().Rebind(`
			UPDATE message_deliveries
			SET status = 'processed', processed_at = ?, lease_expires_at = NULL
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`),
			now, messageID, subscriberID, receipt, now)
		if err != nil {
			return err
		}
		updated, _ := result.RowsAffected()
		if updated > 0 {
			return d.completeScheduleRunsTx(ctx, tx, messageID)
		}
		var state struct {
			Status  string         `db:"status"`
			Receipt sql.NullString `db:"receipt_token"`
		}
		err = tx.GetContext(ctx, &state, d.Conn().Rebind(
			"SELECT status, receipt_token FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"),
			messageID, subscriberID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryNotFound
		}
		if err != nil {
			return err
		}
		if state.Status == "processed" && state.Receipt.String == receipt {
			return nil
		}
		return ErrLeaseLost
	})
}

func (d *db) batchAckDeliveries(ctx context.Context, subscriberID uuid.UUID, requests []BatchAckItem) ([]error, error) {
	results := make([]error, len(requests))
	if len(requests) == 0 {
		return results, nil
	}
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		valid := make([]BatchAckItem, 0, len(requests))
		for index, request := range requests {
			if request.ReceiptToken == "" {
				results[index] = ErrInvalidReceipt
				continue
			}
			if _, err := uuid.Parse(request.MessageID); err != nil {
				results[index] = ErrDeliveryNotFound
				continue
			}
			if _, err := uuid.Parse(request.ReceiptToken); err != nil {
				results[index] = ErrLeaseLost
				continue
			}
			valid = append(valid, request)
		}
		const updateChunk = 300 // 603 binds, below SQLite's conservative 999 limit.
		for start := 0; start < len(valid); start += updateChunk {
			end := min(start+updateChunk, len(valid))
			chunk := valid[start:end]
			var query strings.Builder
			query.WriteString("WITH requested(message_id, receipt_token) AS (VALUES ")
			args := make([]any, 0, len(chunk)*2+3)
			for index, request := range chunk {
				if index > 0 {
					query.WriteByte(',')
				}
				query.WriteString(d.dialect.deliveryIdentityRow())
				args = append(args, request.MessageID, request.ReceiptToken)
			}
			query.WriteString(`)
				UPDATE message_deliveries
				SET status = 'processed', processed_at = ?, lease_expires_at = NULL
				WHERE subscriber_id = ? AND status = 'delivered' AND lease_expires_at > ?
				  AND EXISTS (
					SELECT 1 FROM requested
					WHERE requested.message_id = message_deliveries.message_id
					  AND requested.receipt_token = message_deliveries.receipt_token
				  )`)
			args = append(args, now, subscriberID, now)
			if _, err := tx.ExecContext(ctx, tx.Rebind(query.String()), args...); err != nil {
				return err
			}
		}

		type ackState struct {
			MessageID    string         `db:"message_id"`
			Status       string         `db:"status"`
			ReceiptToken sql.NullString `db:"receipt_token"`
		}
		states := make(map[string]ackState, len(valid))
		messageIDs := make([]string, 0, len(valid))
		seen := make(map[string]struct{}, len(valid))
		for _, request := range valid {
			if _, exists := seen[request.MessageID]; exists {
				continue
			}
			seen[request.MessageID] = struct{}{}
			messageIDs = append(messageIDs, request.MessageID)
		}
		for start := 0; start < len(messageIDs); start += 500 {
			end := min(start+500, len(messageIDs))
			query, args, err := sqlx.In(`
				SELECT message_id, status, receipt_token FROM message_deliveries
				WHERE subscriber_id = ? AND message_id IN (?)`, subscriberID, messageIDs[start:end])
			if err != nil {
				return err
			}
			rows := make([]ackState, 0, end-start)
			if err := tx.SelectContext(ctx, &rows, tx.Rebind(query), args...); err != nil {
				return err
			}
			for _, state := range rows {
				states[state.MessageID] = state
			}
		}

		completed := make([]string, 0, len(valid))
		completedSeen := make(map[string]struct{}, len(valid))
		for index, request := range requests {
			if results[index] != nil {
				continue
			}
			state, exists := states[request.MessageID]
			if !exists {
				results[index] = ErrDeliveryNotFound
				continue
			}
			if state.Status != "processed" || state.ReceiptToken.String != request.ReceiptToken {
				results[index] = ErrLeaseLost
				continue
			}
			if _, exists := completedSeen[request.MessageID]; !exists {
				completedSeen[request.MessageID] = struct{}{}
				completed = append(completed, request.MessageID)
			}
		}
		return d.completeScheduleRunsForMessagesTx(ctx, tx, completed)
	})
	return results, err
}

func (d *db) nackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	return d.nackDeliveryWithTx(ctx, nil, subscriberID, messageID, receipt, delay, errorText)
}

func (d *db) nackDeliveryWithTx(ctx context.Context, external *sql.Tx, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	terminal := false
	err := d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		terminal, err = d.nackDeliveryTx(ctx, tx, subscriberID, messageID, receipt, delay, errorText, now)
		if err != nil {
			return err
		}
		if terminal {
			return d.completeScheduleRunsTx(ctx, tx, messageID)
		}
		return nil
	})
	return terminal, err
}

func (d *db) nackDeliveryTx(
	ctx context.Context,
	tx *sqlx.Tx,
	subscriberID uuid.UUID,
	messageID, receipt string,
	delay time.Duration,
	errorText string,
	now time.Time,
) (bool, error) {
	var state expiredDelivery
	query := `
		SELECT deliveries.message_id, deliveries.subscriber_id, deliveries.failure_count,
		       subscribers.max_attempts, subscribers.retry_initial_delay_ms,
		       subscribers.retry_max_delay_ms, subscribers.retry_multiplier, subscribers.retry_jitter
		FROM message_deliveries deliveries
		JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
		WHERE deliveries.message_id = ? AND deliveries.subscriber_id = ?
		  AND deliveries.status = 'delivered' AND deliveries.receipt_token = ?
		  AND deliveries.lease_expires_at > ?`
	query += d.dialect.lockClause("deliveries", false)
	err := tx.GetContext(ctx, &state, d.Conn().Rebind(query), messageID, subscriberID, receipt, now)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if countErr := tx.GetContext(ctx, &count, d.Conn().Rebind(
			"SELECT COUNT(*) FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"),
			messageID, subscriberID); countErr != nil {
			return false, countErr
		}
		if count == 0 {
			return false, ErrDeliveryNotFound
		}
		return false, ErrLeaseLost
	}
	if err != nil {
		return false, err
	}

	failureCount := state.FailureCount + 1
	terminal := failureCount >= state.MaxAttempts
	if delay <= 0 {
		delay = retryDelayFor(subscriberOptions{
			RetryInitialDelay: time.Duration(state.RetryInitialMillis) * time.Millisecond,
			RetryMaxDelay:     time.Duration(state.RetryMaxMillis) * time.Millisecond,
			RetryMultiplier:   state.RetryMultiplier,
			RetryJitter:       state.RetryJitter,
		}, failureCount, messageID)
	}
	status := "pending"
	visibleAt := now.Add(delay)
	var processedAt any
	if terminal {
		status = "dead_letter"
		visibleAt = now
		processedAt = now
	}
	storedError := errorText
	if storedError == "" {
		storedError = "delivery negatively acknowledged"
	}
	result, err := tx.ExecContext(ctx, d.Conn().Rebind(`
		UPDATE message_deliveries
		SET status = ?, failure_count = ?, visible_at = ?, receipt_token = NULL,
		    lease_expires_at = NULL, processed_at = ?, last_error = ?
		WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
		  AND receipt_token = ? AND lease_expires_at > ?`),
		status, failureCount, visibleAt, processedAt, storedError,
		messageID, subscriberID, receipt, now)
	if err != nil {
		return false, err
	}
	updated, _ := result.RowsAffected()
	if updated == 0 {
		return false, ErrLeaseLost
	}
	if err := d.insertDeliveryErrorsTx(ctx, tx, []expiredDelivery{state}, storedError, now); err != nil {
		return false, err
	}
	return terminal, nil
}

func (d *db) batchNackDeliveries(
	ctx context.Context,
	subscriberID uuid.UUID,
	requests []BatchNackItem,
) ([]bool, []error, error) {
	terminal := make([]bool, len(requests))
	results := make([]error, len(requests))
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		completed := make([]string, 0)
		for index, request := range requests {
			if request.ReceiptToken == "" {
				results[index] = ErrInvalidReceipt
				continue
			}
			if _, err := uuid.Parse(request.MessageID); err != nil {
				results[index] = ErrDeliveryNotFound
				continue
			}
			if _, err := uuid.Parse(request.ReceiptToken); err != nil {
				results[index] = ErrLeaseLost
				continue
			}
			if request.RetryDelay < 0 {
				results[index] = fmt.Errorf("%w: retry delay cannot be negative", ErrInvalidPublish)
				continue
			}
			isTerminal, itemErr := d.nackDeliveryTx(
				ctx, tx, subscriberID, request.MessageID, request.ReceiptToken,
				request.RetryDelay, request.Error, now,
			)
			if itemErr != nil {
				if errors.Is(itemErr, ErrDeliveryNotFound) || errors.Is(itemErr, ErrLeaseLost) {
					results[index] = itemErr
					continue
				}
				return itemErr
			}
			terminal[index] = isTerminal
			if isTerminal {
				completed = append(completed, request.MessageID)
			}
		}
		return d.completeScheduleRunsForMessagesTx(ctx, tx, completed)
	})
	return terminal, results, err
}

func (d *db) extendDeliveryLease(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, extension time.Duration) (time.Time, error) {
	var expires time.Time
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		var current time.Time
		query := `SELECT lease_expires_at FROM message_deliveries
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`
		query += d.dialect.lockClause("", false)
		if err := tx.GetContext(ctx, &current, tx.Rebind(query), messageID, subscriberID, receipt, now); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrLeaseLost
			}
			return err
		}
		expires = current.Add(extension)
		maximumExpiry := now.Add(maximumDeliveryLease)
		if expires.After(maximumExpiry) {
			expires = maximumExpiry
		}
		result, err := tx.ExecContext(ctx, tx.Rebind(`
			UPDATE message_deliveries SET lease_expires_at = ?
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`),
			expires, messageID, subscriberID, receipt, now)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return ErrLeaseLost
		}
		return nil
	})
	return expires, err
}

func (d *db) completeScheduleRunsTx(ctx context.Context, tx *sqlx.Tx, messageID string) error {
	_, err := tx.ExecContext(ctx, d.Conn().Rebind(`
		UPDATE schedule_runs
		SET status = 'completed', finished_at = CURRENT_TIMESTAMP
		WHERE status = 'running' AND message_id = ?
		  AND NOT EXISTS (
			SELECT 1 FROM message_deliveries
			WHERE message_deliveries.message_id = schedule_runs.message_id
			  AND message_deliveries.status NOT IN ('processed', 'dead_letter', 'cancelled')
		  )`), messageID)
	return err
}

func (d *db) completeScheduleRunsForMessagesTx(ctx context.Context, tx *sqlx.Tx, messageIDs []string) error {
	for start := 0; start < len(messageIDs); start += 500 {
		end := min(start+500, len(messageIDs))
		query, args, err := sqlx.In(`
			UPDATE schedule_runs
			SET status = 'completed', finished_at = CURRENT_TIMESTAMP
			WHERE status = 'running' AND message_id IN (?)
			  AND NOT EXISTS (
				SELECT 1 FROM message_deliveries
				WHERE message_deliveries.message_id = schedule_runs.message_id
				  AND message_deliveries.status NOT IN ('processed', 'dead_letter', 'cancelled')
			  )`, messageIDs[start:end])
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, d.Conn().Rebind(query), args...); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) nextDeliveryWake(ctx context.Context, subscriberID uuid.UUID) (time.Time, bool, error) {
	var value any
	err := d.Conn().QueryRowxContext(ctx, d.Conn().Rebind(`
		SELECT MIN(wake_at) FROM (
			SELECT visible_at AS wake_at FROM message_deliveries
			WHERE subscriber_id = ? AND status = 'pending'
			UNION ALL
			SELECT lease_expires_at AS wake_at FROM message_deliveries
			WHERE subscriber_id = ? AND status = 'delivered'
		) wakeups`), subscriberID, subscriberID).Scan(&value)
	if err != nil {
		return time.Time{}, false, err
	}
	return databaseTime(value)
}

func (d *db) listDeliveries(ctx context.Context, subscriberID uuid.UUID, deadLetter bool, limit int, cursor string) ([]deliveryRow, error) {
	statuses := "('pending', 'delivered')"
	if deadLetter {
		statuses = "('dead_letter')"
	}
	query := `
		SELECT deliveries.message_id, messages.message, messages.headers,
		       messages.correlation_id, deliveries.priority, deliveries.status,
		       deliveries.delivery_count, deliveries.failure_count,
		       deliveries.visible_at, deliveries.receipt_token,
		       deliveries.lease_expires_at, deliveries.message_created_at AS created_at
		FROM message_deliveries deliveries
		JOIN messages ON messages.id = deliveries.message_id
		WHERE deliveries.subscriber_id = ? AND deliveries.status IN ` + statuses
	args := []any{subscriberID}
	if cursor != "" {
		createdAt, messageID, err := decodeDeliveryCursor(cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidPublish)
		}
		query += " AND (deliveries.message_created_at < ? OR (deliveries.message_created_at = ? AND deliveries.message_id < ?))"
		args = append(args, createdAt, createdAt, messageID)
	}
	query += " ORDER BY deliveries.message_created_at DESC, deliveries.message_id DESC LIMIT ?"
	args = append(args, limit)
	rows := make([]deliveryRow, 0, limit)
	err := d.Conn().SelectContext(ctx, &rows, d.Conn().Rebind(query), args...)
	return rows, err
}

func (d *db) replayDeadLetter(ctx context.Context, subscriberID uuid.UUID, messageID string) (bool, error) {
	var updated bool
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, tx.Rebind(`
			UPDATE message_deliveries
			SET status = 'pending', delivery_count = 0, failure_count = 0,
			    visible_at = ?, receipt_token = NULL,
			    lease_expires_at = NULL, processed_at = NULL, last_error = NULL
			WHERE subscriber_id = ? AND message_id = ? AND status = 'dead_letter'`),
			now, subscriberID, messageID)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		updated = rows > 0
		return nil
	})
	return updated, err
}
