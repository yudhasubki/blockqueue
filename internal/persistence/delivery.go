package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/internal/textlimit"
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
	claimSelectBase, err := d.statements.get(ctx, d.Conn(), cachedClaimSelectQuery(d))
	if err != nil {
		return nil, err
	}
	err = d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := d.requeueExpiredDeliveriesTx(
			ctx, tx, &subscriberID, defaultDeliveryReaperBatchSize, now,
		); err != nil {
			return err
		}
		candidates := make([]deliveryRow, 0, limit)
		claimSelect := tx.StmtxContext(ctx, claimSelectBase)
		if err := claimSelect.SelectContext(ctx, &candidates, subscriberID, now, limit); err != nil {
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
			args := make([]any, 0, len(chunk)*2+3)
			for _, candidate := range chunk {
				token := uuid.NewString()
				tokens[candidate.MessageID] = token
				args = append(args, candidate.MessageID, token)
			}
			args = append(args, leaseExpires, now, subscriberID)
			updatedIDs := make([]string, 0, len(chunk))
			if err := tx.SelectContext(ctx, &updatedIDs, cachedClaimUpdateQuery(d, len(chunk)), args...); err != nil {
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
			candidate.Status = DeliveryStatusDelivered
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
	args := []any{now}
	if subscriberID != nil {
		args = append(args, *subscriberID)
	}
	args = append(args, limit)
	rows := make([]expiredDelivery, 0, limit)
	if err := tx.SelectContext(ctx, &rows, cachedRequeueSelectQuery(d, subscriberID != nil), args...); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	terminalMessageIDs := make([]string, 0)
	terminalSeen := make(map[string]struct{})
	for start := 0; start < len(rows); start += deliveryFailureUpdateChunk {
		end := min(start+deliveryFailureUpdateChunk, len(rows))
		chunk := rows[start:end]
		updateArgs := make([]any, 0, len(chunk)*6+1)
		for _, row := range chunk {
			failureCount := row.FailureCount + 1
			status := DeliveryStatusPending
			visibleAt := now.Add(retryDelayFor(subscriberOptions{
				RetryInitialDelay: time.Duration(row.RetryInitialMillis) * time.Millisecond,
				RetryMaxDelay:     time.Duration(row.RetryMaxMillis) * time.Millisecond,
				RetryMultiplier:   row.RetryMultiplier,
				RetryJitter:       row.RetryJitter,
			}, failureCount, row.MessageID))
			var processedAt any
			if failureCount >= row.MaxAttempts {
				status = DeliveryStatusDeadLetter
				visibleAt = now
				processedAt = now
				if _, exists := terminalSeen[row.MessageID]; !exists {
					terminalSeen[row.MessageID] = struct{}{}
					terminalMessageIDs = append(terminalMessageIDs, row.MessageID)
				}
			}
			updateArgs = append(updateArgs, row.MessageID, row.SubscriberID, failureCount, status, visibleAt, processedAt)
		}
		updateArgs = append(updateArgs, leaseExpiredError)
		if _, err := tx.ExecContext(ctx, cachedRequeueUpdateQuery(d, len(chunk)), updateArgs...); err != nil {
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
	errorText = textlimit.UTF8(errorText, MaxDeliveryTextBytes)
	for start := 0; start < len(rows); start += deliveryErrorInsertChunk {
		end := min(start+deliveryErrorInsertChunk, len(rows))
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
		limit = defaultDeliveryReaperBatchSize
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

func (d *db) nextLeaseExpiry(ctx context.Context) (time.Time, time.Time, bool, error) {
	statement, err := d.statements.get(ctx, d.Conn(), cachedNextLeaseExpiryQuery(d))
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	var rawNow, value any
	if err := statement.QueryRowxContext(ctx).Scan(&rawNow, &value); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	observedAt, exists, err := databaseTime(rawNow)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if !exists {
		return time.Time{}, time.Time{}, false, errors.New("database returned no current time")
	}
	next, exists, err := databaseTime(value)
	return next, observedAt, exists, err
}

func (d *db) ackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string) error {
	return d.ackDeliveryWithTx(ctx, nil, subscriberID, messageID, receipt)
}

func (d *db) ackDeliveryWithTx(ctx context.Context, external *sql.Tx, subscriberID uuid.UUID, messageID, receipt string) error {
	var ackUpdateBase *sqlx.Stmt
	if external == nil {
		var err error
		ackUpdateBase, err = d.statements.get(ctx, d.Conn(), cachedAckUpdateQuery(d))
		if err != nil {
			return err
		}
	}
	return d.withTx(ctx, external, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		var result sql.Result
		if ackUpdateBase != nil {
			result, err = tx.StmtxContext(ctx, ackUpdateBase).ExecContext(ctx,
				now, messageID, subscriberID, receipt, now)
		} else {
			result, err = tx.ExecContext(ctx, cachedAckUpdateQuery(d),
				now, messageID, subscriberID, receipt, now)
		}
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
		err = tx.GetContext(ctx, &state, cachedAckStateQuery(d), messageID, subscriberID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryNotFound
		}
		if err != nil {
			return err
		}
		if state.Status == DeliveryStatusProcessed && state.Receipt.String == receipt {
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
		for start := 0; start < len(valid); start += deliveryAckUpdateChunk {
			end := min(start+deliveryAckUpdateChunk, len(valid))
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
		for start := 0; start < len(messageIDs); start += deliveryIdentityLookupChunk {
			end := min(start+deliveryIdentityLookupChunk, len(messageIDs))
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
			if state.Status != DeliveryStatusProcessed || state.ReceiptToken.String != request.ReceiptToken {
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
	status := DeliveryStatusPending
	visibleAt := now.Add(delay)
	var processedAt any
	if terminal {
		status = DeliveryStatusDeadLetter
		visibleAt = now
		processedAt = now
	}
	storedError := textlimit.UTF8(errorText, MaxDeliveryTextBytes)
	if storedError == "" {
		storedError = defaultNackError
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
	if len(requests) == 0 {
		return terminal, results, nil
	}
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}

		type nackState struct {
			expiredDelivery
			Status         string         `db:"status"`
			ReceiptToken   sql.NullString `db:"receipt_token"`
			LeaseExpiresAt sql.NullTime   `db:"lease_expires_at"`
		}
		messageIDs := make([]string, 0, len(requests))
		seenMessage := make(map[string]struct{}, len(requests))
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
			if _, exists := seenMessage[request.MessageID]; !exists {
				seenMessage[request.MessageID] = struct{}{}
				messageIDs = append(messageIDs, request.MessageID)
			}
		}

		states := make(map[string]nackState, len(messageIDs))
		for start := 0; start < len(messageIDs); start += deliveryIdentityLookupChunk {
			end := min(start+deliveryIdentityLookupChunk, len(messageIDs))
			query, args, err := sqlx.In(`
				SELECT deliveries.message_id, deliveries.subscriber_id, deliveries.failure_count,
				       subscribers.max_attempts, subscribers.retry_initial_delay_ms,
				       subscribers.retry_max_delay_ms, subscribers.retry_multiplier, subscribers.retry_jitter,
				       deliveries.status, deliveries.receipt_token, deliveries.lease_expires_at
				FROM message_deliveries deliveries
				JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
				WHERE deliveries.subscriber_id = ? AND deliveries.message_id IN (?)`, subscriberID, messageIDs[start:end])
			if err != nil {
				return err
			}
			query = tx.Rebind(query) + d.dialect.lockClause("deliveries", false)
			rows := make([]nackState, 0, end-start)
			if err := tx.SelectContext(ctx, &rows, query, args...); err != nil {
				return err
			}
			for _, state := range rows {
				states[state.MessageID] = state
			}
		}

		type nackUpdate struct {
			requestIndex int
			messageID    string
			receiptToken string
			failureCount int
			status       string
			visibleAt    time.Time
			processedAt  any
			errorText    string
			terminal     bool
		}
		updates := make([]nackUpdate, 0, len(requests))
		consumed := make(map[string]struct{}, len(requests))
		for index, request := range requests {
			if results[index] != nil {
				continue
			}
			state, exists := states[request.MessageID]
			if !exists {
				results[index] = ErrDeliveryNotFound
				continue
			}
			if _, duplicate := consumed[request.MessageID]; duplicate {
				results[index] = ErrLeaseLost
				continue
			}
			if state.Status != DeliveryStatusDelivered || !state.ReceiptToken.Valid ||
				state.ReceiptToken.String != request.ReceiptToken || !state.LeaseExpiresAt.Valid ||
				!state.LeaseExpiresAt.Time.After(now) {
				results[index] = ErrLeaseLost
				continue
			}

			failureCount := state.FailureCount + 1
			isTerminal := failureCount >= state.MaxAttempts
			delay := request.RetryDelay
			if delay <= 0 {
				delay = retryDelayFor(subscriberOptions{
					RetryInitialDelay: time.Duration(state.RetryInitialMillis) * time.Millisecond,
					RetryMaxDelay:     time.Duration(state.RetryMaxMillis) * time.Millisecond,
					RetryMultiplier:   state.RetryMultiplier,
					RetryJitter:       state.RetryJitter,
				}, failureCount, request.MessageID)
			}
			status := DeliveryStatusPending
			visibleAt := now.Add(delay)
			var processedAt any
			if isTerminal {
				status = DeliveryStatusDeadLetter
				visibleAt = now
				processedAt = now
			}
			storedError := textlimit.UTF8(request.Error, MaxDeliveryTextBytes)
			if storedError == "" {
				storedError = defaultNackError
			}
			updates = append(updates, nackUpdate{
				requestIndex: index,
				messageID:    request.MessageID,
				receiptToken: request.ReceiptToken,
				failureCount: failureCount,
				status:       status,
				visibleAt:    visibleAt,
				processedAt:  processedAt,
				errorText:    storedError,
				terminal:     isTerminal,
			})
			consumed[request.MessageID] = struct{}{}
		}

		updated := make(map[string]struct{}, len(updates))
		for start := 0; start < len(updates); start += deliveryNackUpdateChunk {
			end := min(start+deliveryNackUpdateChunk, len(updates))
			chunk := updates[start:end]
			args := make([]any, 0, len(chunk)*7+2)
			for _, update := range chunk {
				args = append(args,
					update.messageID, update.receiptToken, update.failureCount, update.status,
					update.visibleAt, update.processedAt, update.errorText,
				)
			}
			args = append(args, subscriberID, now)
			updatedIDs := make([]string, 0, len(chunk))
			if err := tx.SelectContext(ctx, &updatedIDs, cachedBatchNackUpdateQuery(d, len(chunk)), args...); err != nil {
				return err
			}
			for _, messageID := range updatedIDs {
				updated[messageID] = struct{}{}
			}
		}

		successful := make([]nackUpdate, 0, len(updates))
		completed := make([]string, 0, len(updates))
		for _, update := range updates {
			if _, ok := updated[update.messageID]; !ok {
				results[update.requestIndex] = ErrLeaseLost
				continue
			}
			terminal[update.requestIndex] = update.terminal
			successful = append(successful, update)
			if update.terminal {
				completed = append(completed, update.messageID)
			}
		}
		for start := 0; start < len(successful); start += deliveryErrorInsertChunk {
			end := min(start+deliveryErrorInsertChunk, len(successful))
			var query strings.Builder
			query.WriteString("INSERT INTO delivery_errors (id, message_id, subscriber_id, failure_count, error, failed_at) VALUES ")
			args := make([]any, 0, (end-start)*6)
			for index, update := range successful[start:end] {
				if index > 0 {
					query.WriteByte(',')
				}
				query.WriteString("(?, ?, ?, ?, ?, ?)")
				args = append(args, uuid.NewString(), update.messageID, subscriberID, update.failureCount, update.errorText, now)
			}
			if _, err := tx.ExecContext(ctx, tx.Rebind(query.String()), args...); err != nil {
				return err
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
		maximumExpiry := now.Add(MaximumDeliveryLease)
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
	_, err := tx.ExecContext(ctx, cachedCompleteScheduleRunQuery(d), messageID)
	return err
}

func (d *db) completeScheduleRunsForMessagesTx(ctx context.Context, tx *sqlx.Tx, messageIDs []string) error {
	for start := 0; start < len(messageIDs); start += deliveryIdentityLookupChunk {
		end := min(start+deliveryIdentityLookupChunk, len(messageIDs))
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

func (d *db) nextDeliveryWake(ctx context.Context, subscriberID uuid.UUID) (time.Time, time.Time, bool, error) {
	statement, err := d.statements.get(ctx, d.Conn(), cachedNextDeliveryWakeQuery(d))
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	var rawNow, value any
	err = statement.QueryRowxContext(ctx, subscriberID, subscriberID).Scan(&rawNow, &value)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	observedAt, exists, err := databaseTime(rawNow)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if !exists {
		return time.Time{}, time.Time{}, false, errors.New("database returned no current time")
	}
	next, exists, err := databaseTime(value)
	return next, observedAt, exists, err
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

func (d *db) batchReplayDeadLetters(ctx context.Context, subscriberID uuid.UUID, messageIDs []string) ([]bool, error) {
	results := make([]bool, len(messageIDs))
	if len(messageIDs) == 0 {
		return results, nil
	}
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		now, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}
		unique := make([]string, 0, len(messageIDs))
		seen := make(map[string]struct{}, len(messageIDs))
		for _, messageID := range messageIDs {
			if _, exists := seen[messageID]; exists {
				continue
			}
			seen[messageID] = struct{}{}
			unique = append(unique, messageID)
		}
		updated := make(map[string]struct{}, len(unique))
		for start := 0; start < len(unique); start += deliveryIdentityLookupChunk {
			end := min(start+deliveryIdentityLookupChunk, len(unique))
			query, args, err := sqlx.In(`
				UPDATE message_deliveries
				SET status = 'pending', delivery_count = 0, failure_count = 0,
				    visible_at = ?, receipt_token = NULL,
				    lease_expires_at = NULL, processed_at = NULL, last_error = NULL
				WHERE subscriber_id = ? AND status = 'dead_letter' AND message_id IN (?)
				RETURNING message_id`, now, subscriberID, unique[start:end])
			if err != nil {
				return err
			}
			updatedIDs := make([]string, 0, end-start)
			if err := tx.SelectContext(ctx, &updatedIDs, tx.Rebind(query), args...); err != nil {
				return err
			}
			for _, messageID := range updatedIDs {
				updated[messageID] = struct{}{}
			}
		}
		consumed := make(map[string]struct{}, len(updated))
		for index, messageID := range messageIDs {
			if _, duplicate := consumed[messageID]; duplicate {
				continue
			}
			if _, ok := updated[messageID]; ok {
				results[index] = true
				consumed[messageID] = struct{}{}
			}
		}
		return nil
	})
	return results, err
}
