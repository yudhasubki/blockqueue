package blockqueue

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

var (
	ErrLeaseLost        = persistence.ErrLeaseLost
	ErrDeliveryNotFound = persistence.ErrDeliveryNotFound
	ErrInvalidReceipt   = persistence.ErrInvalidReceipt
	ErrResourcePaused   = errors.New("topic or subscriber is paused")
)

const (
	maxDeliveryClaimSize    = 1000
	deliveryReaperBatchSize = 1000
	deliveryReaperPassLimit = 8
)

// MaxDeliveryTextBytes is the maximum persisted size of a NACK error or
// cancellation reason. Longer values are truncated on a valid UTF-8 boundary.
const MaxDeliveryTextBytes = persistence.MaxDeliveryTextBytes

// Claim atomically leases visible deliveries in canonical priority order.
// Every redelivery receives a fresh receipt token.
func (q *Queue) Claim(ctx context.Context, topic Topic, subscriber string, limit int, lease time.Duration) (Deliveries, error) {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationClaim, started)
	if q.State() != LifecycleRunning {
		return nil, ErrQueueNotRunning
	}
	topicRuntime, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return nil, err
	}
	if topicRuntime.paused.Load() || subscriberRuntime.paused.Load() {
		return Deliveries{}, ErrResourcePaused
	}
	if limit <= 0 {
		limit = subscriberRuntime.options.DequeueBatchSize
	}
	if limit > maxDeliveryClaimSize {
		limit = maxDeliveryClaimSize
	}
	if lease <= 0 {
		lease = subscriberRuntime.options.VisibilityDuration
	} else if lease > MaximumDeliveryLease {
		return nil, fmt.Errorf("%w: lease cannot exceed %s", ErrInvalidPublish, MaximumDeliveryLease)
	}
	if topicRuntime.paused.Load() || subscriberRuntime.paused.Load() || subscriberRuntime.deleted.Load() {
		return Deliveries{}, ErrResourcePaused
	}
	rows, err := q.db.claimDeliveries(ctx, subscriberRuntime.id, limit, lease)
	if err != nil {
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(
				metric.DeliveryOperationClaim, metric.OutcomeFailed,
			).Inc()
		}
		return nil, err
	}
	if !q.options.DisableMetrics {
		metric.DeliveryOperations.WithLabelValues(
			metric.DeliveryOperationClaim, metric.OutcomeSuccess,
		).Add(float64(len(rows)))
	}
	if len(rows) > 0 {
		q.signalReaper()
	}
	response := make(Deliveries, 0, len(rows))
	for _, row := range rows {
		headers := make(map[string]string)
		_ = json.Unmarshal([]byte(row.Headers), &headers)
		response = append(response, Delivery{
			ID:             row.MessageID,
			Message:        row.Message,
			Headers:        headers,
			CorrelationID:  row.CorrelationID.String,
			Status:         row.Status,
			DeliveryCount:  row.DeliveryCount,
			FailureCount:   row.FailureCount,
			Priority:       row.Priority,
			ReceiptToken:   row.ReceiptToken.String,
			LeaseExpiresAt: optionalTime(row.LeaseExpiresAt),
			VisibleAt:      row.VisibleAt,
			CreatedAt:      row.CreatedAt,
		})
	}
	return response, nil
}

// ClaimWait claims immediately available work or long-polls without a polling
// backoff. Its timer follows the earliest pending visibility or expired lease
// deadline stored in the database and also wakes on hints or ctx cancellation.
func (q *Queue) ClaimWait(ctx context.Context, topic Topic, subscriber string, limit int, lease time.Duration) (Deliveries, error) {
	topicRuntime, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return nil, err
	}
	for {
		if subscriberRuntime.deleted.Load() {
			return nil, ErrSubscriberDeleted
		}
		if topicRuntime.paused.Load() || subscriberRuntime.paused.Load() {
			return nil, ErrResourcePaused
		}
		next, observedAt, exists, err := q.db.nextDeliveryWake(ctx, subscriberRuntime.id)
		if err != nil {
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return Deliveries{}, nil
			}
			return nil, err
		}
		if exists && !next.After(observedAt) {
			messages, claimErr := q.Claim(ctx, topic, subscriber, limit, lease)
			if claimErr != nil && ctx.Err() != nil &&
				(errors.Is(claimErr, context.Canceled) || errors.Is(claimErr, context.DeadlineExceeded)) {
				return Deliveries{}, nil
			}
			if claimErr != nil || len(messages) > 0 {
				return messages, claimErr
			}
			// Another PostgreSQL worker may hold the due row under SKIP LOCKED.
			// Avoid a tight read/write loop while that short transaction finishes.
			next = observedAt.Add(10 * time.Millisecond)
		}
		// Notifications are hints; a bounded poll keeps PostgreSQL multi-process
		// publishers correct even if a notification is missed.
		wait := time.Second
		if exists {
			wait = next.Sub(observedAt)
			if wait > time.Second {
				wait = time.Second
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Deliveries{}, nil
		case <-subscriberRuntime.deliveryWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

// AckDelivery receipt-fences a successful lease transition to processed.
// Repeating the same successful receipt is idempotent.
func (q *Queue) AckDelivery(ctx context.Context, topic Topic, subscriber, messageID, receipt string) error {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationAck, started)
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
	}
	if receipt == "" {
		return ErrInvalidReceipt
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return ErrDeliveryNotFound
	}
	if _, err := uuid.Parse(receipt); err != nil {
		return ErrLeaseLost
	}
	if err := q.db.ackDelivery(ctx, subscriberRuntime.id, messageID, receipt); err != nil {
		result := metric.OutcomeFailed
		if errors.Is(err, ErrLeaseLost) {
			result = metric.OutcomeLeaseLost
		}
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationAck, result).Inc()
		}
		return err
	}
	if !q.options.DisableMetrics {
		metric.DeliveryOperations.WithLabelValues(
			metric.DeliveryOperationAck, metric.OutcomeSuccess,
		).Inc()
	}
	return nil
}

// NackDelivery records one failed attempt. A zero retryDelay applies the
// subscriber retry policy; a stale receipt returns ErrLeaseLost.
func (q *Queue) NackDelivery(ctx context.Context, topic Topic, subscriber, messageID, receipt string, retryDelay time.Duration, errorText string) error {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationNack, started)
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(
				metric.DeliveryOperationNack, metric.OutcomeFailed,
			).Inc()
		}
		return err
	}
	if receipt == "" {
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(
				metric.DeliveryOperationNack, metric.OutcomeFailed,
			).Inc()
		}
		return ErrInvalidReceipt
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return ErrDeliveryNotFound
	}
	if _, err := uuid.Parse(receipt); err != nil {
		return ErrLeaseLost
	}
	if retryDelay < 0 {
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(
				metric.DeliveryOperationNack, metric.OutcomeFailed,
			).Inc()
		}
		return fmt.Errorf("%w: retry delay cannot be negative", ErrInvalidPublish)
	}
	terminal, err := q.db.nackDelivery(ctx, subscriberRuntime.id, messageID, receipt, retryDelay, errorText)
	if err != nil {
		result := metric.OutcomeFailed
		if errors.Is(err, ErrLeaseLost) {
			result = metric.OutcomeLeaseLost
		}
		if !q.options.DisableMetrics {
			metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationNack, result).Inc()
		}
		return err
	}
	if !q.options.DisableMetrics {
		result := metric.OutcomeSuccess
		if terminal {
			result = metric.OutcomeDeadLetter
		}
		metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationNack, result).Inc()
	}
	subscriberRuntime.notify()
	return nil
}

// ExtendLease moves an active receipt's expiry forward using database time.
func (q *Queue) ExtendLease(ctx context.Context, topic Topic, subscriber, messageID, receipt string, extension time.Duration) (time.Time, error) {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationLease, started)
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return time.Time{}, err
	}
	if receipt == "" {
		return time.Time{}, ErrInvalidReceipt
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return time.Time{}, ErrDeliveryNotFound
	}
	if _, err := uuid.Parse(receipt); err != nil {
		return time.Time{}, ErrLeaseLost
	}
	if extension <= 0 {
		extension = subscriberRuntime.options.VisibilityDuration
	} else if extension > MaximumDeliveryLease {
		return time.Time{}, fmt.Errorf("%w: lease extension cannot exceed %s", ErrInvalidPublish, MaximumDeliveryLease)
	}
	expires, err := q.db.extendDeliveryLease(ctx, subscriberRuntime.id, messageID, receipt, extension)
	result := metric.OutcomeSuccess
	if err != nil {
		result = metric.OutcomeFailed
		if errors.Is(err, ErrLeaseLost) {
			result = metric.OutcomeLeaseLost
		}
	}
	if !q.options.DisableMetrics {
		metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationLease, result).Inc()
	}
	return expires, err
}

func (q *Queue) signalReaper() {
	select {
	case q.reaperSignal <- struct{}{}:
	default:
	}
}

func (q *Queue) startDeliveryReaper() {
	clock := q.clock()
	for {
		next, observedAt, exists, err := q.db.nextLeaseExpiry(q.serverCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			q.setDeliveryHealthy(false)
			if !waitForMaintenanceRetry(q.serverCtx, clock) {
				return
			}
			continue
		}
		q.setDeliveryHealthy(true)
		wait := time.Second
		if exists {
			wait = next.Sub(observedAt)
			if wait < 0 {
				wait = 0
			}
			// The timer is only a hint. A one-second reconciliation ceiling
			// provides reconciliation if a signal is missed.
			if wait > time.Second {
				wait = time.Second
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-q.serverCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-q.reaperSignal:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
		}

		started := time.Now()
		reapedAny := false
		reapFailed := false
		yielded := false
		var reaped int64
		for batch := 0; batch < deliveryReaperPassLimit; batch++ {
			count, err := q.db.reapExpiredDeliveries(q.serverCtx, deliveryReaperBatchSize)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				q.setDeliveryHealthy(false)
				slog.Error("delivery reaper failed", "error", err)
				reapFailed = true
				break
			}
			reaped += count
			reapedAny = reapedAny || count > 0
			if count < deliveryReaperBatchSize {
				break
			}
			yielded = batch+1 == deliveryReaperPassLimit
		}
		q.observeMaintenancePass(metric.MaintenanceOperationDeliveryReaper, started, reaped, reapFailed, yielded)
		if reapedAny {
			for _, topic := range q.registry.Load().byID {
				topic.notify()
			}
		}
		if reapFailed && !waitForMaintenanceRetry(q.serverCtx, clock) {
			return
		}
		if yielded && !waitForMaintenanceYield(q.serverCtx, clock) {
			return
		}
	}
}

// BatchAckDeliveries acknowledges items in one set-based transaction and
// returns an outcome for every request.
func (q *Queue) BatchAckDeliveries(ctx context.Context, topic Topic, subscriber string, requests []BatchAckItem) []DeliveryResult {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationBatchAck, started)
	results := make([]DeliveryResult, len(requests))
	_, subscriberRuntime, targetErr := q.deliveryTarget(topic, subscriber)
	itemErrors := make([]error, len(requests))
	if targetErr != nil {
		for index := range itemErrors {
			itemErrors[index] = targetErr
		}
	} else {
		var transactionErr error
		itemErrors, transactionErr = q.db.batchAckDeliveries(ctx, subscriberRuntime.id, requests)
		if transactionErr != nil {
			for index := range itemErrors {
				itemErrors[index] = transactionErr
			}
		}
	}
	for index, item := range requests {
		err := itemErrors[index]
		result := DeliveryResult{MessageID: item.MessageID, Status: DeliveryStatusProcessed}
		if err != nil {
			result.Status = DeliveryResultStatusFailed
			result.Error = publicDeliveryError(err)
		}
		results[index] = result
		if !q.options.DisableMetrics {
			label := metric.OutcomeSuccess
			if err != nil {
				label = metric.OutcomeFailed
				if errors.Is(err, ErrLeaseLost) {
					label = metric.OutcomeLeaseLost
				}
			}
			metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationAck, label).Inc()
		}
	}
	return results
}

// BatchNackDeliveries records failures in one set-based transaction and
// returns an outcome for every request.
func (q *Queue) BatchNackDeliveries(ctx context.Context, topic Topic, subscriber string, requests []BatchNackItem) []DeliveryResult {
	started := time.Now()
	defer q.observeDeliveryDuration(metric.DeliveryOperationBatchNack, started)
	results := make([]DeliveryResult, len(requests))
	terminal := make([]bool, len(requests))
	itemErrors := make([]error, len(requests))
	_, subscriberRuntime, targetErr := q.deliveryTarget(topic, subscriber)
	if targetErr != nil {
		for index := range itemErrors {
			itemErrors[index] = targetErr
		}
	} else {
		var transactionErr error
		terminal, itemErrors, transactionErr = q.db.batchNackDeliveries(ctx, subscriberRuntime.id, requests)
		if transactionErr != nil {
			for index := range itemErrors {
				itemErrors[index] = transactionErr
			}
		}
	}
	for index, item := range requests {
		err := itemErrors[index]
		status := DeliveryStatusPending
		if terminal[index] {
			status = DeliveryStatusDeadLetter
		}
		result := DeliveryResult{MessageID: item.MessageID, Status: status}
		if err != nil {
			result.Status = DeliveryResultStatusFailed
			result.Error = publicDeliveryError(err)
		}
		results[index] = result
		if !q.options.DisableMetrics {
			label := metric.OutcomeSuccess
			if terminal[index] {
				label = metric.OutcomeDeadLetter
			}
			if err != nil {
				label = metric.OutcomeFailed
				if errors.Is(err, ErrLeaseLost) {
					label = metric.OutcomeLeaseLost
				}
			}
			metric.DeliveryOperations.WithLabelValues(metric.DeliveryOperationNack, label).Inc()
		}
	}
	if targetErr == nil {
		subscriberRuntime.notify()
	}
	return results
}

// PauseTopic stops new claims while continuing to accept publishes.
func (q *Queue) PauseTopic(ctx context.Context, topic Topic) error {
	return q.setTopicPaused(ctx, topic, true)
}

// ResumeTopic allows claims and wakes waiting subscribers.
func (q *Queue) ResumeTopic(ctx context.Context, topic Topic) error {
	return q.setTopicPaused(ctx, topic, false)
}

func (q *Queue) setTopicPaused(ctx context.Context, topic Topic, paused bool) error {
	mutation, err := q.beginTopicMutation(topic)
	if err != nil {
		return err
	}
	defer mutation.close()
	topicRuntime := mutation.runtime
	if err := q.db.setTopicPaused(ctx, topicRuntime.id, paused); err != nil {
		return err
	}
	topicRuntime.paused.Store(paused)
	q.topologyVersion.Add(1)
	if !paused {
		topicRuntime.notify()
	}
	return nil
}

// PauseSubscriber stops new claims for one subscriber while retaining work.
func (q *Queue) PauseSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.setSubscriberPaused(ctx, topic, subscriber, true)
}

// ResumeSubscriber allows claims and wakes the selected subscriber.
func (q *Queue) ResumeSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.setSubscriberPaused(ctx, topic, subscriber, false)
}

func (q *Queue) setSubscriberPaused(ctx context.Context, topic Topic, subscriber string, paused bool) error {
	mutation, err := q.beginTopicMutation(topic)
	if err != nil {
		return err
	}
	defer mutation.close()
	subscriberRuntime, ok := mutation.runtime.subscriberByName(subscriber)
	if !ok || subscriberRuntime.deleted.Load() {
		return ErrSubscriberNotFound
	}
	if err := q.db.setSubscriberPaused(ctx, subscriberRuntime.id, paused); err != nil {
		return err
	}
	subscriberRuntime.paused.Store(paused)
	q.topologyVersion.Add(1)
	if !paused {
		subscriberRuntime.notify()
	}
	return nil
}

// ListDeliveries returns a cursor page of active deliveries or, when
// deadLetter is true, dead-lettered deliveries.
func (q *Queue) ListDeliveries(ctx context.Context, topic Topic, subscriber string, deadLetter bool, limit int, cursor string) (DeliveryPage, error) {
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return DeliveryPage{}, err
	}
	limit = normalizedResourcePageLimit(limit)
	rows, err := q.db.listDeliveries(ctx, subscriberRuntime.id, deadLetter, limit+1, cursor)
	if err != nil {
		return DeliveryPage{}, err
	}
	next := ""
	if len(rows) > limit {
		last := rows[limit-1]
		next = encodeDeliveryCursor(last.CreatedAt, last.MessageID)
		rows = rows[:limit]
	}
	page := DeliveryPage{Messages: make(Deliveries, 0, len(rows)), NextCursor: next}
	for _, row := range rows {
		headers := make(map[string]string)
		_ = json.Unmarshal([]byte(row.Headers), &headers)
		page.Messages = append(page.Messages, Delivery{
			ID:             row.MessageID,
			Message:        row.Message,
			Headers:        headers,
			CorrelationID:  row.CorrelationID.String,
			Status:         row.Status,
			DeliveryCount:  row.DeliveryCount,
			FailureCount:   row.FailureCount,
			Priority:       row.Priority,
			ReceiptToken:   row.ReceiptToken.String,
			LeaseExpiresAt: optionalTime(row.LeaseExpiresAt),
			VisibleAt:      row.VisibleAt,
			CreatedAt:      row.CreatedAt,
		})
	}
	return page, nil
}

// ReplayDeadLetters returns selected terminal deliveries to pending and resets
// their failure budget while retaining prior error history.
func (q *Queue) ReplayDeadLetters(ctx context.Context, topic Topic, subscriber string, messageIDs []string) []DeliveryResult {
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	results := make([]DeliveryResult, len(messageIDs))
	validIDs := make([]string, 0, len(messageIDs))
	validIndexes := make([]int, 0, len(messageIDs))
	for index, messageID := range messageIDs {
		results[index] = DeliveryResult{MessageID: messageID, Status: DeliveryStatusPending}
		if err != nil {
			results[index].Status = DeliveryResultStatusFailed
			results[index].Error = publicDeliveryError(err)
			continue
		}
		if _, parseErr := uuid.Parse(messageID); parseErr != nil {
			results[index].Status = DeliveryResultStatusFailed
			results[index].Error = ErrDeliveryNotFound.Error()
			continue
		}
		validIDs = append(validIDs, messageID)
		validIndexes = append(validIndexes, index)
	}
	if subscriberRuntime != nil && len(validIDs) > 0 {
		updated, replayErr := q.db.batchReplayDeadLetters(ctx, subscriberRuntime.id, validIDs)
		for validIndex, resultIndex := range validIndexes {
			if replayErr == nil && updated[validIndex] {
				continue
			}
			results[resultIndex].Status = DeliveryResultStatusFailed
			if replayErr != nil {
				results[resultIndex].Error = "replay failed"
			} else {
				results[resultIndex].Error = ErrDeliveryNotFound.Error()
			}
		}
	}
	if subscriberRuntime != nil {
		subscriberRuntime.notify()
	}
	return results
}

func (q *Queue) deliveryTarget(topic Topic, subscriber string) (*topicRuntime, *subscriberRuntime, error) {
	topicRuntime, ok := q.getTopicRuntime(topic)
	if !ok {
		return nil, nil, ErrTopicNotFound
	}
	subscriberRuntime, ok := topicRuntime.subscriberByName(subscriber)
	if !ok {
		return nil, nil, ErrSubscriberNotFound
	}
	if subscriberRuntime.deleted.Load() {
		return nil, nil, ErrSubscriberNotFound
	}
	return topicRuntime, subscriberRuntime, nil
}

func (q *Queue) observeDeliveryDuration(operation string, started time.Time) {
	if !q.options.DisableMetrics {
		metric.DeliveryDuration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
	}
}

func publicDeliveryError(err error) string {
	switch {
	case errors.Is(err, ErrLeaseLost):
		return ErrLeaseLost.Error()
	case errors.Is(err, ErrDeliveryNotFound):
		return ErrDeliveryNotFound.Error()
	case errors.Is(err, ErrInvalidReceipt):
		return ErrInvalidReceipt.Error()
	default:
		return "delivery operation failed"
	}
}

func encodeDeliveryCursor(createdAt time.Time, messageID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + messageID))
}

func optionalTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
