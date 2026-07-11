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
	maximumDeliveryLease    = persistence.MaximumDeliveryLease
	maxDeliveryClaimSize    = 1000
	maxDeliveryPageSize     = 1000
	deliveryReaperBatchSize = 1000
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
	} else if lease > maximumDeliveryLease {
		return nil, fmt.Errorf("%w: lease cannot exceed 12h", ErrInvalidPublish)
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

// ClaimWait long-polls without a polling backoff. Its timer is reset to the
// earliest pending visibility or expired lease deadline stored in the DB.
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
		next, exists, err := q.db.nextDeliveryWake(ctx, subscriberRuntime.id)
		if err != nil {
			return nil, err
		}
		if exists && !next.After(time.Now()) {
			messages, claimErr := q.Claim(ctx, topic, subscriber, limit, lease)
			if claimErr != nil || len(messages) > 0 {
				return messages, claimErr
			}
			// Another PostgreSQL worker may hold the due row under SKIP LOCKED.
			// Avoid a tight read/write loop while that short transaction finishes.
			next = time.Now().Add(10 * time.Millisecond)
		}
		// Notifications are hints; a bounded poll keeps PostgreSQL multi-process
		// publishers correct even if a notification is missed.
		wait := time.Second
		if exists {
			wait = time.Until(next)
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
	} else if extension > maximumDeliveryLease {
		return time.Time{}, fmt.Errorf("%w: lease extension cannot exceed 12h", ErrInvalidPublish)
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
		next, exists, err := q.db.nextLeaseExpiry(q.serverCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			q.deliveryHealthy.Store(false)
			if !waitForMaintenanceRetry(q.serverCtx, clock) {
				return
			}
			continue
		}
		q.deliveryHealthy.Store(true)
		wait := time.Second
		if exists {
			wait = time.Until(next)
			if wait < 0 {
				wait = 0
			}
			// The timer is only a hint. A one-second reconciliation ceiling
			// keeps cross-node clock skew from delaying lease expiry.
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

		reapedAny := false
		reapFailed := false
		for {
			count, err := q.db.reapExpiredDeliveries(q.serverCtx, deliveryReaperBatchSize)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				q.deliveryHealthy.Store(false)
				slog.Error("delivery reaper failed", "error", err)
				reapFailed = true
				break
			}
			reapedAny = reapedAny || count > 0
			if count < deliveryReaperBatchSize {
				break
			}
		}
		if reapedAny {
			for _, topic := range q.registry.Load().byID {
				topic.notify()
			}
		}
		if reapFailed && !waitForMaintenanceRetry(q.serverCtx, clock) {
			return
		}
	}
}

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

func (q *Queue) PauseTopic(ctx context.Context, topic Topic) error {
	return q.setTopicPaused(ctx, topic, true)
}

func (q *Queue) ResumeTopic(ctx context.Context, topic Topic) error {
	return q.setTopicPaused(ctx, topic, false)
}

func (q *Queue) setTopicPaused(ctx context.Context, topic Topic, paused bool) error {
	if err := q.beginControlOperation(); err != nil {
		return err
	}
	defer q.controlOps.Done()
	q.mtx.Lock()
	defer q.mtx.Unlock()
	topicRuntime, ok := q.getTopicRuntime(topic)
	if !ok {
		return ErrTopicNotFound
	}
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

func (q *Queue) PauseSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.setSubscriberPaused(ctx, topic, subscriber, true)
}

func (q *Queue) ResumeSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.setSubscriberPaused(ctx, topic, subscriber, false)
}

func (q *Queue) setSubscriberPaused(ctx context.Context, topic Topic, subscriber string, paused bool) error {
	if err := q.beginControlOperation(); err != nil {
		return err
	}
	defer q.controlOps.Done()
	q.mtx.Lock()
	defer q.mtx.Unlock()
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return err
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

func (q *Queue) ListDeliveries(ctx context.Context, topic Topic, subscriber string, deadLetter bool, limit int, cursor string) (DeliveryPage, error) {
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	if err != nil {
		return DeliveryPage{}, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maxDeliveryPageSize {
		limit = maxDeliveryPageSize
	}
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
			ID: row.MessageID, Message: row.Message, Headers: headers,
			CorrelationID: row.CorrelationID.String, Status: row.Status,
			DeliveryCount: row.DeliveryCount, FailureCount: row.FailureCount, Priority: row.Priority,
			ReceiptToken:   row.ReceiptToken.String,
			LeaseExpiresAt: optionalTime(row.LeaseExpiresAt),
			VisibleAt:      row.VisibleAt,
			CreatedAt:      row.CreatedAt,
		})
	}
	return page, nil
}

func (q *Queue) ReplayDeadLetters(ctx context.Context, topic Topic, subscriber string, messageIDs []string) []DeliveryResult {
	_, subscriberRuntime, err := q.deliveryTarget(topic, subscriber)
	results := make([]DeliveryResult, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		result := DeliveryResult{MessageID: messageID, Status: DeliveryStatusPending}
		if err != nil {
			result.Status = DeliveryResultStatusFailed
			result.Error = publicDeliveryError(err)
		} else if _, parseErr := uuid.Parse(messageID); parseErr != nil {
			result.Status = DeliveryResultStatusFailed
			result.Error = ErrDeliveryNotFound.Error()
		} else {
			updated, replayErr := q.db.replayDeadLetter(ctx, subscriberRuntime.id, messageID)
			if replayErr != nil || !updated {
				result.Status = DeliveryResultStatusFailed
				if replayErr != nil {
					result.Error = "replay failed"
				} else {
					result.Error = ErrDeliveryNotFound.Error()
				}
			}
		}
		results = append(results, result)
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
