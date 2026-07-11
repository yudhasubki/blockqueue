package blockqueue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Publish is durable by default. A nil error means the canonical message and
// every subscriber delivery row committed successfully.
func (q *Queue) Publish(ctx context.Context, topic Topic, request Message) (PublishReceipt, error) {
	return q.publishOne(ctx, topic, request, true)
}

func (q *Queue) BatchPublish(ctx context.Context, topic Topic, requests []Message) (PublishReceipts, error) {
	return q.publishRequests(ctx, topic, requests, true)
}

// PublishDurable waits until the message and all subscriber delivery rows are
// committed. Duplicate is definitive in the returned receipt.
func (q *Queue) PublishDurable(ctx context.Context, topic Topic, request Message) (PublishReceipt, error) {
	return q.Publish(ctx, topic, request)
}

func (q *Queue) BatchPublishDurable(ctx context.Context, topic Topic, requests []Message) (PublishReceipts, error) {
	return q.publishRequests(ctx, topic, requests, true)
}

// PublishAsync returns the message identity at admission time. It is useful to
// Go callers that need the same receipt exposed by HTTP 202 responses.
func (q *Queue) PublishAsync(ctx context.Context, topic Topic, request Message) (PublishReceipt, error) {
	return q.publishOne(ctx, topic, request, false)
}

func (q *Queue) BatchPublishAsync(ctx context.Context, topic Topic, requests []Message) (PublishReceipts, error) {
	return q.publishRequests(ctx, topic, requests, false)
}

func (q *Queue) publishOne(ctx context.Context, topic Topic, request Message, durable bool) (PublishReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	state := q.State()
	if state != LifecycleRunning {
		if state == LifecycleStopping || state == LifecycleStopped {
			return PublishReceipt{}, ErrQueueStopping
		}
		return PublishReceipt{}, ErrQueueNotRunning
	}

	var write writeRequest
	var scheduledAt time.Time
	var admission *writeAdmission
	err := func() error {
		runtime, exists := q.getTopicRuntime(topic)
		if !exists {
			return ErrTopicNotFound
		}
		runtime.admissionMu.RLock()
		defer runtime.admissionMu.RUnlock()
		q.admissionMu.RLock()
		defer q.admissionMu.RUnlock()
		if err := q.requireRunning(); err != nil {
			return err
		}
		if current := q.registry.Load().byName[topic.Name]; current != runtime || runtime.deleted.Load() {
			return ErrTopicNotFound
		}
		if len(runtime.registry.Load().byID) == 0 {
			return ErrNoActiveSubscriber
		}
		now := time.Now().UTC().Truncate(time.Millisecond)
		var err error
		write, scheduledAt, err = buildWriteRequest(runtime.id, request, now)
		if err != nil {
			return err
		}
		admission, err = q.writer.admitOne(ctx, write, durable)
		return err
	}()
	if err != nil {
		return PublishReceipt{}, err
	}
	receipt := PublishReceipt{
		MessageID: write.MessageID, State: PublishStateAdmitted, ScheduledAt: scheduledAt,
	}
	if durable {
		duplicates, err := q.writer.waitAdmission(ctx, admission)
		if err != nil {
			return PublishReceipt{}, err
		}
		duplicate := duplicates[0]
		receipt.State = PublishStatePersisted
		receipt.Duplicate = &duplicate
		receipts := PublishReceipts{receipt}
		if err := q.restorePersistedScheduledTimes(ctx, nil, receipts); err != nil {
			return PublishReceipt{}, err
		}
		receipt = receipts[0]
	}
	return receipt, nil
}

func (q *Queue) publishRequests(ctx context.Context, topic Topic, requests []Message, durable bool) (PublishReceipts, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	state := q.State()
	if state != LifecycleRunning {
		if state == LifecycleStopping || state == LifecycleStopped {
			return nil, ErrQueueStopping
		}
		return nil, ErrQueueNotRunning
	}
	if len(requests) == 0 {
		return PublishReceipts{}, nil
	}

	batch := make([]writeRequest, len(requests))
	receipts := make(PublishReceipts, len(requests))
	var admission *writeAdmission
	err := func() error {
		runtime, exists := q.getTopicRuntime(topic)
		if !exists {
			return ErrTopicNotFound
		}
		runtime.admissionMu.RLock()
		defer runtime.admissionMu.RUnlock()
		q.admissionMu.RLock()
		defer q.admissionMu.RUnlock()
		if err := q.requireRunning(); err != nil {
			return err
		}
		if current := q.registry.Load().byName[topic.Name]; current != runtime || runtime.deleted.Load() {
			return ErrTopicNotFound
		}
		if len(runtime.registry.Load().byID) == 0 {
			return ErrNoActiveSubscriber
		}

		// SQLite's database clock has millisecond precision. Keeping admission
		// timestamps at the same precision prevents an immediately committed
		// message from appearing fractionally in the future to a claim transaction.
		now := time.Now().UTC().Truncate(time.Millisecond)
		var seenKeys map[string]string
		for i, request := range requests {
			write, scheduledAt, err := buildWriteRequest(runtime.id, request, now)
			if err != nil {
				return err
			}
			if request.IdempotencyKey != "" {
				if seenKeys == nil {
					seenKeys = make(map[string]string)
				}
				if previous, ok := seenKeys[request.IdempotencyKey]; ok && previous != write.IdempotencyHash {
					return fmt.Errorf("%w: duplicate key %q has different payload", ErrInvalidPublish, request.IdempotencyKey)
				}
				seenKeys[request.IdempotencyKey] = write.IdempotencyHash
			}
			batch[i] = write
			receipts[i] = PublishReceipt{
				MessageID:   write.MessageID,
				State:       PublishStateAdmitted,
				Duplicate:   nil,
				ScheduledAt: scheduledAt,
			}
		}
		var err error
		admission, err = q.writer.admitBatch(ctx, batch, durable)
		return err
	}()
	if err != nil {
		return nil, err
	}

	if durable {
		duplicates, err := q.writer.waitAdmission(ctx, admission)
		if err != nil {
			return nil, err
		}
		for i := range receipts {
			duplicate := duplicates[i]
			receipts[i].State = PublishStatePersisted
			receipts[i].Duplicate = &duplicate
		}
		if err := q.restorePersistedScheduledTimes(ctx, nil, receipts); err != nil {
			return nil, err
		}
	}
	return receipts, nil
}

func (q *Queue) restorePersistedScheduledTimes(ctx context.Context, tx *sql.Tx, receipts PublishReceipts) error {
	ids := make([]string, 0, len(receipts))
	for _, receipt := range receipts {
		ids = append(ids, receipt.MessageID)
	}
	if len(ids) == 0 {
		return nil
	}
	times, err := q.db.messageScheduledTimes(ctx, tx, ids)
	if err != nil {
		return err
	}
	for index := range receipts {
		if persisted, ok := times[receipts[index].MessageID]; ok {
			receipts[index].ScheduledAt = persisted
		}
	}
	return nil
}

func buildWriteRequest(topicID uuid.UUID, request Message, now time.Time) (writeRequest, time.Time, error) {
	if len(request.Message) > 1<<20 {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: message exceeds 1MiB", ErrInvalidPublish)
	}
	if request.Priority < -1000 || request.Priority > 1000 {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: priority must be between -1000 and 1000", ErrInvalidPublish)
	}
	if len([]byte(request.IdempotencyKey)) > 128 {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: idempotency_key exceeds 128 bytes", ErrInvalidPublish)
	}
	if len([]byte(request.CorrelationID)) > 255 {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: correlation_id exceeds 255 bytes", ErrInvalidPublish)
	}
	if request.Delay != "" && request.ScheduleAt != "" {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: delay and schedule_at are mutually exclusive", ErrInvalidPublish)
	}
	scheduledAt := now
	scheduleMode := scheduleModeImmediate
	scheduleValue := ""
	var scheduleDelay time.Duration
	if request.Delay != "" {
		delay, err := time.ParseDuration(request.Delay)
		if err != nil {
			return writeRequest{}, time.Time{}, fmt.Errorf("%w: invalid delay: %v", ErrInvalidPublish, err)
		}
		if delay < 0 {
			return writeRequest{}, time.Time{}, fmt.Errorf("%w: delay cannot be negative", ErrInvalidPublish)
		}
		scheduledAt = now.Add(delay)
		scheduleMode = scheduleModeDelay
		scheduleDelay = delay
		scheduleValue = delay.String()
	}
	if request.ScheduleAt != "" {
		parsed, err := time.Parse(time.RFC3339, request.ScheduleAt)
		if err != nil {
			return writeRequest{}, time.Time{}, fmt.Errorf("%w: schedule_at must be RFC3339 with timezone", ErrInvalidPublish)
		}
		scheduledAt = parsed.UTC()
		scheduleMode = scheduleModeAbsolute
		scheduleValue = scheduledAt.Format(time.RFC3339Nano)
	}
	headers := []byte("{}")
	if request.Headers != nil {
		var err error
		headers, err = json.Marshal(request.Headers)
		if err != nil {
			return writeRequest{}, time.Time{}, fmt.Errorf("%w: invalid headers", ErrInvalidPublish)
		}
	}
	if len(headers) > 16<<10 {
		return writeRequest{}, time.Time{}, fmt.Errorf("%w: headers exceed 16KiB", ErrInvalidPublish)
	}
	messageID := newMessageIDAt(now)
	idempotencyHash := ""
	if request.IdempotencyKey != "" {
		messageID = uuid.NewSHA1(topicID, []byte(request.IdempotencyKey)).String()
		fingerprint, err := json.Marshal(struct {
			Message       string          `json:"message"`
			Headers       json.RawMessage `json:"headers"`
			CorrelationID string          `json:"correlation_id"`
			Priority      int             `json:"priority"`
			ScheduleMode  string          `json:"schedule_mode"`
			ScheduleValue string          `json:"schedule_value"`
		}{
			Message: request.Message, Headers: json.RawMessage(headers),
			CorrelationID: request.CorrelationID, Priority: request.Priority,
			ScheduleMode: scheduleMode, ScheduleValue: scheduleValue,
		})
		if err != nil {
			return writeRequest{}, time.Time{}, fmt.Errorf("%w: fingerprint message", ErrInvalidPublish)
		}
		sum := sha256.Sum256(fingerprint)
		idempotencyHash = fmt.Sprintf("%x", sum[:])
	}
	return writeRequest{
		TopicID:         topicID,
		MessageID:       messageID,
		Message:         request.Message,
		Headers:         headers,
		CorrelationID:   request.CorrelationID,
		IdempotencyKey:  request.IdempotencyKey,
		IdempotencyHash: idempotencyHash,
		ScheduleMode:    scheduleMode,
		ScheduleDelay:   scheduleDelay,
		Priority:        request.Priority,
		VisibleAt:       scheduledAt,
		CreatedAt:       now,
	}, scheduledAt, nil
}

// newMessageID creates a UUIDv7-compatible, time-ordered identifier. Keeping
// newly published IDs close in the ACK index avoids random B-tree writes and
// substantially reduces SQLite WAL amplification compared with UUIDv4.
func newMessageID() string {
	return newMessageIDAt(time.Now())
}

func newMessageIDAt(now time.Time) string {
	id := uuid.New()
	milliseconds := uint64(now.UnixMilli())
	id[0] = byte(milliseconds >> 40)
	id[1] = byte(milliseconds >> 32)
	id[2] = byte(milliseconds >> 24)
	id[3] = byte(milliseconds >> 16)
	id[4] = byte(milliseconds >> 8)
	id[5] = byte(milliseconds)
	id[6] = (id[6] & 0x0f) | 0x70
	return id.String()
}
