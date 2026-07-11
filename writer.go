package blockqueue

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

var (
	ErrWriterClosed          = errors.New("writer closed")
	ErrPendingBudgetExceeded = errors.New("pending write budget exceeded")
	ErrWriterDrainTimeout    = errors.New("writer shutdown with unpersisted messages")
	ErrIdempotencyConflict   = errors.New("idempotency key conflicts with a different message")
	ErrCommitUnknown         = errors.New("publish commit outcome unknown")
)

// CommitUnknownError means the caller stopped waiting after admission. The
// writer still owns the messages and may already have committed them; callers
// can safely reconcile or retry using the included stable IDs/idempotency keys.
type CommitUnknownError struct {
	MessageIDs []string
	Cause      error
}

func (err *CommitUnknownError) Error() string {
	return fmt.Sprintf("publish commit outcome unknown for %d message(s): %v", len(err.MessageIDs), err.Cause)
}

func (err *CommitUnknownError) Unwrap() error { return err.Cause }

func (err *CommitUnknownError) Is(target error) bool {
	return target == ErrCommitUnknown || errors.Is(err.Cause, target)
}

const (
	defaultMaxPendingMessages int64 = 100_000
	defaultMaxPendingBytes    int64 = 256 << 20
)

// writeRequest is the canonical message admitted to the in-memory writer.
type writeRequest struct {
	TopicID        uuid.UUID
	MessageID      string
	Message        string
	Headers        []byte
	CorrelationID  string
	IdempotencyKey string
	Priority       int
	VisibleAt      time.Time
	CreatedAt      time.Time
}

func (r writeRequest) weight() int64 {
	return int64(len(r.Message) + len(r.Headers) + len(r.CorrelationID) + len(r.IdempotencyKey) + len(r.MessageID) + 128)
}

type writeOutcome struct {
	duplicates []bool
	err        error
}

type writeAdmission struct {
	requests []writeRequest
	result   chan writeOutcome
	barrier  chan error
	messages int64
	bytes    int64
}

// WriterOptions controls batching and the weighted pending budget.
type WriterOptions struct {
	BatchSize          int
	FlushInterval      time.Duration
	MaxPendingMessages int64
	MaxPendingBytes    int64
	RetryMin           time.Duration
	RetryMax           time.Duration
	notify             func(topicID uuid.UUID)
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		BatchSize:          100,
		FlushInterval:      50 * time.Millisecond,
		MaxPendingMessages: defaultMaxPendingMessages,
		MaxPendingBytes:    defaultMaxPendingBytes,
		RetryMin:           10 * time.Millisecond,
		RetryMax:           time.Second,
		notify:             func(uuid.UUID) {},
	}
}

// writer never releases a reservation until the corresponding database
// transaction commits. A failed flush remains owned by the writer and is
// retried with bounded exponential backoff.
type writer struct {
	db            *db
	queue         chan *writeAdmission
	batchSize     int
	flushInterval time.Duration
	retryMin      time.Duration
	retryMax      time.Duration
	notify        func(uuid.UUID)

	maxMessages  int64
	maxBytes     int64
	pendingMsgs  atomic.Int64
	pendingBytes atomic.Int64
	budgetMu     sync.Mutex

	admissionMu sync.RWMutex
	accepting   atomic.Bool
	stop        chan struct{}
	abort       chan struct{} // only for process teardown after a failed shutdown
	persistCtx  context.Context
	persistStop context.CancelFunc
	done        chan struct{}
	stopOnce    sync.Once
	abortOnce   sync.Once

	healthy   atomic.Bool
	lastErrMu sync.RWMutex
	lastErr   error
}

func newWriter(ctx context.Context, database *db, config WriterOptions) *writer {
	defaults := DefaultWriterOptions()
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = defaults.FlushInterval
	}
	if config.MaxPendingMessages <= 0 {
		config.MaxPendingMessages = defaultMaxPendingMessages
	}
	if config.MaxPendingBytes <= 0 {
		config.MaxPendingBytes = defaults.MaxPendingBytes
	}
	if config.RetryMin <= 0 {
		config.RetryMin = defaults.RetryMin
	}
	if config.RetryMax <= 0 {
		config.RetryMax = defaults.RetryMax
	}
	if config.RetryMax < config.RetryMin {
		config.RetryMax = config.RetryMin
	}
	queueSize := int(min(config.MaxPendingMessages, int64(100_000)))
	if queueSize < 1 {
		queueSize = 1
	}

	persistCtx, persistStop := context.WithCancel(context.Background())
	w := &writer{
		db:            database,
		queue:         make(chan *writeAdmission, queueSize),
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		retryMin:      config.RetryMin,
		retryMax:      config.RetryMax,
		notify:        config.notify,
		maxMessages:   config.MaxPendingMessages,
		maxBytes:      config.MaxPendingBytes,
		stop:          make(chan struct{}),
		abort:         make(chan struct{}),
		persistCtx:    persistCtx,
		persistStop:   persistStop,
		done:          make(chan struct{}),
	}
	w.accepting.Store(true)
	w.healthy.Store(true)
	go w.run()
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				w.initiateClose()
			case <-w.done:
			}
		}()
	}
	return w
}

func (w *writer) Enqueue(topicID uuid.UUID, messageID, message string, delay time.Duration) {
	_ = w.EnqueueContext(context.Background(), topicID, messageID, message, delay)
}

func (w *writer) EnqueueContext(ctx context.Context, topicID uuid.UUID, messageID, message string, delay time.Duration) error {
	return w.enqueueAtContext(ctx, topicID, messageID, message, time.Now().UTC().Add(delay))
}

func (w *writer) enqueueAtContext(ctx context.Context, topicID uuid.UUID, messageID, message string, visibleAt time.Time) error {
	return w.EnqueueBatchContext(ctx, []writeRequest{{
		TopicID: topicID, MessageID: messageID, Message: message, VisibleAt: visibleAt,
	}})
}

func (w *writer) EnqueueBatchContext(ctx context.Context, requests []writeRequest) error {
	_, err := w.enqueue(ctx, requests, false)
	return err
}

func (w *writer) EnqueueBatchDurable(ctx context.Context, requests []writeRequest) ([]bool, error) {
	return w.enqueue(ctx, requests, true)
}

func (w *writer) enqueue(ctx context.Context, requests []writeRequest, durable bool) ([]bool, error) {
	admission, err := w.admitOwned(ctx, append([]writeRequest(nil), requests...), durable)
	if err != nil {
		return nil, err
	}
	return w.waitAdmission(ctx, admission)
}

func (w *writer) admitOne(ctx context.Context, request writeRequest, durable bool) (*writeAdmission, error) {
	return w.admitOwned(ctx, []writeRequest{request}, durable)
}

func (w *writer) admitBatch(ctx context.Context, requests []writeRequest, durable bool) (*writeAdmission, error) {
	return w.admitOwned(ctx, append([]writeRequest(nil), requests...), durable)
}

func (w *writer) admitOwned(ctx context.Context, owned []writeRequest, durable bool) (*writeAdmission, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(owned) == 0 {
		return &writeAdmission{}, nil
	}
	now := time.Now().UTC()
	var bytes int64
	for i := range owned {
		if len(owned[i].Headers) == 0 {
			owned[i].Headers = []byte("{}")
		}
		if owned[i].CreatedAt.IsZero() {
			owned[i].CreatedAt = now
		}
		bytes += owned[i].weight()
	}
	if err := w.reserve(int64(len(owned)), bytes); err != nil {
		return nil, err
	}

	admission := &writeAdmission{
		requests: owned,
		messages: int64(len(owned)),
		bytes:    bytes,
	}
	if durable {
		admission.result = make(chan writeOutcome, 1)
	}

	w.admissionMu.RLock()
	if !w.accepting.Load() {
		w.admissionMu.RUnlock()
		w.release(admission.messages, admission.bytes)
		return nil, ErrWriterClosed
	}
	select {
	case w.queue <- admission:
		w.admissionMu.RUnlock()
		if !w.db.disableMetrics {
			metric.PublishResults.WithLabelValues("admitted").Add(float64(len(owned)))
		}
	case <-ctx.Done():
		w.admissionMu.RUnlock()
		w.release(admission.messages, admission.bytes)
		return nil, ctx.Err()
	case <-w.stop:
		w.admissionMu.RUnlock()
		w.release(admission.messages, admission.bytes)
		return nil, ErrWriterClosed
	}
	return admission, nil
}

func (w *writer) waitAdmission(ctx context.Context, admission *writeAdmission) ([]bool, error) {
	if admission == nil || admission.result == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case outcome := <-admission.result:
		return outcome.duplicates, outcome.err
	case <-ctx.Done():
		// Admission remains owned by the writer even if its waiter leaves.
		ids := make([]string, len(admission.requests))
		for index, request := range admission.requests {
			ids[index] = request.MessageID
		}
		return nil, &CommitUnknownError{MessageIDs: ids, Cause: ctx.Err()}
	}
}

func (w *writer) reserve(messages, bytes int64) error {
	w.budgetMu.Lock()
	defer w.budgetMu.Unlock()
	if messages > w.maxMessages || bytes > w.maxBytes ||
		w.pendingMsgs.Load()+messages > w.maxMessages ||
		w.pendingBytes.Load()+bytes > w.maxBytes {
		return ErrPendingBudgetExceeded
	}
	w.pendingMsgs.Add(messages)
	w.pendingBytes.Add(bytes)
	if !w.db.disableMetrics {
		metric.PendingMessages.Set(float64(w.pendingMsgs.Load()))
		metric.PendingBytes.Set(float64(w.pendingBytes.Load()))
	}
	return nil
}

func (w *writer) release(messages, bytes int64) {
	w.budgetMu.Lock()
	w.pendingMsgs.Add(-messages)
	w.pendingBytes.Add(-bytes)
	if !w.db.disableMetrics {
		metric.PendingMessages.Set(float64(w.pendingMsgs.Load()))
		metric.PendingBytes.Set(float64(w.pendingBytes.Load()))
	}
	w.budgetMu.Unlock()
}

func (w *writer) Pending() (messages, bytes int64) {
	return w.pendingMsgs.Load(), w.pendingBytes.Load()
}

func (w *writer) Healthy() bool { return w.healthy.Load() }

func (w *writer) LastError() error {
	w.lastErrMu.RLock()
	defer w.lastErrMu.RUnlock()
	return w.lastErr
}

func (w *writer) setHealth(healthy bool, err error) {
	w.healthy.Store(healthy)
	if !w.db.disableMetrics {
		if healthy {
			metric.WriterHealthy.Set(1)
		} else {
			metric.WriterHealthy.Set(0)
		}
	}
	w.lastErrMu.Lock()
	w.lastErr = err
	w.lastErrMu.Unlock()
}

// Barrier waits until every admission accepted before it has committed.
func (w *writer) Barrier(ctx context.Context) error {
	barrier := &writeAdmission{barrier: make(chan error, 1)}
	w.admissionMu.RLock()
	if !w.accepting.Load() {
		w.admissionMu.RUnlock()
		return ErrWriterClosed
	}
	select {
	case w.queue <- barrier:
		w.admissionMu.RUnlock()
	case <-ctx.Done():
		w.admissionMu.RUnlock()
		return ctx.Err()
	case <-w.stop:
		w.admissionMu.RUnlock()
		return ErrWriterClosed
	}
	select {
	case err := <-barrier.barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *writer) initiateClose() {
	w.stopOnce.Do(func() {
		w.admissionMu.Lock()
		w.accepting.Store(false)
		close(w.stop)
		w.admissionMu.Unlock()
	})
}

func (w *writer) Close() {
	_ = w.CloseContext(context.Background())
}

func (w *writer) CloseContext(ctx context.Context) error {
	w.initiateClose()
	select {
	case <-w.done:
		if messages, _ := w.Pending(); messages != 0 {
			return ErrWriterDrainTimeout
		}
		return nil
	case <-ctx.Done():
		w.abortWrites()
		return fmt.Errorf("%w: %d messages remain: %v", ErrWriterDrainTimeout, w.pendingMsgs.Load(), ctx.Err())
	}
}

func (w *writer) abortWrites() {
	w.abortOnce.Do(func() {
		close(w.abort)
		w.persistStop()
	})
}

func (w *writer) run() {
	defer func() {
		w.persistStop()
		w.failQueuedAdmissions(ErrWriterDrainTimeout)
		close(w.done)
	}()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	pending := make([]*writeAdmission, 0, w.batchSize)
	pendingMessages := 0
	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		ok := w.flush(pending)
		pending = pending[:0]
		pendingMessages = 0
		return ok
	}

	for {
		select {
		case admission := <-w.queue:
			if admission.barrier != nil {
				if flush() {
					admission.barrier <- nil
				} else {
					admission.barrier <- ErrWriterDrainTimeout
					return
				}
				continue
			}
			pending = append(pending, admission)
			pendingMessages += len(admission.requests)
			if pendingMessages >= w.batchSize && !flush() {
				return
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		case <-w.stop:
			for {
				select {
				case admission := <-w.queue:
					if admission.barrier != nil {
						if flush() {
							admission.barrier <- nil
						}
						continue
					}
					pending = append(pending, admission)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (w *writer) flush(admissions []*writeAdmission) bool {
	started := time.Now()
	requests := make([]writeRequest, 0)
	for _, admission := range admissions {
		requests = append(requests, admission.requests...)
	}

	duplicates, err, aborted := w.persistWithRetry(requests)
	if aborted {
		w.completeAdmissions(admissions, nil, ErrWriterDrainTimeout, started)
		return false
	}
	if err == nil {
		w.completeAdmissions(admissions, duplicates, nil, started)
		return true
	}

	// A permanent error in an aggregated flush must not poison unrelated
	// admissions. Retry each admission independently. Each admission remains
	// transactionally all-or-nothing, while valid neighbours can still commit.
	if len(admissions) > 1 {
		slog.Warn("isolating permanently failed write admission",
			"error", err,
			"admissions", len(admissions),
			"batch_size", len(requests),
		)
		for _, admission := range admissions {
			duplicates, admissionErr, aborted := w.persistWithRetry(admission.requests)
			if aborted {
				return false
			}
			w.completeAdmissions([]*writeAdmission{admission}, duplicates, admissionErr, started)
		}
		return true
	}

	w.completeAdmissions(admissions, nil, err, started)
	return true
}

// persistWithRetry retries only errors that the backend classifies as
// transient. Validation, constraint, and idempotency failures are returned to
// the owning admission immediately instead of making the writer unhealthy
// forever.
func (w *writer) persistWithRetry(requests []writeRequest) ([]bool, error, bool) {
	backoff := w.retryMin
	for {
		select {
		case <-w.abort:
			return nil, ErrWriterDrainTimeout, true
		default:
		}
		duplicates, err := w.db.persistWriteRequests(w.persistCtx, requests)
		if err == nil {
			w.setHealth(true, nil)
			return duplicates, nil, false
		}
		if w.persistCtx.Err() != nil {
			return nil, ErrWriterDrainTimeout, true
		}
		if !isTransientWriteError(err) {
			return nil, err, false
		}

		w.setHealth(false, err)
		if !w.db.disableMetrics {
			metric.FlushTotal.WithLabelValues("retry").Inc()
		}
		slog.Error("writer flush failed; retaining batch for retry",
			"error", err,
			"batch_size", len(requests),
			"retry_in", backoff,
		)
		jitter := time.Duration(rand.Int63n(max(int64(backoff/2), 1)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-timer.C:
			backoff = min(backoff*2, w.retryMax)
		case <-w.abort:
			if !timer.Stop() {
				<-timer.C
			}
			return nil, err, true
		}
	}
}

func (w *writer) completeAdmissions(admissions []*writeAdmission, duplicates []bool, outcomeErr error, started time.Time) {
	requestCount := 0
	duplicateCount := 0
	uniqueTopics := make(map[uuid.UUID]struct{})
	for _, duplicate := range duplicates {
		if duplicate {
			duplicateCount++
		}
	}
	for _, admission := range admissions {
		requestCount += len(admission.requests)
		if outcomeErr == nil {
			for _, request := range admission.requests {
				uniqueTopics[request.TopicID] = struct{}{}
			}
		}
	}
	if outcomeErr != nil && !errors.Is(outcomeErr, ErrWriterDrainTimeout) {
		for _, admission := range admissions {
			if admission.result != nil {
				continue
			}
			firstMessageID := ""
			if len(admission.requests) > 0 {
				firstMessageID = admission.requests[0].MessageID
			}
			slog.Error("writer permanently rejected admission",
				"error", outcomeErr,
				"message_count", len(admission.requests),
				"first_message_id", firstMessageID,
				"durable", false,
			)
		}
	}

	if !w.db.disableMetrics {
		result := "success"
		if outcomeErr != nil {
			result = "failed"
		}
		metric.FlushTotal.WithLabelValues(result).Inc()
		metric.FlushSize.Observe(float64(requestCount))
		metric.FlushDuration.Observe(time.Since(started).Seconds())
		if outcomeErr == nil {
			metric.PublishResults.WithLabelValues("persisted").Add(float64(requestCount))
			metric.PublishResults.WithLabelValues("duplicate").Add(float64(duplicateCount))
		} else {
			metric.PublishResults.WithLabelValues("failed").Add(float64(requestCount))
		}
	}

	offset := 0
	for _, admission := range admissions {
		n := len(admission.requests)
		if admission.result != nil {
			outcome := writeOutcome{err: outcomeErr}
			if outcomeErr == nil {
				outcome.duplicates = append([]bool(nil), duplicates[offset:offset+n]...)
			}
			admission.result <- outcome
		}
		w.release(admission.messages, admission.bytes)
		offset += n
	}

	if outcomeErr == nil {
		oldest := time.Now().UTC()
		for _, admission := range admissions {
			for _, request := range admission.requests {
				if request.CreatedAt.Before(oldest) {
					oldest = request.CreatedAt
				}
			}
		}
		if !w.db.disableMetrics {
			metric.PersistenceLag.Set(time.Since(oldest).Seconds())
		}
		if w.notify != nil {
			for topicID := range uniqueTopics {
				w.notify(topicID)
			}
		}
	}
	if !w.db.disableMetrics && w.pendingMsgs.Load() == 0 {
		metric.PersistenceLag.Set(0)
	}
}

func (w *writer) failQueuedAdmissions(outcomeErr error) {
	queued := make([]*writeAdmission, 0)
	for {
		select {
		case admission := <-w.queue:
			if admission.barrier != nil {
				admission.barrier <- outcomeErr
				continue
			}
			queued = append(queued, admission)
		default:
			if len(queued) > 0 {
				w.completeAdmissions(queued, nil, outcomeErr, time.Now())
			}
			return
		}
	}
}

func isTransientWriteError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, ErrNoActiveSubscriber) ||
		errors.Is(err, ErrTopicNotFound) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return strings.HasPrefix(code, "08") || strings.HasPrefix(code, "40") ||
			strings.HasPrefix(code, "53") || code == "55P03" || code == "57P01" ||
			code == "57P02" || code == "57P03" || strings.HasPrefix(code, "58")
	}
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) {
		switch sqliteError.Code {
		case sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrIoErr, sqlite3.ErrInterrupt, sqlite3.ErrProtocol:
			return true
		default:
			return false
		}
	}
	lower := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"database is locked", "database table is locked", "connection reset",
		"broken pipe", "connection refused", "server closed",
	} {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}
