package blockqueue

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

var (
	ErrWriterClosed          = persistence.ErrWriterClosed
	ErrPendingBudgetExceeded = errors.New("pending write budget exceeded")
	ErrWriterDrainTimeout    = errors.New("writer shutdown with unpersisted messages")
	ErrIdempotencyConflict   = persistence.ErrIdempotencyConflict
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

	postgresClassConnectionException  = "08"
	postgresClassTransactionRollback  = "40"
	postgresClassInsufficientResource = "53"
	postgresClassSystemError          = "58"
	postgresLockNotAvailable          = "55P03"
	postgresAdminShutdown             = "57P01"
	postgresCrashShutdown             = "57P02"
	postgresCannotConnectNow          = "57P03"
)

var transientWriteErrorFragments = []string{
	"database is locked",
	"database table is locked",
	"connection reset",
	"broken pipe",
	"connection refused",
	"server closed",
}

// writeRequest is the canonical message admitted to the in-memory writer.
// Its storage representation lives with the persistence implementation.
type writeRequest = persistence.WriteRequest

func writeRequestWeight(r writeRequest) int64 {
	return int64(len(r.Message) + len(r.Headers) + len(r.CorrelationID) + len(r.IdempotencyKey) + len(r.MessageID) + 128)
}

type writeOutcome struct {
	result persistWriteResult
	err    error
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
	persist            func(context.Context, []writeRequest) (persistWriteResult, error)
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
	persist       func(context.Context, []writeRequest) (persistWriteResult, error)
	metricID      uint64

	maxMessages  int64
	maxBytes     int64
	pendingMsgs  atomic.Int64
	pendingBytes atomic.Int64
	budgetMu     sync.Mutex

	admissionMu      sync.Mutex
	admissionSenders sync.WaitGroup
	accepting        atomic.Bool
	stop             chan struct{}
	abort            chan struct{} // only for process teardown after a failed shutdown
	persistCtx       context.Context
	persistStop      context.CancelFunc
	done             chan struct{}
	stopOnce         sync.Once
	abortOnce        sync.Once

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
		persist:       config.persist,
		maxMessages:   config.MaxPendingMessages,
		maxBytes:      config.MaxPendingBytes,
		stop:          make(chan struct{}),
		abort:         make(chan struct{}),
		persistCtx:    persistCtx,
		persistStop:   persistStop,
		done:          make(chan struct{}),
	}
	if w.persist == nil {
		w.persist = database.persistWriteRequests
	}
	if !database.disableMetrics {
		w.metricID = metric.RegisterWriter()
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
	result, err := w.waitAdmission(ctx, admission)
	return result.Duplicates, err
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
		bytes += writeRequestWeight(owned[i])
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

	if !w.beginAdmissionSend() {
		w.release(admission.messages, admission.bytes)
		return nil, ErrWriterClosed
	}
	defer w.admissionSenders.Done()
	select {
	case w.queue <- admission:
		if !w.db.disableMetrics {
			metric.PublishResults.WithLabelValues(metric.PublishResultAdmitted).Add(float64(len(owned)))
		}
	case <-ctx.Done():
		w.release(admission.messages, admission.bytes)
		return nil, ctx.Err()
	case <-w.stop:
		w.release(admission.messages, admission.bytes)
		return nil, ErrWriterClosed
	}
	return admission, nil
}

func (w *writer) waitAdmission(ctx context.Context, admission *writeAdmission) (persistWriteResult, error) {
	if admission == nil || admission.result == nil {
		return persistWriteResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case outcome := <-admission.result:
		return outcome.result, outcome.err
	case <-ctx.Done():
		// Admission remains owned by the writer even if its waiter leaves.
		ids := make([]string, len(admission.requests))
		for index, request := range admission.requests {
			ids[index] = request.MessageID
		}
		return persistWriteResult{}, &CommitUnknownError{MessageIDs: ids, Cause: ctx.Err()}
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
		metric.PendingMessages.Add(float64(messages))
		metric.PendingBytes.Add(float64(bytes))
	}
	return nil
}

func (w *writer) release(messages, bytes int64) {
	w.budgetMu.Lock()
	w.pendingMsgs.Add(-messages)
	w.pendingBytes.Add(-bytes)
	if !w.db.disableMetrics {
		metric.PendingMessages.Add(-float64(messages))
		metric.PendingBytes.Add(-float64(bytes))
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
		metric.SetWriterHealth(w.metricID, healthy)
	}
	w.lastErrMu.Lock()
	w.lastErr = err
	w.lastErrMu.Unlock()
}

// Barrier waits until every admission accepted before it has committed.
func (w *writer) Barrier(ctx context.Context) error {
	barrier := &writeAdmission{barrier: make(chan error, 1)}
	if !w.beginAdmissionSend() {
		return ErrWriterClosed
	}
	select {
	case w.queue <- barrier:
		w.admissionSenders.Done()
	case <-ctx.Done():
		w.admissionSenders.Done()
		return ctx.Err()
	case <-w.stop:
		w.admissionSenders.Done()
		return ErrWriterClosed
	}
	select {
	case err := <-barrier.barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *writer) beginAdmissionSend() bool {
	w.admissionMu.Lock()
	defer w.admissionMu.Unlock()
	if !w.accepting.Load() {
		return false
	}
	// Add is serialized with initiateClose. Once accepting becomes false,
	// writer.run may Wait without racing another Add.
	w.admissionSenders.Add(1)
	return true
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
		if !w.db.disableMetrics {
			metric.UnregisterWriter(w.metricID)
		}
		close(w.done)
	}()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	pending := make([]*writeAdmission, 0, w.batchSize)
	pendingMessages := 0
	pendingDurable := false
	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		ok := w.flush(pending)
		pending = pending[:0]
		pendingMessages = 0
		pendingDurable = false
		return ok
	}
	processAdmission := func(admission *writeAdmission) (ok, groupComplete bool) {
		if admission.barrier != nil {
			if flush() {
				admission.barrier <- nil
				return true, true
			}
			admission.barrier <- ErrWriterDrainTimeout
			return false, true
		}
		pending = append(pending, admission)
		pendingMessages += len(admission.requests)
		pendingDurable = pendingDurable || admission.result != nil
		if pendingMessages >= w.batchSize {
			return flush(), true
		}
		return true, false
	}

	for {
		select {
		case admission := <-w.queue:
			ok, complete := processAdmission(admission)
			if !ok {
				return
			}
			// Coalesce everything already admitted before touching storage. When
			// the channel drains, flush immediately only if a durable caller is
			// waiting. Async-only cohorts retain the configured group-commit
			// window; flushing those eagerly collapses HTTP ingress throughput.
			for !complete {
				select {
				case admission = <-w.queue:
					ok, complete = processAdmission(admission)
					if !ok {
						return
					}
				default:
					if pendingDurable {
						if !flush() {
							return
						}
					}
					complete = true
				}
			}
		case <-ticker.C:
			if !flush() {
				return
			}
		case <-w.stop:
			// A sender registered before stop may observe both a writable queue
			// and the closed stop channel and choose either select branch. Wait
			// for every such sender before the final drain so no admission can
			// arrive after the writer exits.
			w.admissionSenders.Wait()
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

	result, err, aborted := w.persistWithRetry(requests)
	if aborted {
		w.completeAdmissions(admissions, persistWriteResult{}, ErrWriterDrainTimeout, started)
		return false
	}
	if err == nil {
		w.completeAdmissions(admissions, result, nil, started)
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
		for index, admission := range admissions {
			result, admissionErr, aborted := w.persistWithRetry(admission.requests)
			if aborted {
				w.completeAdmissions(admissions[index:], persistWriteResult{}, ErrWriterDrainTimeout, started)
				return false
			}
			w.completeAdmissions([]*writeAdmission{admission}, result, admissionErr, started)
		}
		return true
	}

	w.completeAdmissions(admissions, persistWriteResult{}, err, started)
	return true
}

// persistWithRetry retries only errors that the backend classifies as
// transient. Validation, constraint, and idempotency failures are returned to
// the owning admission immediately instead of making the writer unhealthy
// forever.
func (w *writer) persistWithRetry(requests []writeRequest) (persistWriteResult, error, bool) {
	backoff := w.retryMin
	for {
		select {
		case <-w.abort:
			return persistWriteResult{}, ErrWriterDrainTimeout, true
		default:
		}
		result, err := w.persist(w.persistCtx, requests)
		if err == nil {
			w.setHealth(true, nil)
			return result, nil, false
		}
		if w.persistCtx.Err() != nil {
			return persistWriteResult{}, ErrWriterDrainTimeout, true
		}
		if !isTransientWriteError(err) {
			if isDomainWriteError(err) {
				// A definitive application-level response proves the database is
				// reachable and must not leave stale infrastructure health behind.
				w.setHealth(true, nil)
			} else {
				// Unknown permanent storage failures are fatal for this writer.
				// Stop new admission so HTTP/embedded callers observe 503/closed
				// instead of accepting async work that cannot be persisted.
				w.setHealth(false, err)
				w.initiateClose()
			}
			return persistWriteResult{}, err, false
		}

		w.setHealth(false, err)
		if !w.db.disableMetrics {
			metric.FlushTotal.WithLabelValues(metric.OutcomeRetry).Inc()
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
			return persistWriteResult{}, err, true
		}
	}
}

func (w *writer) completeAdmissions(admissions []*writeAdmission, result persistWriteResult, outcomeErr error, started time.Time) {
	requestCount := 0
	duplicateCount := 0
	uniqueTopics := make(map[uuid.UUID]struct{})
	for _, duplicate := range result.Duplicates {
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
		result := metric.OutcomeSuccess
		if outcomeErr != nil {
			result = metric.OutcomeFailed
		}
		metric.FlushTotal.WithLabelValues(result).Inc()
		metric.FlushSize.Observe(float64(requestCount))
		metric.FlushDuration.Observe(time.Since(started).Seconds())
		if outcomeErr == nil {
			metric.PublishResults.WithLabelValues(metric.PublishResultPersisted).Add(float64(requestCount))
			metric.PublishResults.WithLabelValues(metric.PublishResultDuplicate).Add(float64(duplicateCount))
		} else {
			metric.PublishResults.WithLabelValues(metric.OutcomeFailed).Add(float64(requestCount))
		}
	}

	offset := 0
	for _, admission := range admissions {
		n := len(admission.requests)
		if admission.result != nil {
			outcome := writeOutcome{err: outcomeErr}
			if outcomeErr == nil {
				outcome.result.Duplicates = append([]bool(nil), result.Duplicates[offset:offset+n]...)
				outcome.result.ScheduledAt = append([]time.Time(nil), result.ScheduledAt[offset:offset+n]...)
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
			metric.SetPersistenceLag(w.metricID, time.Since(oldest).Seconds())
		}
		if w.notify != nil {
			for topicID := range uniqueTopics {
				w.notify(topicID)
			}
		}
	}
	if !w.db.disableMetrics && w.pendingMsgs.Load() == 0 {
		metric.SetPersistenceLag(w.metricID, 0)
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
				w.completeAdmissions(queued, persistWriteResult{}, outcomeErr, time.Now())
			}
			return
		}
	}
}

func isDomainWriteError(err error) bool {
	return errors.Is(err, ErrInvalidPublish) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrNoActiveSubscriber) ||
		errors.Is(err, ErrTopicNotFound)
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
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return strings.HasPrefix(code, postgresClassConnectionException) ||
			strings.HasPrefix(code, postgresClassTransactionRollback) ||
			strings.HasPrefix(code, postgresClassInsufficientResource) ||
			code == postgresLockNotAvailable || code == postgresAdminShutdown ||
			code == postgresCrashShutdown || code == postgresCannotConnectNow ||
			strings.HasPrefix(code, postgresClassSystemError)
	}
	if sqliteCode, ok := sqlitePrimaryErrorCode(err); ok {
		switch sqliteCode {
		case sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrIoErr, sqlite3.ErrInterrupt,
			sqlite3.ErrProtocol, sqlite3.ErrFull, sqlite3.ErrNomem:
			return true
		default:
			return false
		}
	}
	lower := strings.ToLower(err.Error())
	for _, fragment := range transientWriteErrorFragments {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func isAmbiguousCommitError(err error) bool {
	// Once Commit has been invoked, a context cancellation or deadline cannot
	// prove whether the server committed before the client stopped waiting.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return strings.HasPrefix(code, postgresClassConnectionException) ||
			strings.HasPrefix(code, postgresClassSystemError) ||
			code == postgresAdminShutdown || code == postgresCrashShutdown || code == postgresCannotConnectNow
	}
	if sqliteCode, ok := sqlitePrimaryErrorCode(err); ok {
		return sqliteCode == sqlite3.ErrIoErr || sqliteCode == sqlite3.ErrProtocol
	}
	lower := strings.ToLower(err.Error())
	for _, fragment := range transientWriteErrorFragments {
		if fragment != "database is locked" && fragment != "database table is locked" &&
			strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

// sqlitePrimaryErrorCode accepts every error shape exposed by go-sqlite3:
// value or pointer Error structs as well as bare primary/extended result codes.
// Extended result codes retain the primary code in their low byte.
func sqlitePrimaryErrorCode(err error) (sqlite3.ErrNo, bool) {
	var value sqlite3.Error
	if errors.As(err, &value) {
		return value.Code, true
	}
	var pointer *sqlite3.Error
	if errors.As(err, &pointer) && pointer != nil {
		return pointer.Code, true
	}
	var primary sqlite3.ErrNo
	if errors.As(err, &primary) {
		return primary, true
	}
	var extended sqlite3.ErrNoExtended
	if errors.As(err, &extended) {
		return sqlite3.ErrNo(int(extended) & 0xff), true
	}
	return 0, false
}
