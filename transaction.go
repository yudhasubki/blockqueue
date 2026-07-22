package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidTransaction reports a missing or otherwise unusable caller
// transaction.
var ErrInvalidTransaction = errors.New("invalid blockqueue transaction")

// ErrTransactionCommitUnknown means Commit returned a connection-level error
// for a caller-owned transaction. The database may have committed the
// application writes and queue changes; callers must reconcile and must not
// blindly repeat non-idempotent business operations.
var ErrTransactionCommitUnknown = errors.New("transaction commit outcome unknown")

// TransactionCommitUnknownError reports that a caller-owned transaction may
// have committed even though Commit returned an error.
type TransactionCommitUnknownError struct {
	Cause error
}

func (err *TransactionCommitUnknownError) Error() string {
	return fmt.Sprintf("transaction commit outcome unknown: %v", err.Cause)
}

func (err *TransactionCommitUnknownError) Unwrap() error { return err.Cause }

func (err *TransactionCommitUnknownError) Is(target error) bool {
	return target == ErrTransactionCommitUnknown || errors.Is(err.Cause, target)
}

type activeTransaction struct {
	topics map[uuid.UUID]struct{}
}

// WithTx runs fn in a transaction owned by the queue. It is the preferred way
// to atomically change application tables and publish or complete deliveries:
// the queue can notify local waiters only after commit succeeds. A context
// cancellation or deadline reported by Commit is outcome-unknown because the
// server may have committed before the client stopped waiting; fn is never
// retried.
func (q *Queue) WithTx(ctx context.Context, options *sql.TxOptions, fn func(*sql.Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return fmt.Errorf("%w: callback is required", ErrInvalidTransaction)
	}
	if err := q.reserveTransactionStart(); err != nil {
		return err
	}
	tx, err := q.db.Database.DB().BeginTx(ctx, options)
	if err != nil {
		q.transactions.Done()
		return err
	}
	q.registerTransaction(tx)
	defer q.unregisterTransaction(tx)
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil || isAmbiguousCommitError(err) {
			cause := err
			if contextErr != nil && !errors.Is(err, contextErr) {
				cause = errors.Join(err, contextErr)
			}
			return &TransactionCommitUnknownError{Cause: cause}
		}
		return err
	}
	for _, topicID := range q.transactionTopics(tx) {
		q.notify(topicID)
	}
	return nil
}

// PublishTx stages a canonical message and its fan-out in caller-owned tx.
// The caller owns commit or rollback; staged rows are not claimable before a
// successful commit.
func (q *Queue) PublishTx(ctx context.Context, tx *sql.Tx, topic Topic, request Message) (PublishReceipt, error) {
	receipts, err := q.BatchPublishTx(ctx, tx, topic, []Message{request})
	if err != nil {
		return PublishReceipt{}, err
	}
	return receipts[0], nil
}

// BatchPublishTx validates the complete batch before writing it to tx.
func (q *Queue) BatchPublishTx(ctx context.Context, tx *sql.Tx, topic Topic, requests []Message) (PublishReceipts, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx == nil {
		return nil, fmt.Errorf("%w: transaction is required", ErrInvalidTransaction)
	}
	if err := q.requireTransactionAllowed(tx); err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return PublishReceipts{}, nil
	}

	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return nil, ErrTopicNotFound
	}
	q.admissionMu.RLock()
	defer q.admissionMu.RUnlock()
	if err := q.requireTransactionAllowed(tx); err != nil {
		return nil, err
	}
	if current := q.registry.Load().byName[topic.Name]; current != runtime || runtime.deleted.Load() {
		return nil, ErrTopicNotFound
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	writes := make([]writeRequest, len(requests))
	receipts := make(PublishReceipts, len(requests))
	seenKeys := make(map[string]string)
	for index, request := range requests {
		write, scheduledAt, err := buildWriteRequest(runtime.id, request, now)
		if err != nil {
			return nil, err
		}
		if request.IdempotencyKey != "" {
			if previous, ok := seenKeys[request.IdempotencyKey]; ok && previous != write.IdempotencyHash {
				return nil, fmt.Errorf("%w: duplicate key %q has different payload", ErrInvalidPublish, request.IdempotencyKey)
			}
			seenKeys[request.IdempotencyKey] = write.IdempotencyHash
		}
		writes[index] = write
		receipts[index] = PublishReceipt{
			MessageID:   write.MessageID,
			State:       PublishStateStaged,
			ScheduledAt: scheduledAt,
		}
	}

	result, err := q.db.persistWriteRequestsWithTx(ctx, tx, writes)
	if err != nil {
		return nil, err
	}
	for index := range receipts {
		duplicate := result.Duplicates[index]
		receipts[index].Duplicate = &duplicate
		receipts[index].ScheduledAt = result.ScheduledAt[index]
	}
	q.markTransactionTopic(tx, runtime.id)
	return receipts, nil
}

func (q *Queue) requireRunning() error {
	switch q.State() {
	case LifecycleRunning:
		return nil
	case LifecycleStopping, LifecycleStopped:
		return ErrQueueStopping
	default:
		return ErrQueueNotRunning
	}
}

func (q *Queue) requireTransactionAllowed(tx *sql.Tx) error {
	if q.State() == LifecycleRunning {
		return nil
	}
	if tx != nil {
		q.transactionMu.RLock()
		_, active := q.activeTx[tx]
		q.transactionMu.RUnlock()
		if active {
			return nil
		}
	}
	return q.requireRunning()
}

func (q *Queue) registerTransaction(tx *sql.Tx) {
	q.transactionMu.Lock()
	q.activeTx[tx] = &activeTransaction{topics: make(map[uuid.UUID]struct{})}
	q.transactionMu.Unlock()
}

func (q *Queue) markTransactionTopic(tx *sql.Tx, topicID uuid.UUID) {
	if tx == nil {
		return
	}
	q.transactionMu.Lock()
	if active := q.activeTx[tx]; active != nil {
		active.topics[topicID] = struct{}{}
	}
	q.transactionMu.Unlock()
}

func (q *Queue) transactionTopics(tx *sql.Tx) []uuid.UUID {
	q.transactionMu.RLock()
	active := q.activeTx[tx]
	if active == nil {
		q.transactionMu.RUnlock()
		return nil
	}
	topics := make([]uuid.UUID, 0, len(active.topics))
	for topicID := range active.topics {
		topics = append(topics, topicID)
	}
	q.transactionMu.RUnlock()
	return topics
}

func (q *Queue) unregisterTransaction(tx *sql.Tx) {
	q.transactionMu.Lock()
	delete(q.activeTx, tx)
	q.transactionMu.Unlock()
	q.transactions.Done()
}

func (q *Queue) reserveTransactionStart() error {
	q.admissionMu.RLock()
	defer q.admissionMu.RUnlock()
	if err := q.requireRunning(); err != nil {
		return err
	}
	// Add happens behind the same lifecycle fence acquired by Shutdown before
	// Wait, satisfying sync.WaitGroup's Add-before-Wait requirement without
	// holding a mutex across a potentially blocking BeginTx call.
	q.transactions.Add(1)
	return nil
}
