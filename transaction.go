package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidTransaction reports a missing or otherwise unusable caller
// transaction.
var ErrInvalidTransaction = errors.New("invalid blockqueue transaction")

// WithTx runs fn in a transaction owned by the queue. It is the preferred way
// to atomically change application tables and publish or complete deliveries:
// the queue can notify local waiters only after commit succeeds.
func (q *Queue) WithTx(ctx context.Context, options *sql.TxOptions, fn func(*sql.Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return fmt.Errorf("%w: callback is required", ErrInvalidTransaction)
	}
	q.admissionMu.RLock()
	if err := q.requireRunning(); err != nil {
		q.admissionMu.RUnlock()
		return err
	}
	tx, err := q.db.Database.DB().BeginTx(ctx, options)
	if err != nil {
		q.admissionMu.RUnlock()
		return err
	}
	q.registerTransaction(tx)
	q.admissionMu.RUnlock()
	defer q.unregisterTransaction(tx)
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	q.notifyAllTopics()
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
	seenKeys := make(map[string]Message)
	for index, request := range requests {
		write, scheduledAt, err := buildWriteRequest(runtime.id, request, now)
		if err != nil {
			return nil, err
		}
		if request.IdempotencyKey != "" {
			if previous, ok := seenKeys[request.IdempotencyKey]; ok && !samePublish(previous, request) {
				return nil, fmt.Errorf("%w: duplicate key %q has different payload", ErrInvalidPublish, request.IdempotencyKey)
			}
			seenKeys[request.IdempotencyKey] = request
		}
		writes[index] = write
		receipts[index] = PublishReceipt{
			MessageID: write.MessageID, State: PublishStateStaged, ScheduledAt: scheduledAt,
		}
	}

	duplicates, err := q.db.persistWriteRequestsWithTx(ctx, tx, writes)
	if err != nil {
		return nil, err
	}
	for index := range receipts {
		duplicate := duplicates[index]
		receipts[index].Duplicate = &duplicate
	}
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
	q.activeTx[tx] = struct{}{}
	q.transactions.Add(1)
	q.transactionMu.Unlock()
}

func (q *Queue) unregisterTransaction(tx *sql.Tx) {
	q.transactionMu.Lock()
	delete(q.activeTx, tx)
	q.transactionMu.Unlock()
	q.transactions.Done()
}

func (q *Queue) notifyAllTopics() {
	for _, topic := range q.registry.Load().byID {
		topic.notify()
	}
}
