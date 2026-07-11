package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yudhasubki/blockqueue"
)

var (
	errJobCompleted  = errors.New("worker job completed")
	errWorkerStopped = errors.New("worker stopped")
)

// Job is one receipt-fenced delivery attempt. Delivery is embedded so handler
// code can access Message, Headers, ID, and ReceiptToken directly.
type Job struct {
	blockqueue.Delivery

	client     Client
	topic      blockqueue.Topic
	subscriber string
	completion sync.Mutex
	outcome    atomic.Uint32
}

type jobOutcome uint32

const (
	jobOutcomeNone jobOutcome = iota
	jobOutcomeProcessed
	jobOutcomeFailed
	jobOutcomeCancelled
)

func newJob(client Client, topic blockqueue.Topic, subscriber string, delivery blockqueue.Delivery) *Job {
	return &Job{Delivery: delivery, client: client, topic: topic, subscriber: subscriber}
}

// Completed reports whether this attempt has already been ACKed, NACKed,
// cancelled, or transactionally completed through Job.
func (job *Job) Completed() bool {
	return jobOutcome(job.outcome.Load()) != jobOutcomeNone
}

// Ack manually acknowledges the job. A handler that calls Ack should return
// immediately; the worker will observe Completed and will not ACK twice.
func (job *Job) Ack(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return job.completeWith(jobOutcomeProcessed, func() error {
		return job.client.AckDelivery(ctx, job.topic, job.subscriber, job.ID, job.ReceiptToken)
	})
}

// Nack manually records a failure. A zero delay selects the subscriber retry
// policy. A handler that calls Nack should return immediately.
func (job *Job) Nack(ctx context.Context, retryDelay time.Duration, failure error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if retryDelay < 0 {
		return fmt.Errorf("%w: retry delay cannot be negative", ErrInvalidConfiguration)
	}
	errorText := ErrRetryRequested.Error()
	if failure != nil {
		errorText = failure.Error()
	}
	errorText = boundedDeliveryText(errorText)
	return job.completeWith(jobOutcomeFailed, func() error {
		return job.client.NackDelivery(
			ctx, job.topic, job.subscriber, job.ID, job.ReceiptToken, retryDelay, errorText,
		)
	})
}

// Cancel terminally cancels this exact receipt-fenced attempt. Unlike Nack,
// cancellation does not consume retry budget or enter the DLQ.
func (job *Job) Cancel(ctx context.Context, reason string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	reason = boundedDeliveryText(reason)
	return job.completeWith(jobOutcomeCancelled, func() error {
		return job.client.CancelClaimedDelivery(
			ctx, job.topic, job.subscriber, job.ID, job.ReceiptToken, reason,
		)
	})
}

// CompleteTx executes application writes and this job's ACK in one database
// transaction. The completion outcome is set only after the transaction
// commits.
func (job *Job) CompleteTx(
	ctx context.Context,
	options *sql.TxOptions,
	fn func(*sql.Tx) error,
) error {
	if fn == nil {
		return fmt.Errorf("%w: transaction callback is required", ErrInvalidConfiguration)
	}
	client, ok := job.client.(TransactionalClient)
	if !ok {
		return ErrTransactionsUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return job.completeWith(jobOutcomeProcessed, func() error {
		return client.WithTx(ctx, options, func(tx *sql.Tx) error {
			if err := fn(tx); err != nil {
				return err
			}
			return client.AckDeliveryTx(
				ctx, tx, job.topic, job.subscriber, job.ID, job.ReceiptToken,
			)
		})
	})
}

// CancelTx executes application writes and receipt-fenced cancellation in one
// database transaction. The completion outcome is set only after the
// transaction commits.
func (job *Job) CancelTx(
	ctx context.Context,
	options *sql.TxOptions,
	reason string,
	fn func(*sql.Tx) error,
) error {
	if fn == nil {
		return fmt.Errorf("%w: transaction callback is required", ErrInvalidConfiguration)
	}
	client, ok := job.client.(TransactionalClient)
	if !ok {
		return ErrTransactionsUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reason = boundedDeliveryText(reason)
	return job.completeWith(jobOutcomeCancelled, func() error {
		return client.WithTx(ctx, options, func(tx *sql.Tx) error {
			if err := fn(tx); err != nil {
				return err
			}
			return client.CancelClaimedDeliveryTx(
				ctx, tx, job.topic, job.subscriber, job.ID, job.ReceiptToken, reason,
			)
		})
	})
}

func (job *Job) completeWith(outcome jobOutcome, operation func() error) error {
	job.completion.Lock()
	defer job.completion.Unlock()
	if jobOutcome(job.outcome.Load()) != jobOutcomeNone {
		return ErrJobCompleted
	}
	if err := operation(); err != nil {
		return err
	}
	job.outcome.Store(uint32(outcome))
	return nil
}

func (job *Job) completionOutcome() jobOutcome {
	return jobOutcome(job.outcome.Load())
}
