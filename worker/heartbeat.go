package worker

import (
	"context"
	"errors"
	"hash/fnv"
	"time"

	"github.com/yudhasubki/blockqueue"
)

func (worker *Worker) heartbeat(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	job *Job,
	done chan<- struct{},
) {
	defer close(done)
	leaseDeadline := time.Now().Add(worker.options.LeaseDuration)
	heartbeatInterval := jitterHeartbeat(worker.options.HeartbeatInterval, job.ReceiptToken)
	next := heartbeatInterval
	for {
		remaining := time.Until(leaseDeadline)
		if remaining <= 0 {
			if !job.Completed() {
				worker.metrics.heartbeat("lease_lost")
				cancel(blockqueue.ErrLeaseLost)
			}
			return
		}
		if next >= remaining {
			next = remaining / 2
			if next <= 0 {
				next = time.Nanosecond
			}
		}
		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		if job.Completed() {
			return
		}

		started := time.Now()
		operationTimeout := worker.options.OperationTimeout
		if remaining := time.Until(leaseDeadline); operationTimeout > remaining {
			operationTimeout = remaining
		}
		operationContext, cancelOperation := context.WithTimeout(ctx, operationTimeout)
		_, err := worker.client.ExtendLease(
			operationContext,
			worker.topic,
			worker.subscriber,
			job.ID,
			job.ReceiptToken,
			worker.options.LeaseDuration,
		)
		cancelOperation()
		if job.Completed() {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			worker.metrics.heartbeat("success")
			// The database applies leaseDuration from its own current time. Using
			// request start is a conservative local deadline that avoids relying
			// on application/database wall clocks being identical.
			leaseDeadline = started.Add(worker.options.LeaseDuration)
			next = heartbeatInterval
			continue
		}
		if terminalHeartbeatError(err) || time.Now().After(leaseDeadline) {
			result := "failed"
			if errors.Is(err, blockqueue.ErrLeaseLost) || time.Now().After(leaseDeadline) {
				result = "lease_lost"
			}
			worker.metrics.heartbeat(result)
			cancel(err)
			return
		}
		worker.options.Logger.Warn("blockqueue worker heartbeat failed",
			"message_id", job.ID, "error", err)
		worker.metrics.heartbeat("failed")
		remaining = time.Until(leaseDeadline)
		next = 100 * time.Millisecond
		if candidate := remaining / 3; candidate < next {
			next = candidate
		}
		if next <= 0 {
			worker.metrics.heartbeat("lease_lost")
			cancel(blockqueue.ErrLeaseLost)
			return
		}
	}
}

// jitterHeartbeat spreads workers that claimed the same batch over the first
// 20% of the heartbeat window while remaining deterministic for one receipt.
func jitterHeartbeat(interval time.Duration, receipt string) time.Duration {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(receipt))
	unit := float64(hash.Sum64()%1001) / 1000
	factor := 0.8 + (unit * 0.2)
	delay := time.Duration(float64(interval) * factor)
	if delay <= 0 {
		return interval
	}
	return delay
}

func terminalHeartbeatError(err error) bool {
	return errors.Is(err, blockqueue.ErrLeaseLost) ||
		errors.Is(err, blockqueue.ErrDeliveryNotFound) ||
		errors.Is(err, blockqueue.ErrInvalidReceipt) ||
		errors.Is(err, blockqueue.ErrSubscriberNotFound) ||
		errors.Is(err, blockqueue.ErrSubscriberDeleted) ||
		errors.Is(err, blockqueue.ErrTopicNotFound) ||
		errors.Is(err, blockqueue.ErrQueueNotRunning) ||
		errors.Is(err, blockqueue.ErrQueueStopping)
}
