package worker

import (
	"context"
	"errors"
	"time"

	"github.com/yudhasubki/blockqueue"
)

type completionKind uint8

const (
	completionAck completionKind = iota
	completionNack
)

type completionRequest struct {
	ctx        context.Context
	job        *Job
	kind       completionKind
	retryDelay time.Duration
	failure    error
	result     chan error
}

func (worker *Worker) completeAutomatically(
	ctx context.Context,
	job *Job,
	kind completionKind,
	retryDelay time.Duration,
	failure error,
) error {
	outcome := jobOutcomeProcessed
	if kind == completionNack {
		outcome = jobOutcomeFailed
	}
	return job.completeWith(outcome, func() error {
		if worker.batchClient == nil {
			return worker.completeIndividually(ctx, job, kind, retryDelay, failure)
		}
		request := completionRequest{
			ctx: ctx, job: job, kind: kind, retryDelay: retryDelay, failure: failure,
			result: make(chan error, 1),
		}
		select {
		case worker.completions <- request:
		case <-ctx.Done():
			return contextCause(ctx)
		}
		select {
		case err := <-request.result:
			return err
		case <-ctx.Done():
			return contextCause(ctx)
		}
	})
}

func (worker *Worker) completionLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	maximum := worker.options.CompletionBatchSize
	if maximum > worker.options.Concurrency {
		maximum = worker.options.Concurrency
	}
	for {
		var first completionRequest
		select {
		case <-ctx.Done():
			return
		case first = <-worker.completions:
		}
		batch := []completionRequest{first}
		if maximum > 1 {
			timer := time.NewTimer(worker.options.CompletionFlushInterval)
		collect:
			for len(batch) < maximum {
				select {
				case request := <-worker.completions:
					batch = append(batch, request)
				case <-timer.C:
					break collect
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					failCompletions(batch, contextCause(ctx))
					return
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		worker.flushCompletions(ctx, batch)
	}
}

func (worker *Worker) flushCompletions(ctx context.Context, batch []completionRequest) {
	acks := make([]completionRequest, 0, len(batch))
	nacks := make([]completionRequest, 0, len(batch))
	for _, request := range batch {
		if request.ctx.Err() != nil {
			request.result <- contextCause(request.ctx)
			continue
		}
		if request.kind == completionAck {
			acks = append(acks, request)
		} else {
			nacks = append(nacks, request)
		}
	}
	if len(acks) > 0 {
		worker.flushAcks(ctx, acks)
	}
	if len(nacks) > 0 {
		worker.flushNacks(ctx, nacks)
	}
}

func (worker *Worker) flushAcks(ctx context.Context, requests []completionRequest) {
	items := make([]blockqueue.BatchAckItem, len(requests))
	for index, request := range requests {
		items[index] = blockqueue.BatchAckItem{MessageID: request.job.ID, ReceiptToken: request.job.ReceiptToken}
	}
	operationContext, cancel := worker.batchContext(ctx, requests)
	results := worker.batchClient.BatchAckDeliveries(operationContext, worker.topic, worker.subscriber, items)
	cancel()
	worker.finishBatch(requests, results)
}

func (worker *Worker) flushNacks(ctx context.Context, requests []completionRequest) {
	items := make([]blockqueue.BatchNackItem, len(requests))
	for index, request := range requests {
		errorText := ErrRetryRequested.Error()
		if request.failure != nil {
			errorText = request.failure.Error()
		}
		errorText = boundedDeliveryText(errorText)
		items[index] = blockqueue.BatchNackItem{
			MessageID: request.job.ID, ReceiptToken: request.job.ReceiptToken,
			RetryDelay: request.retryDelay, Error: errorText,
		}
	}
	operationContext, cancel := worker.batchContext(ctx, requests)
	results := worker.batchClient.BatchNackDeliveries(operationContext, worker.topic, worker.subscriber, items)
	cancel()
	worker.finishBatch(requests, results)
}

func (worker *Worker) batchContext(parent context.Context, requests []completionRequest) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(worker.options.OperationTimeout)
	for _, request := range requests {
		if requestDeadline, ok := request.ctx.Deadline(); ok && requestDeadline.Before(deadline) {
			deadline = requestDeadline
		}
	}
	return context.WithDeadline(parent, deadline)
}

func (worker *Worker) finishBatch(requests []completionRequest, results []blockqueue.DeliveryResult) {
	for index, request := range requests {
		if index < len(results) && results[index].Status != "failed" {
			request.result <- nil
			continue
		}
		// Batch APIs intentionally return public per-item errors. Retry a
		// failed item through the typed single-item API so lease loss and an
		// ambiguous prior commit are fenced correctly.
		go func(request completionRequest) {
			request.result <- worker.completeIndividually(
				request.ctx, request.job, request.kind, request.retryDelay, request.failure,
			)
		}(request)
	}
}

func (worker *Worker) completeIndividually(
	ctx context.Context,
	job *Job,
	kind completionKind,
	retryDelay time.Duration,
	failure error,
) error {
	if kind == completionAck {
		return worker.retryOperation(ctx, "ack", func(operationContext context.Context) error {
			return worker.client.AckDelivery(
				operationContext, worker.topic, worker.subscriber, job.ID, job.ReceiptToken,
			)
		})
	}
	errorText := ErrRetryRequested.Error()
	if failure != nil {
		errorText = failure.Error()
	}
	errorText = boundedDeliveryText(errorText)
	return worker.retryOperation(ctx, "nack", func(operationContext context.Context) error {
		return worker.client.NackDelivery(
			operationContext, worker.topic, worker.subscriber,
			job.ID, job.ReceiptToken, retryDelay, errorText,
		)
	})
}

func failCompletions(requests []completionRequest, err error) {
	for _, request := range requests {
		request.result <- err
	}
}

func contextCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

func shouldLogCompletionError(err error, jobContext context.Context) bool {
	return err != nil && !expectedOwnershipLoss(err) &&
		context.Cause(jobContext) == nil &&
		!errors.Is(err, context.Canceled)
}
