package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

// Run claims and processes deliveries until ctx is canceled or a terminal
// topology/lifecycle error occurs. Context cancellation is a normal shutdown:
// claiming stops immediately and active handlers are given DrainTimeout to
// finish while their leases continue to heartbeat. After DrainTimeout, handler
// contexts are canceled and Run waits HardStopTimeout once more. A handler that
// ignores context cancellation can still outlive Run; such handlers violate the
// worker contract and must not access Queue after Run returns.
func (worker *Worker) Run(ctx context.Context) error {
	if !worker.state.CompareAndSwap(0, 1) {
		return ErrAlreadyStarted
	}
	defer worker.state.Store(2)
	if ctx == nil {
		ctx = context.Background()
	}

	handlerContext, cancelHandlers := context.WithCancelCause(context.WithoutCancel(ctx))
	var completerDone chan struct{}
	if worker.batchClient != nil {
		completerDone = make(chan struct{})
		go worker.completionLoop(handlerContext, completerDone)
	}
	semaphore := make(chan struct{}, worker.options.Concurrency)
	var handlers sync.WaitGroup
	runErr := worker.claimLoop(ctx, handlerContext, semaphore, &handlers)

	done := make(chan struct{})
	go func() {
		handlers.Wait()
		close(done)
	}()
	timer := time.NewTimer(worker.options.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
		cancelHandlers(errWorkerStopped)
		if !waitForRuntimeStop(nil, completerDone, true, worker.options.HardStopTimeout) {
			return errors.Join(runErr, ErrDrainTimeout)
		}
	case <-timer.C:
		cancelHandlers(ErrDrainTimeout)
		_ = waitForRuntimeStop(done, completerDone, false, worker.options.HardStopTimeout)
		return errors.Join(runErr, ErrDrainTimeout)
	}
	return runErr
}

func waitForRuntimeStop(
	handlersDone <-chan struct{},
	completerDone <-chan struct{},
	handlersStopped bool,
	timeout time.Duration,
) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for !handlersStopped || completerDone != nil {
		select {
		case <-handlersDone:
			handlersStopped = true
			handlersDone = nil
		case <-completerDone:
			completerDone = nil
		case <-timer.C:
			return false
		}
	}
	return true
}

func (worker *Worker) claimLoop(
	claimContext context.Context,
	handlerContext context.Context,
	semaphore chan struct{},
	handlers *sync.WaitGroup,
) error {
	backoff := claimRetryMinimum
	paused := false
	for {
		if claimContext.Err() != nil {
			return nil
		}
		reserved, ok := reserveSlots(claimContext, semaphore, worker.options.BatchSize)
		if !ok {
			return nil
		}
		deliveries, err := worker.client.ClaimWait(
			claimContext, worker.topic, worker.subscriber, reserved, worker.options.LeaseDuration,
		)
		if err != nil {
			for range reserved {
				<-semaphore
			}
			if claimContext.Err() != nil {
				return nil
			}
			if errors.Is(err, blockqueue.ErrResourcePaused) {
				if !paused {
					worker.options.Logger.Info("blockqueue worker paused",
						"topic", worker.topic.Name, "subscriber", worker.subscriber)
					paused = true
				}
				if !waitForRetry(claimContext, jitter(worker.options.PausePollInterval)) {
					return nil
				}
				continue
			}
			if terminalClaimError(err) {
				return err
			}
			worker.options.Logger.Warn("blockqueue worker claim failed",
				"topic", worker.topic.Name, "subscriber", worker.subscriber, "error", err)
			if !waitForRetry(claimContext, jitter(backoff)) {
				return nil
			}
			backoff *= 2
			if backoff > claimRetryMaximum {
				backoff = claimRetryMaximum
			}
			continue
		}
		if len(deliveries) > reserved {
			for range reserved {
				<-semaphore
			}
			return fmt.Errorf("%w: requested=%d returned=%d", ErrClientProtocol, reserved, len(deliveries))
		}
		for index := len(deliveries); index < reserved; index++ {
			<-semaphore
		}
		if claimContext.Err() != nil && len(deliveries) == 0 {
			return nil
		}
		if paused {
			worker.options.Logger.Info("blockqueue worker resumed",
				"topic", worker.topic.Name, "subscriber", worker.subscriber)
			paused = false
		}
		backoff = claimRetryMinimum
		for _, delivery := range deliveries {
			delivery := delivery
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer func() { <-semaphore }()
				worker.process(handlerContext, delivery)
			}()
		}
	}
}

func reserveSlots(ctx context.Context, semaphore chan struct{}, limit int) (int, bool) {
	select {
	case semaphore <- struct{}{}:
	case <-ctx.Done():
		return 0, false
	}
	reserved := 1
	for reserved < limit {
		select {
		case semaphore <- struct{}{}:
			reserved++
		default:
			return reserved, true
		}
	}
	return reserved, true
}

func (worker *Worker) process(parent context.Context, delivery blockqueue.Delivery) {
	jobContext, cancelJob := context.WithCancelCause(parent)
	job := newJob(worker.client, worker.topic, worker.subscriber, delivery)
	heartbeatDone := make(chan struct{})
	if worker.options.DisableHeartbeat {
		close(heartbeatDone)
	} else {
		go worker.heartbeat(jobContext, cancelJob, job, heartbeatDone)
	}

	handlerStarted := time.Now()
	worker.metrics.handlerStarted()
	handlerErr, panicked := safelyHandle(worker.handler, jobContext, job, worker.options.Logger)
	handlerResult := metric.WorkerHandlerResultOK
	if panicked {
		handlerResult = metric.WorkerHandlerResultPanic
	} else if _, cancelled := cancellationDetails(handlerErr); cancelled {
		handlerResult = metric.WorkerHandlerResultCancelRequested
	} else if handlerErr != nil {
		handlerResult = metric.WorkerHandlerResultError
	}
	worker.metrics.handlerFinished(handlerResult, handlerStarted)
	if job.Completed() {
		worker.metrics.jobCompleted(job.completionOutcome())
		cancelJob(errJobCompleted)
		<-heartbeatDone
		if handlerErr != nil {
			worker.options.Logger.Warn("blockqueue handler returned an error after completing its job",
				"message_id", job.ID, "error", handlerErr)
		}
		return
	}
	if context.Cause(jobContext) != nil {
		if errors.Is(context.Cause(jobContext), blockqueue.ErrLeaseLost) {
			worker.metrics.jobLeaseLost()
		}
		cancelJob(context.Cause(jobContext))
		<-heartbeatDone
		return
	}

	operationContext, cancelOperation := context.WithTimeout(jobContext, worker.options.OperationTimeout)
	var completionErr error
	if cancellation, requested := cancellationDetails(handlerErr); requested {
		completionErr = job.completeWith(jobOutcomeCancelled, func() error {
			return worker.retryOperation(operationContext, completionOperationCancel, func(ctx context.Context) error {
				return worker.client.CancelClaimedDelivery(
					ctx, worker.topic, worker.subscriber, job.ID, job.ReceiptToken,
					boundedDeliveryText(cancellation.Error()),
				)
			})
		})
		if shouldLogCompletionError(completionErr, jobContext) {
			worker.options.Logger.Error("blockqueue worker cancellation failed",
				"message_id", job.ID, "error", completionErr)
		}
	} else if handlerErr == nil {
		completionErr = worker.completeAutomatically(operationContext, job, completionAck, 0, nil)
		if shouldLogCompletionError(completionErr, jobContext) {
			worker.options.Logger.Error("blockqueue worker ACK failed",
				"message_id", job.ID, "error", completionErr)
		}
	} else {
		retryDelay, failure := retryDetails(handlerErr)
		completionErr = worker.completeAutomatically(operationContext, job, completionNack, retryDelay, failure)
		if shouldLogCompletionError(completionErr, jobContext) {
			worker.options.Logger.Error("blockqueue worker NACK failed",
				"message_id", job.ID, "error", completionErr)
		}
	}
	worker.recordCompletion(job, completionErr)
	cancelOperation()
	cancelJob(errJobCompleted)
	<-heartbeatDone
}

func safelyHandle(handler Handler, ctx context.Context, job *Job, logger *slog.Logger) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("worker handler panic: %v", recovered)
			panicked = true
			logger.Error("blockqueue worker recovered handler panic",
				"message_id", job.ID, "panic", recovered, "stack", string(debug.Stack()))
		}
	}()
	return handler.Handle(ctx, job), false
}

func (worker *Worker) recordCompletion(job *Job, err error) {
	if err == nil {
		worker.metrics.jobCompleted(job.completionOutcome())
		return
	}
	if expectedOwnershipLoss(err) {
		worker.metrics.jobLeaseLost()
		return
	}
	worker.metrics.jobCompletionFailed()
}

func terminalClaimError(err error) bool {
	return errors.Is(err, blockqueue.ErrSubscriberDeleted) ||
		errors.Is(err, blockqueue.ErrSubscriberNotFound) ||
		errors.Is(err, blockqueue.ErrTopicNotFound) ||
		errors.Is(err, blockqueue.ErrInvalidTopic) ||
		errors.Is(err, blockqueue.ErrInvalidSubscriber) ||
		errors.Is(err, blockqueue.ErrQueueNotRunning) ||
		errors.Is(err, blockqueue.ErrQueueStopping)
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func jitter(delay time.Duration) time.Duration {
	spread := delay / 5
	if spread <= 0 {
		return delay
	}
	return delay - spread + time.Duration(rand.Int63n(int64(spread*2)+1))
}
