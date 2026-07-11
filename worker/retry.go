package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yudhasubki/blockqueue"
)

type retryError struct {
	delay time.Duration
	err   error
}

type cancelError struct {
	err error
}

func (err *cancelError) Error() string {
	return err.err.Error()
}

func (err *cancelError) Unwrap() error {
	return err.err
}

func (err *retryError) Error() string {
	return err.err.Error()
}

func (err *retryError) Unwrap() error {
	return err.err
}

// RetryAfter returns a handler error that overrides the subscriber retry delay
// for this attempt. Non-positive delay falls back to the subscriber policy.
func RetryAfter(delay time.Duration, err error) error {
	if err == nil {
		err = ErrRetryRequested
	}
	if delay < 0 {
		delay = 0
	}
	return &retryError{delay: delay, err: err}
}

// CancelJob marks a handler failure as permanent. The worker receipt-fences a
// terminal cancellation instead of consuming retries or moving the job to DLQ.
func CancelJob(err error) error {
	if err == nil {
		err = ErrCancelRequested
	}
	return &cancelError{err: err}
}

func cancellationDetails(err error) (error, bool) {
	var cancellation *cancelError
	if errors.As(err, &cancellation) {
		return cancellation.err, true
	}
	return nil, false
}

func retryDetails(err error) (time.Duration, error) {
	var retry *retryError
	if errors.As(err, &retry) {
		return retry.delay, retry.err
	}
	return 0, err
}

func (worker *Worker) retryOperation(
	ctx context.Context,
	operationName string,
	operation func(context.Context) error,
) error {
	backoff := 10 * time.Millisecond
	for {
		err := operation(ctx)
		if err == nil || terminalCompletionError(err) {
			return err
		}
		worker.options.Logger.Warn("blockqueue worker completion retry",
			"operation", operationName, "error", err)
		if !waitForRetry(ctx, jitter(backoff)) {
			return fmt.Errorf("%s delivery: %w", operationName, ctx.Err())
		}
		backoff *= 2
		if backoff > 250*time.Millisecond {
			backoff = 250 * time.Millisecond
		}
	}
}

func terminalCompletionError(err error) bool {
	return expectedOwnershipLoss(err) ||
		errors.Is(err, blockqueue.ErrInvalidReceipt) ||
		errors.Is(err, blockqueue.ErrSubscriberNotFound) ||
		errors.Is(err, blockqueue.ErrSubscriberDeleted) ||
		errors.Is(err, blockqueue.ErrTopicNotFound) ||
		errors.Is(err, blockqueue.ErrQueueNotRunning) ||
		errors.Is(err, blockqueue.ErrQueueStopping)
}

func expectedOwnershipLoss(err error) bool {
	return errors.Is(err, blockqueue.ErrLeaseLost) || errors.Is(err, blockqueue.ErrDeliveryNotFound)
}
