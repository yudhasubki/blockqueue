package blockqueue

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	ErrSubscriberNotFound = errors.New("subscriber not found")
	ErrSubscriberDeleted  = errors.New("subscriber was deleted")
)

type subscriberOptions struct {
	MaxAttempts        int
	VisibilityDuration time.Duration
	DequeueBatchSize   int
	RetryInitialDelay  time.Duration
	RetryMaxDelay      time.Duration
	RetryMultiplier    float64
	RetryJitter        float64
}

// subscriberRuntime is immutable except for pause/deletion flags. It owns no
// goroutine and no claim mutex: PostgreSQL row locks and SQLite's immediate
// writer transaction are the authoritative claim serialization mechanisms.
type subscriberRuntime struct {
	id           uuid.UUID
	name         string
	paused       atomic.Bool
	deleted      atomic.Bool
	options      subscriberOptions
	deliveryWake chan struct{}
}

func newSubscriberRuntime(subscriber Subscriber) (*subscriberRuntime, error) {
	options, err := parseSubscriberOptions(subscriber)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSubscriber, err)
	}
	runtime := &subscriberRuntime{
		id:           subscriber.ID,
		name:         subscriber.Name,
		options:      options,
		deliveryWake: make(chan struct{}, 1),
	}
	runtime.paused.Store(subscriber.Paused)
	return runtime, nil
}

func parseSubscriberOptions(subscriber Subscriber) (subscriberOptions, error) {
	option := subscriber.Options.normalized()
	visibilityDuration, err := time.ParseDuration(option.VisibilityDuration)
	if err != nil || visibilityDuration <= 0 {
		return subscriberOptions{}, errors.New("subscriber visibility_duration must be positive")
	}
	if visibilityDuration > maximumDeliveryLease {
		return subscriberOptions{}, errors.New("subscriber visibility_duration cannot exceed 12h")
	}
	if visibilityDuration < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber visibility_duration must be at least 1ms")
	}
	maxAttempts := option.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	dequeueBatchSize := option.DequeueBatchSize
	if dequeueBatchSize <= 0 {
		dequeueBatchSize = 10
	}
	if dequeueBatchSize > 1000 {
		return subscriberOptions{}, errors.New("subscriber dequeue_batch_size exceeds 1000")
	}
	retryInitialDelay, err := time.ParseDuration(option.RetryPolicy.InitialDelay)
	if err != nil || retryInitialDelay < 0 {
		return subscriberOptions{}, errors.New("subscriber retry initial_delay must be non-negative")
	}
	if retryInitialDelay > 0 && retryInitialDelay < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber retry initial_delay must be zero or at least 1ms")
	}
	retryMaxDelay, err := time.ParseDuration(option.RetryPolicy.MaxDelay)
	if err != nil || retryMaxDelay < retryInitialDelay {
		return subscriberOptions{}, errors.New("subscriber retry max_delay must be at least initial_delay")
	}
	if retryMaxDelay > 0 && retryMaxDelay < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber retry max_delay must be zero or at least 1ms")
	}
	if math.IsNaN(option.RetryPolicy.Multiplier) || math.IsInf(option.RetryPolicy.Multiplier, 0) ||
		option.RetryPolicy.Multiplier < 1 {
		return subscriberOptions{}, errors.New("subscriber retry multiplier must be at least 1")
	}
	if math.IsNaN(option.RetryPolicy.Jitter) || math.IsInf(option.RetryPolicy.Jitter, 0) ||
		option.RetryPolicy.Jitter < 0 || option.RetryPolicy.Jitter > 1 {
		return subscriberOptions{}, errors.New("subscriber retry jitter must be between 0 and 1")
	}
	return subscriberOptions{
		MaxAttempts:        maxAttempts,
		VisibilityDuration: visibilityDuration,
		DequeueBatchSize:   dequeueBatchSize,
		RetryInitialDelay:  retryInitialDelay,
		RetryMaxDelay:      retryMaxDelay,
		RetryMultiplier:    option.RetryPolicy.Multiplier,
		RetryJitter:        option.RetryPolicy.Jitter,
	}, nil
}

func (subscriber *subscriberRuntime) notify() {
	if subscriber == nil || subscriber.deleted.Load() {
		return
	}
	select {
	case subscriber.deliveryWake <- struct{}{}:
	default:
	}
}
