package blockqueue

import (
	"errors"
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
		return nil, err
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
	return subscriberOptions{
		MaxAttempts:        maxAttempts,
		VisibilityDuration: visibilityDuration,
		DequeueBatchSize:   dequeueBatchSize,
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
