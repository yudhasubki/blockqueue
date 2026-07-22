package blockqueue

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/internal/subscriberconfig"
)

var (
	ErrSubscriberNotFound = persistence.ErrSubscriberNotFound
	ErrSubscriberDeleted  = errors.New("subscriber was deleted")
)

type subscriberOptions = subscriberconfig.Parsed

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
	return subscriberconfig.Parse(toSubscriberConfigOptions(subscriber.Options))
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
