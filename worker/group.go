package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// Group supervises multiple independent topic/subscriber workers under one
// lifecycle. It does not impose global concurrency; every Worker retains its
// own bounded concurrency and drain policy.
type Group struct {
	workers []*Worker
	started atomic.Bool
}

// NewGroup validates and registers workers. A Worker may belong to only one
// position in the group, and both Group and Worker are single-use.
func NewGroup(workers ...*Worker) (*Group, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("%w: at least one worker is required", ErrInvalidConfiguration)
	}
	seen := make(map[*Worker]struct{}, len(workers))
	copyOfWorkers := make([]*Worker, len(workers))
	for index, worker := range workers {
		if worker == nil {
			return nil, fmt.Errorf("%w: worker %d is nil", ErrInvalidConfiguration, index)
		}
		if _, exists := seen[worker]; exists {
			return nil, fmt.Errorf("%w: worker %d is registered more than once", ErrInvalidConfiguration, index)
		}
		seen[worker] = struct{}{}
		copyOfWorkers[index] = worker
	}
	return &Group{workers: copyOfWorkers}, nil
}

// Run starts every registered Worker. A terminal error from one worker
// cancels and drains the others, then all worker errors are joined. Canceling
// ctx is a normal shutdown and returns nil when every worker drains cleanly.
func (group *Group) Run(ctx context.Context) error {
	if !group.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(group.workers))
	for _, registered := range group.workers {
		worker := registered
		go func() { results <- worker.Run(runContext) }()
	}
	errorsSeen := make([]error, 0, len(group.workers))
	for range group.workers {
		if err := <-results; err != nil {
			errorsSeen = append(errorsSeen, err)
			cancel()
		}
	}
	return errors.Join(errorsSeen...)
}
