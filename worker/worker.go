// Package worker provides a bounded, lease-aware consumer runtime for
// BlockQueue. It owns delivery execution, not the underlying queue lifecycle.
package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue"
)

var (
	ErrAlreadyStarted          = errors.New("worker has already been started")
	ErrInvalidConfiguration    = errors.New("invalid worker configuration")
	ErrDrainTimeout            = errors.New("worker drain deadline exceeded")
	ErrJobCompleted            = errors.New("job attempt is already completed")
	ErrTransactionsUnsupported = errors.New("worker client does not support transactions")
	ErrRetryRequested          = errors.New("worker retry requested")
	ErrCancelRequested         = errors.New("worker cancellation requested")
	ErrClientProtocol          = errors.New("worker client violated claim contract")
)

const (
	defaultLeaseDuration    = time.Minute
	defaultOperationTimeout = 10 * time.Second
	defaultDrainTimeout     = 30 * time.Second
	defaultCompletionBatch  = 100
	defaultCompletionFlush  = time.Millisecond
	defaultPausePoll        = 5 * time.Second
	defaultHardStopTimeout  = time.Second
	claimRetryMinimum       = 50 * time.Millisecond
	claimRetryMaximum       = time.Second
	maximumConcurrency      = 1000
)

// Client is the queue surface required by a Worker. ClaimWait implementations
// must never return more deliveries than limit. *blockqueue.Queue implements
// Client.
type Client interface {
	ClaimWait(context.Context, blockqueue.Topic, string, int, time.Duration) (blockqueue.Deliveries, error)
	AckDelivery(context.Context, blockqueue.Topic, string, string, string) error
	NackDelivery(context.Context, blockqueue.Topic, string, string, string, time.Duration, string) error
	ExtendLease(context.Context, blockqueue.Topic, string, string, string, time.Duration) (time.Time, error)
	CancelClaimedDelivery(context.Context, blockqueue.Topic, string, string, string, string) error
}

// BatchClient enables set-based automatic ACK/NACK. *blockqueue.Queue
// implements BatchClient. Failed batch items are retried through Client using
// their original receipt tokens.
type BatchClient interface {
	Client
	BatchAckDeliveries(context.Context, blockqueue.Topic, string, []blockqueue.BatchAckItem) []blockqueue.DeliveryResult
	BatchNackDeliveries(context.Context, blockqueue.Topic, string, []blockqueue.BatchNackItem) []blockqueue.DeliveryResult
}

// TransactionalClient is implemented by clients that can atomically combine
// application writes and delivery acknowledgement. *blockqueue.Queue
// implements TransactionalClient.
type TransactionalClient interface {
	Client
	WithTx(context.Context, *sql.TxOptions, func(*sql.Tx) error) error
	AckDeliveryTx(context.Context, *sql.Tx, blockqueue.Topic, string, string, string) error
	CancelClaimedDeliveryTx(context.Context, *sql.Tx, blockqueue.Topic, string, string, string, string) error
}

var (
	_ Client              = (*blockqueue.Queue)(nil)
	_ BatchClient         = (*blockqueue.Queue)(nil)
	_ TransactionalClient = (*blockqueue.Queue)(nil)
)

// Handler processes one leased delivery. Returning nil acknowledges it;
// returning an error records a NACK and applies the subscriber retry policy.
// Return CancelJob for a permanent, receipt-fenced cancellation.
type Handler interface {
	Handle(context.Context, *Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *Job) error

func (handler HandlerFunc) Handle(ctx context.Context, job *Job) error {
	return handler(ctx, job)
}

// TypedJob contains JSON-decoded arguments and the underlying delivery job.
type TypedJob[T any] struct {
	*Job
	Args T
}

// TypedHandler processes a JSON-decoded delivery.
type TypedHandler[T any] interface {
	Handle(context.Context, *TypedJob[T]) error
}

// TypedHandlerFunc adapts a function to TypedHandler.
type TypedHandlerFunc[T any] func(context.Context, *TypedJob[T]) error

func (handler TypedHandlerFunc[T]) Handle(ctx context.Context, job *TypedJob[T]) error {
	return handler(ctx, job)
}

// Options controls execution and lease ownership. Zero values use safe
// defaults.
type Options struct {
	// Concurrency is the maximum number of active handlers. Default: 1.
	Concurrency int
	// BatchSize caps one claim and is always limited by free concurrency slots.
	// Default: Concurrency.
	BatchSize int
	// LeaseDuration is renewed while a handler is active. Default: 1m.
	LeaseDuration time.Duration
	// HeartbeatInterval is deterministically jittered to avoid a batch
	// stampede. Default: one third of LeaseDuration.
	HeartbeatInterval time.Duration
	// DisableHeartbeat is intended for short tests and specialized consumers.
	DisableHeartbeat bool
	// OperationTimeout bounds automatic ACK/NACK and heartbeat calls.
	// Default: 10s.
	OperationTimeout time.Duration
	// DrainTimeout bounds graceful handler drain after Run context cancellation.
	// Default: 30s.
	DrainTimeout time.Duration
	// HardStopTimeout is a second, bounded wait after DrainTimeout cancels
	// handler contexts. Default: 1s.
	HardStopTimeout time.Duration
	// PausePollInterval controls how often an intentionally paused resource is
	// checked for resume. Pause is logged once, not on every poll. Default: 5s.
	PausePollInterval time.Duration
	// CompletionBatchSize caps set-based ACK/NACK transactions when the client
	// implements BatchClient. Default: 100; also capped by Concurrency.
	CompletionBatchSize int
	// CompletionFlushInterval bounds completion batching latency. Default: 1ms.
	CompletionFlushInterval time.Duration
	// Logger receives operational retry, panic, and completion failures.
	// Default: slog.Default().
	Logger *slog.Logger
	// DisableMetrics makes every worker collector a no-op.
	DisableMetrics bool
	// MetricRegisterer receives worker collectors. Default: Prometheus global
	// registerer.
	MetricRegisterer prometheus.Registerer
}

// Worker runs one bounded consumer for a topic/subscriber pair.
type Worker struct {
	client      Client
	batchClient BatchClient
	topic       blockqueue.Topic
	subscriber  string
	handler     Handler
	options     Options
	completions chan completionRequest
	metrics     workerMetrics
	state       atomic.Uint32
}

// New creates a worker for one topic/subscriber pair. The returned worker is
// single-use; create a new Worker after Run returns.
func New(client Client, topic blockqueue.Topic, subscriber string, handler Handler, options Options) (*Worker, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is required", ErrInvalidConfiguration)
	}
	if topic.ID.String() == "00000000-0000-0000-0000-000000000000" && topic.Name == "" {
		return nil, fmt.Errorf("%w: topic is required", ErrInvalidConfiguration)
	}
	if subscriber == "" {
		return nil, fmt.Errorf("%w: subscriber is required", ErrInvalidConfiguration)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfiguration)
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	metrics, err := newWorkerMetrics(normalized, topic.Name, subscriber)
	if err != nil {
		return nil, err
	}
	worker := &Worker{
		client:     client,
		topic:      topic,
		subscriber: subscriber,
		handler:    handler,
		options:    normalized,
		metrics:    metrics,
	}
	if batchClient, ok := client.(BatchClient); ok {
		worker.batchClient = batchClient
		worker.completions = make(chan completionRequest, normalized.Concurrency)
	}
	return worker, nil
}

// NewJSON creates a worker that decodes Delivery.Message as JSON before
// invoking handler. Decode failures follow the subscriber NACK/DLQ policy so
// they remain observable and replayable; handlers explicitly return CancelJob
// only when cancellation is the intended business outcome.
func NewJSON[T any](
	client Client,
	topic blockqueue.Topic,
	subscriber string,
	handler TypedHandler[T],
	options Options,
) (*Worker, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfiguration)
	}
	return New(client, topic, subscriber, HandlerFunc(func(ctx context.Context, job *Job) error {
		var arguments T
		if err := json.Unmarshal([]byte(job.Message), &arguments); err != nil {
			return fmt.Errorf("decode worker message: %w", err)
		}
		return handler.Handle(ctx, &TypedJob[T]{Job: job, Args: arguments})
	}), options)
}

func normalizeOptions(options Options) (Options, error) {
	if options.Concurrency < 0 || options.Concurrency > maximumConcurrency {
		return Options{}, fmt.Errorf("%w: concurrency must be between 1 and %d", ErrInvalidConfiguration, maximumConcurrency)
	}
	if options.Concurrency == 0 {
		options.Concurrency = 1
	}
	if options.BatchSize < 0 || options.BatchSize > maximumConcurrency {
		return Options{}, fmt.Errorf("%w: batch size must be between 1 and %d", ErrInvalidConfiguration, maximumConcurrency)
	}
	if options.BatchSize == 0 {
		options.BatchSize = options.Concurrency
	}
	if options.LeaseDuration < 0 || options.LeaseDuration > blockqueue.MaximumDeliveryLease {
		return Options{}, fmt.Errorf("%w: lease duration must be between 1ms and %s", ErrInvalidConfiguration, blockqueue.MaximumDeliveryLease)
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = defaultLeaseDuration
	}
	if options.LeaseDuration < time.Millisecond {
		return Options{}, fmt.Errorf("%w: lease duration must be between 1ms and %s", ErrInvalidConfiguration, blockqueue.MaximumDeliveryLease)
	}
	if options.HeartbeatInterval < 0 {
		return Options{}, fmt.Errorf("%w: heartbeat interval cannot be negative", ErrInvalidConfiguration)
	}
	if !options.DisableHeartbeat {
		if options.HeartbeatInterval == 0 {
			options.HeartbeatInterval = options.LeaseDuration / 3
		}
		if options.HeartbeatInterval <= 0 || options.HeartbeatInterval >= options.LeaseDuration {
			return Options{}, fmt.Errorf("%w: heartbeat interval must be shorter than lease duration", ErrInvalidConfiguration)
		}
	}
	if options.OperationTimeout < 0 {
		return Options{}, fmt.Errorf("%w: operation timeout cannot be negative", ErrInvalidConfiguration)
	}
	if options.OperationTimeout == 0 {
		options.OperationTimeout = defaultOperationTimeout
	}
	if options.DrainTimeout < 0 {
		return Options{}, fmt.Errorf("%w: drain timeout cannot be negative", ErrInvalidConfiguration)
	}
	if options.DrainTimeout == 0 {
		options.DrainTimeout = defaultDrainTimeout
	}
	if options.HardStopTimeout < 0 {
		return Options{}, fmt.Errorf("%w: hard stop timeout cannot be negative", ErrInvalidConfiguration)
	}
	if options.HardStopTimeout == 0 {
		options.HardStopTimeout = defaultHardStopTimeout
	}
	if options.PausePollInterval < 0 {
		return Options{}, fmt.Errorf("%w: pause poll interval cannot be negative", ErrInvalidConfiguration)
	}
	if options.PausePollInterval == 0 {
		options.PausePollInterval = defaultPausePoll
	}
	if options.CompletionBatchSize < 0 || options.CompletionBatchSize > maximumConcurrency {
		return Options{}, fmt.Errorf("%w: completion batch size must be between 1 and %d", ErrInvalidConfiguration, maximumConcurrency)
	}
	if options.CompletionBatchSize == 0 {
		options.CompletionBatchSize = defaultCompletionBatch
	}
	if options.CompletionFlushInterval < 0 {
		return Options{}, fmt.Errorf("%w: completion flush interval cannot be negative", ErrInvalidConfiguration)
	}
	if options.CompletionFlushInterval == 0 {
		options.CompletionFlushInterval = defaultCompletionFlush
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return options, nil
}
