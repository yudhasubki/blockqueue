package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"github.com/yudhasubki/blockqueue/store"
)

var (
	ErrTopicNotFound      = persistence.ErrTopicNotFound
	ErrQueueNotRunning    = errors.New("blockqueue is not running")
	ErrQueueStopping      = errors.New("blockqueue is stopping")
	ErrNoActiveSubscriber = persistence.ErrNoActiveSubscriber
	ErrInvalidPublish     = persistence.ErrInvalidPublish
	ErrInvalidTopic       = errors.New("invalid topic")
	ErrInvalidSubscriber  = errors.New("invalid subscriber")
	ErrResourceConflict   = persistence.ErrResourceConflict
)

type LifecycleState uint32

const (
	LifecycleNew LifecycleState = iota
	LifecycleRunning
	LifecycleStopping
	LifecycleStopped
)

// Queue is the import-first BlockQueue engine. It owns the supplied database
// driver from construction until shutdown.
type Queue struct {
	mtx              sync.Mutex
	runMu            sync.Mutex
	shutdownMu       sync.Mutex
	admissionMu      sync.RWMutex
	serverCtx        context.Context
	cancel           context.CancelFunc
	registry         atomic.Pointer[topicRegistry]
	db               *db
	writer           *writer
	state            atomic.Uint32
	topologyVersion  atomic.Uint64
	workers          sync.WaitGroup
	transactions     sync.WaitGroup
	controlOps       sync.WaitGroup
	transactionMu    sync.RWMutex
	activeTx         map[*sql.Tx]*activeTransaction
	schedulerSignal  chan struct{}
	reaperSignal     chan struct{}
	prunerSignal     chan struct{}
	schedulerOwner   string
	schedulerHealthy atomic.Bool
	deliveryHealthy  atomic.Bool
	listenerHealthy  atomic.Bool
	runtimeMetricID  uint64
	options          Options
}

// topicRegistry is immutable after publication. Hot-path reads only perform
// an atomic pointer load; rare topology mutations copy and replace it.
type topicRegistry struct {
	byName map[string]*topicRuntime
	byID   map[uuid.UUID]*topicRuntime
}

type Options struct {
	Writer               WriterOptions
	CheckpointInterval   time.Duration         // Default: 30s
	RetentionPeriod      time.Duration         // Default: 7d
	DeadLetterRetention  time.Duration         // Default: disabled; operators opt in explicitly
	ScheduleRunRetention time.Duration         // Default: 30d
	ShutdownTimeout      time.Duration         // Default: 30s for Close
	ReadinessBacklog     int64                 // Default: 90% of pending message budget
	Clock                Clock                 // Optional deterministic scheduler clock
	DisableMetrics       bool                  // Skip per-message metric updates on the hot path
	MetricRegisterer     prometheus.Registerer // Optional collector registry; defaults to Prometheus global registry
}

func New(driver store.Driver, opt Options) *Queue {
	if !opt.DisableMetrics {
		if err := metric.Register(opt.MetricRegisterer); err != nil {
			slog.Error("register blockqueue metrics", "error", err)
		}
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	queue := &Queue{
		db:              newDb(driver),
		options:         opt,
		serverCtx:       baseCtx,
		cancel:          cancel,
		schedulerSignal: make(chan struct{}, 1),
		reaperSignal:    make(chan struct{}, 1),
		prunerSignal:    make(chan struct{}, 1),
		schedulerOwner:  uuid.NewString(),
		activeTx:        make(map[*sql.Tx]*activeTransaction),
	}
	queue.db.setMetricsDisabled(opt.DisableMetrics)
	queue.schedulerHealthy.Store(true)
	queue.deliveryHealthy.Store(true)
	queue.listenerHealthy.Store(true)
	queue.registry.Store(&topicRegistry{
		byName: make(map[string]*topicRuntime),
		byID:   make(map[uuid.UUID]*topicRuntime),
	})
	queue.state.Store(uint32(LifecycleNew))

	return queue
}

func (q *Queue) setSchedulerHealthy(healthy bool) {
	q.schedulerHealthy.Store(healthy)
	if !q.options.DisableMetrics {
		metric.SetSchedulerHealth(q.runtimeMetricID, healthy)
	}
}

func (q *Queue) setDeliveryHealthy(healthy bool) {
	q.deliveryHealthy.Store(healthy)
	if !q.options.DisableMetrics {
		metric.SetDeliveryReaperHealth(q.runtimeMetricID, healthy)
	}
}

func (q *Queue) setListenerHealthy(healthy bool) {
	q.listenerHealthy.Store(healthy)
	if !q.options.DisableMetrics {
		metric.SetDatabaseListenerHealth(q.runtimeMetricID, healthy)
	}
}
