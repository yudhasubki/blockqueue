package metric

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	writerMetricSequence  atomic.Uint64
	writerMetricMu        sync.Mutex
	writerHealth          = make(map[uint64]bool)
	writerLag             = make(map[uint64]float64)
	runtimeMetricSequence atomic.Uint64
	runtimeMetricMu       sync.Mutex
	runtimeHealth         = make(map[uint64]runtimeHealthState)
)

type runtimeHealthState struct {
	scheduler    bool
	delivery     bool
	listener     bool
	schedulerLag float64
}

var (
	PublishResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "persistence_total", Help: "Persistence outcomes.",
	}, []string{"result"})
	PendingMessages = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "pending_messages", Help: "Admitted messages waiting for commit.",
	})
	PendingBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "pending_bytes", Help: "Admitted bytes waiting for commit.",
	})
	WriterHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "writer_healthy", Help: "Whether the last writer flush succeeded.",
	})
	PersistenceLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "persistence_lag_seconds", Help: "Age of the oldest message in the current flush.",
	})
	FlushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "flush_total", Help: "Writer flush outcomes.",
	}, []string{"result"})
	FlushSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "flush_size", Help: "Messages per database flush.", Buckets: prometheus.ExponentialBuckets(1, 2, 11),
	})
	FlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "flush_duration_seconds", Help: "Database flush duration.",
	})
	DeliveryOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "delivery_operation_total", Help: "Delivery ACK/NACK/lease outcomes.",
	}, []string{"operation", "result"})
	DeliveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "delivery_operation_duration_seconds", Help: "Delivery operation latency.",
	}, []string{"operation"})
	CheckpointDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "checkpoint_duration_seconds", Help: "SQLite checkpoint duration.",
	})
	CheckpointResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "checkpoint_total", Help: "SQLite checkpoint outcomes.",
	}, []string{"result"})
	SchedulerOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "scheduler_operation_total", Help: "Scheduler outcomes.",
	}, []string{"operation", "result"})
	SchedulerHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "scheduler_healthy", Help: "Whether every active scheduler can query and claim work.",
	})
	SchedulerDueLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "scheduler_due_lag_seconds", Help: "Maximum overdue age observed by active schedulers.",
	})
	DeliveryReaperHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "delivery_reaper_healthy", Help: "Whether every active lease reaper can query and update deliveries.",
	})
	DatabaseListenerHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "database_listener_healthy", Help: "Whether every active PostgreSQL queue has a notification listener.",
	})
	MaintenanceRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "maintenance_rows_total", Help: "Rows processed by bounded maintenance operations.",
	}, []string{"operation"})
	MaintenancePasses = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "maintenance_pass_total", Help: "Bounded maintenance pass outcomes.",
	}, []string{"operation", "result"})
	MaintenanceDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "maintenance_duration_seconds", Help: "Duration of bounded maintenance passes.",
	}, []string{"operation"})
	WorkerJobs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "worker_jobs_total", Help: "Completed worker attempts by final disposition.",
	}, []string{"topic", "subscriber", "outcome"})
	WorkerHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "blockqueue", Name: "worker_handler_duration_seconds", Help: "Worker handler execution duration by return semantics.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600},
	}, []string{"topic", "subscriber", "result"})
	WorkerActiveHandlers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "blockqueue", Name: "worker_active_handlers", Help: "Currently executing worker handlers.",
	}, []string{"topic", "subscriber"})
	WorkerHeartbeats = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockqueue", Name: "worker_heartbeat_total", Help: "Worker lease heartbeat outcomes.",
	}, []string{"topic", "subscriber", "result"})
)

func Register(registerer prometheus.Registerer) error {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	collectors := []prometheus.Collector{
		PublishResults, PendingMessages, PendingBytes, WriterHealthy, FlushTotal,
		PersistenceLag, FlushSize, FlushDuration, DeliveryOperations, CheckpointDuration,
		DeliveryDuration, CheckpointResults, SchedulerOperations, WorkerJobs,
		SchedulerHealthy, SchedulerDueLag, DeliveryReaperHealthy, DatabaseListenerHealthy,
		MaintenanceRows, MaintenancePasses, MaintenanceDuration, WorkerHandlerDuration,
		WorkerActiveHandlers, WorkerHeartbeats,
	}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) {
				continue
			}
			return err
		}
	}
	return nil
}

func RegisterRuntime() uint64 {
	id := runtimeMetricSequence.Add(1)
	runtimeMetricMu.Lock()
	runtimeHealth[id] = runtimeHealthState{scheduler: true, delivery: true, listener: true}
	updateRuntimeGaugesLocked()
	runtimeMetricMu.Unlock()
	return id
}

func SetSchedulerHealth(id uint64, healthy bool) {
	updateRuntimeState(id, func(state *runtimeHealthState) { state.scheduler = healthy })
}

func SetDeliveryReaperHealth(id uint64, healthy bool) {
	updateRuntimeState(id, func(state *runtimeHealthState) { state.delivery = healthy })
}

func SetDatabaseListenerHealth(id uint64, healthy bool) {
	updateRuntimeState(id, func(state *runtimeHealthState) { state.listener = healthy })
}

func SetSchedulerDueLag(id uint64, seconds float64) {
	updateRuntimeState(id, func(state *runtimeHealthState) { state.schedulerLag = max(seconds, 0) })
}

func UnregisterRuntime(id uint64) {
	if id == 0 {
		return
	}
	runtimeMetricMu.Lock()
	delete(runtimeHealth, id)
	updateRuntimeGaugesLocked()
	runtimeMetricMu.Unlock()
}

func updateRuntimeState(id uint64, update func(*runtimeHealthState)) {
	if id == 0 {
		return
	}
	runtimeMetricMu.Lock()
	state, exists := runtimeHealth[id]
	if exists {
		update(&state)
		runtimeHealth[id] = state
		updateRuntimeGaugesLocked()
	}
	runtimeMetricMu.Unlock()
}

func updateRuntimeGaugesLocked() {
	scheduler, delivery, listener := 1.0, 1.0, 1.0
	maximumLag := 0.0
	for _, state := range runtimeHealth {
		if !state.scheduler {
			scheduler = 0
		}
		if !state.delivery {
			delivery = 0
		}
		if !state.listener {
			listener = 0
		}
		if state.schedulerLag > maximumLag {
			maximumLag = state.schedulerLag
		}
	}
	SchedulerHealthy.Set(scheduler)
	DeliveryReaperHealthy.Set(delivery)
	DatabaseListenerHealthy.Set(listener)
	SchedulerDueLag.Set(maximumLag)
}

func RegisterWriter() uint64 {
	id := writerMetricSequence.Add(1)
	writerMetricMu.Lock()
	writerHealth[id] = true
	writerLag[id] = 0
	updateWriterGaugesLocked()
	writerMetricMu.Unlock()
	return id
}

func SetWriterHealth(id uint64, healthy bool) {
	if id == 0 {
		return
	}
	writerMetricMu.Lock()
	writerHealth[id] = healthy
	updateWriterGaugesLocked()
	writerMetricMu.Unlock()
}

func SetPersistenceLag(id uint64, seconds float64) {
	if id == 0 {
		return
	}
	writerMetricMu.Lock()
	writerLag[id] = max(seconds, 0)
	updateWriterGaugesLocked()
	writerMetricMu.Unlock()
}

func UnregisterWriter(id uint64) {
	if id == 0 {
		return
	}
	writerMetricMu.Lock()
	delete(writerHealth, id)
	delete(writerLag, id)
	updateWriterGaugesLocked()
	writerMetricMu.Unlock()
}

func updateWriterGaugesLocked() {
	healthy := 1.0
	maximumLag := 0.0
	for id, state := range writerHealth {
		if !state {
			healthy = 0
		}
		if writerLag[id] > maximumLag {
			maximumLag = writerLag[id]
		}
	}
	WriterHealthy.Set(healthy)
	PersistenceLag.Set(maximumLag)
}
