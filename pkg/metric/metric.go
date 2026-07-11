package metric

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

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
)

func Register(registerer prometheus.Registerer) error {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	collectors := []prometheus.Collector{
		PublishResults, PendingMessages, PendingBytes, WriterHealthy, FlushTotal,
		PersistenceLag, FlushSize, FlushDuration, DeliveryOperations, CheckpointDuration,
		DeliveryDuration, CheckpointResults, SchedulerOperations,
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
	WriterHealthy.Set(1)
	return nil
}
