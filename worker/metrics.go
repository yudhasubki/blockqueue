package worker

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

type workerMetrics struct {
	active prometheus.Gauge

	processed        prometheus.Counter
	nacked           prometheus.Counter
	cancelled        prometheus.Counter
	leaseLost        prometheus.Counter
	completionFailed prometheus.Counter

	handlerOK              prometheus.Observer
	handlerError           prometheus.Observer
	handlerCancelRequested prometheus.Observer
	handlerPanic           prometheus.Observer

	heartbeatSuccess   prometheus.Counter
	heartbeatFailed    prometheus.Counter
	heartbeatLeaseLost prometheus.Counter
}

func newWorkerMetrics(options Options, topic, subscriber string) (workerMetrics, error) {
	if options.DisableMetrics {
		return workerMetrics{}, nil
	}
	if err := metric.Register(options.MetricRegisterer); err != nil {
		return workerMetrics{}, fmt.Errorf("register worker metrics: %w", err)
	}
	labels := []string{topic, subscriber}
	return workerMetrics{
		active: metric.WorkerActiveHandlers.WithLabelValues(labels...),

		processed:        metric.WorkerJobs.WithLabelValues(topic, subscriber, "processed"),
		nacked:           metric.WorkerJobs.WithLabelValues(topic, subscriber, "nacked"),
		cancelled:        metric.WorkerJobs.WithLabelValues(topic, subscriber, "cancelled"),
		leaseLost:        metric.WorkerJobs.WithLabelValues(topic, subscriber, "lease_lost"),
		completionFailed: metric.WorkerJobs.WithLabelValues(topic, subscriber, "completion_failed"),

		handlerOK:              metric.WorkerHandlerDuration.WithLabelValues(topic, subscriber, "ok"),
		handlerError:           metric.WorkerHandlerDuration.WithLabelValues(topic, subscriber, "error"),
		handlerCancelRequested: metric.WorkerHandlerDuration.WithLabelValues(topic, subscriber, "cancel_requested"),
		handlerPanic:           metric.WorkerHandlerDuration.WithLabelValues(topic, subscriber, "panic"),

		heartbeatSuccess:   metric.WorkerHeartbeats.WithLabelValues(topic, subscriber, "success"),
		heartbeatFailed:    metric.WorkerHeartbeats.WithLabelValues(topic, subscriber, "failed"),
		heartbeatLeaseLost: metric.WorkerHeartbeats.WithLabelValues(topic, subscriber, "lease_lost"),
	}, nil
}

func (metrics workerMetrics) handlerStarted() {
	if metrics.active != nil {
		metrics.active.Inc()
	}
}

func (metrics workerMetrics) handlerFinished(result string, started time.Time) {
	if metrics.active == nil {
		return
	}
	metrics.active.Dec()
	duration := time.Since(started).Seconds()
	switch result {
	case "ok":
		metrics.handlerOK.Observe(duration)
	case "cancel_requested":
		metrics.handlerCancelRequested.Observe(duration)
	case "panic":
		metrics.handlerPanic.Observe(duration)
	default:
		metrics.handlerError.Observe(duration)
	}
}

func (metrics workerMetrics) jobCompleted(outcome jobOutcome) {
	if metrics.active == nil {
		return
	}
	switch outcome {
	case jobOutcomeProcessed:
		metrics.processed.Inc()
	case jobOutcomeFailed:
		metrics.nacked.Inc()
	case jobOutcomeCancelled:
		metrics.cancelled.Inc()
	}
}

func (metrics workerMetrics) jobLeaseLost() {
	if metrics.leaseLost != nil {
		metrics.leaseLost.Inc()
	}
}

func (metrics workerMetrics) jobCompletionFailed() {
	if metrics.completionFailed != nil {
		metrics.completionFailed.Inc()
	}
}

func (metrics workerMetrics) heartbeat(result string) {
	if metrics.active == nil {
		return
	}
	switch result {
	case "success":
		metrics.heartbeatSuccess.Inc()
	case "lease_lost":
		metrics.heartbeatLeaseLost.Inc()
	default:
		metrics.heartbeatFailed.Inc()
	}
}
