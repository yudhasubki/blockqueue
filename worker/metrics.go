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

		processed: metric.WorkerJobs.WithLabelValues(
			topic, subscriber, metric.WorkerOutcomeProcessed,
		),
		nacked: metric.WorkerJobs.WithLabelValues(
			topic, subscriber, metric.WorkerOutcomeNacked,
		),
		cancelled: metric.WorkerJobs.WithLabelValues(
			topic, subscriber, metric.WorkerOutcomeCancelled,
		),
		leaseLost: metric.WorkerJobs.WithLabelValues(
			topic, subscriber, metric.WorkerOutcomeLeaseLost,
		),
		completionFailed: metric.WorkerJobs.WithLabelValues(
			topic, subscriber, metric.WorkerOutcomeCompletionFailed,
		),

		handlerOK: metric.WorkerHandlerDuration.WithLabelValues(
			topic, subscriber, metric.WorkerHandlerResultOK,
		),
		handlerError: metric.WorkerHandlerDuration.WithLabelValues(
			topic, subscriber, metric.WorkerHandlerResultError,
		),
		handlerCancelRequested: metric.WorkerHandlerDuration.WithLabelValues(
			topic, subscriber, metric.WorkerHandlerResultCancelRequested,
		),
		handlerPanic: metric.WorkerHandlerDuration.WithLabelValues(
			topic, subscriber, metric.WorkerHandlerResultPanic,
		),

		heartbeatSuccess: metric.WorkerHeartbeats.WithLabelValues(
			topic, subscriber, metric.OutcomeSuccess,
		),
		heartbeatFailed: metric.WorkerHeartbeats.WithLabelValues(
			topic, subscriber, metric.OutcomeFailed,
		),
		heartbeatLeaseLost: metric.WorkerHeartbeats.WithLabelValues(
			topic, subscriber, metric.OutcomeLeaseLost,
		),
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
	case metric.WorkerHandlerResultOK:
		metrics.handlerOK.Observe(duration)
	case metric.WorkerHandlerResultCancelRequested:
		metrics.handlerCancelRequested.Observe(duration)
	case metric.WorkerHandlerResultPanic:
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
	case metric.OutcomeSuccess:
		metrics.heartbeatSuccess.Inc()
	case metric.OutcomeLeaseLost:
		metrics.heartbeatLeaseLost.Inc()
	default:
		metrics.heartbeatFailed.Inc()
	}
}
