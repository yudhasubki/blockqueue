package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yudhasubki/blockqueue/pkg/metric"
)

const (
	maintenanceYieldInterval = 10 * time.Millisecond
	topologyCleanupBudget    = 2 * time.Second
)

// startCheckpointer runs adaptive WAL checkpoints for SQLite. PostgreSQL owns
// its checkpoint lifecycle and returns immediately here.
func (q *Queue) startCheckpointer() {
	if !q.db.supportsSQLiteMaintenance() {
		return
	}

	interval := q.options.CheckpointInterval
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	lastCheckpoint := time.Now()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-q.serverCtx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(lastCheckpoint)
			if elapsed < interval {
				continue
			}
			if q.shouldDeferCheckpoint(elapsed) {
				continue
			}
			q.checkpointSQLite(q.serverCtx, sqliteCheckpointPassive)
			lastCheckpoint = time.Now()
		}
	}
}

func (q *Queue) shouldDeferCheckpoint(elapsed time.Duration) bool {
	if q.writer == nil || elapsed >= 2*time.Minute {
		return false
	}
	pending, _ := q.writer.Pending()
	return pending > int64(q.writer.batchSize)
}

// waitForMaintenanceRetry prevents a due row from turning a transient write
// failure into a hot loop. The injected clock keeps scheduler tests fully
// deterministic while production uses the real clock.
func waitForMaintenanceRetry(ctx context.Context, clock Clock) bool {
	select {
	case <-ctx.Done():
		return false
	case <-clock.After(time.Second):
		return true
	}
}

func waitForMaintenanceYield(ctx context.Context, clock Clock) bool {
	select {
	case <-ctx.Done():
		return false
	case <-clock.After(maintenanceYieldInterval):
		return true
	}
}

func (q *Queue) observeMaintenancePass(operation string, started time.Time, rows int64, failed, yielded bool) {
	if q.options.DisableMetrics {
		return
	}
	if rows > 0 {
		metric.MaintenanceRows.WithLabelValues(operation).Add(float64(rows))
	}
	outcome := metric.OutcomeSuccess
	if failed {
		outcome = metric.OutcomeFailed
	} else if yielded {
		outcome = metric.OutcomeYielded
	}
	metric.MaintenancePasses.WithLabelValues(operation, outcome).Inc()
	metric.MaintenanceDuration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
}

func (q *Queue) checkpointSQLite(ctx context.Context, mode sqliteCheckpointMode) {
	started := time.Now()
	result, err := q.db.checkpointSQLite(ctx, mode)
	if !q.options.DisableMetrics {
		metric.CheckpointDuration.Observe(time.Since(started).Seconds())
	}
	if err != nil {
		if !q.options.DisableMetrics {
			metric.CheckpointResults.WithLabelValues(metric.OutcomeFailed).Inc()
		}
		slog.Error("sqlite checkpoint failed", "error", err, "mode", mode)
		return
	}
	label := metric.OutcomeSuccess
	if result.Busy != 0 {
		label = metric.OutcomeBusy
	}
	if !q.options.DisableMetrics {
		metric.CheckpointResults.WithLabelValues(label).Inc()
	}
	slog.Debug("sqlite checkpoint", "mode", mode, "duration", time.Since(started),
		"busy", result.Busy, "wal_frames", result.LogFrames, "checkpointed_frames", result.Checkpointed)
}

func (q *Queue) startPruner() {
	retention := q.options.RetentionPeriod
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	scheduleRunRetention := q.options.ScheduleRunRetention
	if scheduleRunRetention <= 0 {
		scheduleRunRetention = 30 * 24 * time.Hour
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-q.serverCtx.Done():
			return
		case <-q.prunerSignal:
			q.runPrunerCycle(false, retention, scheduleRunRetention)
		case <-ticker.C:
			q.runPrunerCycle(true, retention, scheduleRunRetention)
		}
	}
}

func (q *Queue) runPrunerCycle(includeRetention bool, retention, scheduleRunRetention time.Duration) {
	leader, release, err := q.db.tryMaintenanceLeadership(q.serverCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("error acquiring maintenance leadership", "error", err)
		}
		return
	}
	if !leader {
		if q.serverCtx.Err() == nil {
			time.AfterFunc(time.Second, q.signalPruner)
		}
		return
	}
	defer func() {
		if err := release(); err != nil {
			slog.Error("error releasing maintenance leadership", "error", err)
		}
	}()

	q.pruneDeletedTopology()
	if !includeRetention || q.serverCtx.Err() != nil {
		return
	}
	if q.options.DeadLetterRetention > 0 {
		if err := q.db.pruneDeadLetters(q.serverCtx, q.options.DeadLetterRetention); err != nil {
			slog.Error("error pruning dead letters", "error", err)
		}
	}
	if err := q.db.pruneProcessedMessages(q.serverCtx, retention); err != nil {
		slog.Error("error pruning messages", "error", err)
	}
	if err := q.db.pruneScheduleRuns(q.serverCtx, scheduleRunRetention); err != nil {
		slog.Error("error pruning schedule runs", "error", err)
	}
	if q.db.supportsSQLiteMaintenance() {
		if err := q.db.incrementalVacuum(q.serverCtx); err != nil {
			slog.Error("error running incremental vacuum", "error", err)
		}
	}
}

func (q *Queue) signalPruner() {
	select {
	case q.prunerSignal <- struct{}{}:
	default:
	}
}

func (q *Queue) pruneDeletedTopology() {
	started := time.Now()
	rows, yielded, more, err := q.db.pruneDeletedTopology(q.serverCtx, topologyCleanupBudget)
	q.observeMaintenancePass(
		metric.MaintenanceOperationTopology, started, rows, err != nil, yielded,
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("error pruning deleted topology", "error", err)
	}
	if q.serverCtx.Err() == nil {
		switch {
		case err != nil:
			time.AfterFunc(time.Second, q.signalPruner)
		case more:
			time.AfterFunc(maintenanceYieldInterval, q.signalPruner)
		}
	}
}
