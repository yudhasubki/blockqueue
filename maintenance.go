package blockqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/yudhasubki/blockqueue/pkg/metric"
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
			pending := int64(0)
			if q.writer != nil {
				pending, _ = q.writer.Pending()
			}
			if pending > int64(q.writer.batchSize) && elapsed < 2*time.Minute {
				continue
			}
			q.checkpointSQLite(q.serverCtx, sqliteCheckpointPassive)
			lastCheckpoint = time.Now()
		}
	}
}

func (q *Queue) checkpointSQLite(ctx context.Context, mode sqliteCheckpointMode) {
	started := time.Now()
	result, err := q.db.checkpointSQLite(ctx, mode)
	if !q.options.DisableMetrics {
		metric.CheckpointDuration.Observe(time.Since(started).Seconds())
	}
	if err != nil {
		if !q.options.DisableMetrics {
			metric.CheckpointResults.WithLabelValues("failed").Inc()
		}
		slog.Error("sqlite checkpoint failed", "error", err, "mode", mode)
		return
	}
	label := "success"
	if result.Busy != 0 {
		label = "busy"
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
		case <-ticker.C:
			if err := q.db.pruneDeletedSubscriberDeliveries(q.serverCtx, 2*time.Second); err != nil {
				slog.Error("error pruning deleted subscriber deliveries", "error", err)
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
	}
}
