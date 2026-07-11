package persistence

import (
	"context"
	"log/slog"
	"time"
)

const (
	retentionBatchSize  = 1000
	retentionPassBudget = 2 * time.Second
)

// pruneProcessedMessages uses processed_at as the retention clock and works in
// bounded chunks so maintenance cannot monopolize the writer.
func (d *db) pruneProcessedMessages(ctx context.Context, retention time.Duration) error {
	now, err := d.databaseNow(ctx)
	if err != nil {
		return err
	}
	threshold := now.Add(-retention)
	deadline := time.Now().Add(retentionPassBudget)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT message_id, subscriber_id FROM message_deliveries
				WHERE status IN ('processed', 'cancelled') AND processed_at < ?
				ORDER BY processed_at LIMIT ?
			)`), threshold, retentionBatchSize)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < retentionBatchSize {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	var orphaned int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM messages
			WHERE id IN (
				SELECT candidate.id FROM messages candidate
				WHERE NOT EXISTS (
					SELECT 1 FROM message_deliveries
					WHERE message_deliveries.message_id = candidate.id
				)
				ORDER BY candidate.id LIMIT ?
			)`), retentionBatchSize)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		orphaned += rows
		if rows < retentionBatchSize {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 || orphaned > 0 {
		slog.Info("pruned processed queue data", "deliveries", total, "messages", orphaned, "threshold", threshold)
	}
	return nil
}

func (d *db) pruneScheduleRuns(ctx context.Context, retention time.Duration) error {
	now, err := d.databaseNow(ctx)
	if err != nil {
		return err
	}
	threshold := now.Add(-retention)
	deadline := time.Now().Add(retentionPassBudget)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM schedule_runs
			WHERE id IN (
				SELECT id FROM schedule_runs
				WHERE status <> 'running' AND COALESCE(finished_at, created_at) < ?
				ORDER BY COALESCE(finished_at, created_at), id LIMIT ?
			)`), threshold, retentionBatchSize)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < retentionBatchSize {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 {
		slog.Info("pruned schedule runs", "count", total, "threshold", threshold)
	}
	return nil
}

func (d *db) pruneDeadLetters(ctx context.Context, retention time.Duration) error {
	now, err := d.databaseNow(ctx)
	if err != nil {
		return err
	}
	threshold := now.Add(-retention)
	deadline := time.Now().Add(retentionPassBudget)
	var total int64
	for time.Now().Before(deadline) {
		result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
			DELETE FROM message_deliveries
			WHERE (message_id, subscriber_id) IN (
				SELECT message_id, subscriber_id FROM message_deliveries
				WHERE status = 'dead_letter' AND processed_at < ?
				ORDER BY processed_at LIMIT ?
			)`), threshold, retentionBatchSize)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		total += rows
		if rows < retentionBatchSize {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if total > 0 {
		slog.Info("pruned dead letters", "count", total, "threshold", threshold)
	}
	return nil
}
