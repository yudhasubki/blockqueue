package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const (
	scheduleColumns = `id, topic_id, name, cron_expression, timezone, message,
		headers, correlation_id, priority, misfire_policy, overlap_policy, paused,
		version, next_run_at, owner_id, lease_expires_at, fencing_token, created_at, updated_at`
	scheduleRunColumns = `id, schedule_id, message_id, scheduled_for, started_at,
		finished_at, status, error, created_at`
)

func (d *db) scheduleNameExists(ctx context.Context, topicID uuid.UUID, name string) (bool, error) {
	var count int
	err := d.Conn().GetContext(ctx, &count,
		d.Conn().Rebind("SELECT COUNT(*) FROM schedules WHERE topic_id = ? AND name = ?"), topicID, name)
	return count > 0, err
}

func (d *db) createSchedule(ctx context.Context, schedule Schedule) error {
	_, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
		INSERT INTO schedules
			(id, topic_id, name, cron_expression, timezone, message, headers,
			 correlation_id, priority, misfire_policy, overlap_policy, next_run_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
		schedule.ID, schedule.TopicID, schedule.Name, schedule.CronExpression,
		schedule.Timezone, schedule.Message, schedule.Headers, nullString(schedule.CorrelationID.String),
		schedule.Priority, schedule.MisfirePolicy, schedule.OverlapPolicy, schedule.NextRunAt)
	if err == nil {
		_ = d.notifyDatabase(ctx, "scheduler")
	}
	return normalizeResourceConflict(err)
}

func (d *db) getSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) (Schedule, error) {
	var schedule Schedule
	err := d.Conn().GetContext(ctx, &schedule, d.Conn().Rebind(
		"SELECT "+scheduleColumns+" FROM schedules WHERE id = ? AND topic_id = ?"), scheduleID, topicID)
	if errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, ErrScheduleNotFound
	}
	return schedule, err
}

func (d *db) listSchedules(ctx context.Context, topicID uuid.UUID) ([]Schedule, error) {
	items := make([]Schedule, 0)
	err := d.Conn().SelectContext(ctx, &items, d.Conn().Rebind(
		"SELECT "+scheduleColumns+" FROM schedules WHERE topic_id = ? ORDER BY created_at, id"), topicID)
	return items, err
}

func (d *db) updateSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string, expectedVersion int, schedule Schedule) error {
	result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
		UPDATE schedules
		SET name = ?, cron_expression = ?, timezone = ?, message = ?, headers = ?,
		    correlation_id = ?, priority = ?, misfire_policy = ?, overlap_policy = ?,
		    next_run_at = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP,
		    owner_id = NULL, lease_expires_at = NULL
		WHERE id = ? AND topic_id = ? AND version = ?`),
		schedule.Name, schedule.CronExpression, schedule.Timezone, schedule.Message, schedule.Headers,
		nullString(schedule.CorrelationID.String), schedule.Priority, schedule.MisfirePolicy,
		schedule.OverlapPolicy, schedule.NextRunAt, scheduleID, topicID, expectedVersion)
	if err != nil {
		return normalizeResourceConflict(err)
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		_ = d.notifyDatabase(ctx, "scheduler")
		return nil
	}
	exists, err := d.scheduleExists(ctx, topicID, scheduleID)
	if err != nil {
		return err
	}
	if !exists {
		return ErrScheduleNotFound
	}
	return ErrScheduleVersion
}

func (d *db) scheduleExists(ctx context.Context, topicID uuid.UUID, scheduleID string) (bool, error) {
	var count int
	err := d.Conn().GetContext(ctx, &count, d.Conn().Rebind(
		"SELECT COUNT(*) FROM schedules WHERE id = ? AND topic_id = ?"), scheduleID, topicID)
	return count > 0, err
}

func (d *db) deleteSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) error {
	result, err := d.Conn().ExecContext(ctx,
		d.Conn().Rebind("DELETE FROM schedules WHERE id = ? AND topic_id = ?"), scheduleID, topicID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrScheduleNotFound
	}
	_ = d.notifyDatabase(ctx, "scheduler")
	return nil
}

func (d *db) setSchedulePaused(ctx context.Context, topicID uuid.UUID, scheduleID string, paused bool) error {
	result, err := d.Conn().ExecContext(ctx, d.Conn().Rebind(`
		UPDATE schedules SET paused = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP,
		owner_id = NULL, lease_expires_at = NULL WHERE id = ? AND topic_id = ?`), paused, scheduleID, topicID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrScheduleNotFound
	}
	_ = d.notifyDatabase(ctx, "scheduler")
	return nil
}

func (d *db) listScheduleRuns(ctx context.Context, scheduleID string, limit int, before time.Time, beforeID string) ([]ScheduleRun, error) {
	query := "SELECT " + scheduleRunColumns + " FROM schedule_runs WHERE schedule_id = ?"
	args := []any{scheduleID}
	if beforeID != "" {
		query += " AND (scheduled_for < ? OR (scheduled_for = ? AND id < ?))"
		args = append(args, before, before, beforeID)
	}
	query += " ORDER BY scheduled_for DESC, id DESC LIMIT ?"
	args = append(args, limit)
	runs := make([]ScheduleRun, 0, limit)
	err := d.Conn().SelectContext(ctx, &runs, d.Conn().Rebind(query), args...)
	return runs, err
}

func (d *db) nextScheduleDue(ctx context.Context, now time.Time) (time.Time, bool, error) {
	var next any
	err := d.Conn().QueryRowxContext(ctx, d.Conn().Rebind(`
		SELECT MIN(CASE
			WHEN owner_id IS NOT NULL AND lease_expires_at > ? THEN lease_expires_at
			ELSE next_run_at
		END)
		FROM schedules WHERE paused = `+boolLiteral(d, false)), now).Scan(&next)
	if err != nil {
		return time.Time{}, false, err
	}
	return databaseTime(next)
}

func (d *db) claimDueSchedule(ctx context.Context, owner string, now time.Time, lease time.Duration) (Schedule, bool, error) {
	var schedule Schedule
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		query := `SELECT ` + scheduleColumns + ` FROM schedules
			WHERE paused = ` + boolLiteral(d, false) + ` AND next_run_at <= ?
			  AND (owner_id IS NULL OR lease_expires_at <= ?)
			ORDER BY next_run_at, id LIMIT 1`
		query += d.dialect.lockClause("", true)
		if err := tx.GetContext(ctx, &schedule, tx.Rebind(query), now, now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, tx.Rebind(`
			UPDATE schedules SET owner_id = ?, lease_expires_at = ?, fencing_token = fencing_token + 1
			WHERE id = ? AND (owner_id IS NULL OR lease_expires_at <= ?)`),
			owner, now.Add(lease), schedule.ID, now)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return sql.ErrNoRows
		}
		schedule.OwnerID = sql.NullString{String: owner, Valid: true}
		schedule.LeaseExpiresAt = sql.NullTime{Time: now.Add(lease), Valid: true}
		schedule.FencingToken++
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, false, nil
	}
	return schedule, err == nil, err
}

// persistScheduleOccurrence makes run creation, canonical publish, fanout, and
// schedule advancement one atomic transaction. The fencing token prevents a
// stale owner from advancing a schedule after takeover.
func (d *db) persistScheduleOccurrence(
	ctx context.Context,
	claimed Schedule,
	scheduledFor, nextRunAt time.Time,
	force, advance bool,
	owner string,
) (ScheduleRun, error) {
	var resultRun ScheduleRun
	err := d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		var topicID string
		if err := tx.GetContext(ctx, &topicID, tx.Rebind(
			"SELECT topic_id FROM schedules WHERE id = ?"), claimed.ID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrScheduleNotFound
			}
			return err
		}
		topicLock := "SELECT id FROM topics WHERE id = ? AND deleted_at IS NULL"
		topicLock += d.dialect.lockClause("", false)
		var lockedTopic string
		if err := tx.GetContext(ctx, &lockedTopic, tx.Rebind(topicLock), topicID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrTopicNotFound
			}
			return err
		}

		query := "SELECT " + scheduleColumns + " FROM schedules WHERE id = ?"
		query += d.dialect.lockClause("", false)
		var schedule Schedule
		if err := tx.GetContext(ctx, &schedule, tx.Rebind(query), claimed.ID); err != nil {
			return err
		}
		if advance && (schedule.OwnerID.String != owner || schedule.FencingToken != claimed.FencingToken) {
			return ErrScheduleLeaseLost
		}
		visibleAt, err := d.nowTx(ctx, tx)
		if err != nil {
			return err
		}

		if !force {
			var active int
			if err := tx.GetContext(ctx, &active, tx.Rebind(`
				SELECT COUNT(*) FROM schedule_runs runs
				WHERE runs.schedule_id = ? AND runs.status = 'running'
				  AND runs.scheduled_for <> ?
				  AND (runs.message_id IS NULL OR EXISTS (
					SELECT 1 FROM message_deliveries deliveries
					WHERE deliveries.message_id = runs.message_id
					  AND deliveries.status NOT IN ('processed', 'dead_letter')
				  ))`), schedule.ID, scheduledFor); err != nil {
				return err
			}
			if active > 0 {
				resultRun = ScheduleRun{
					ID: uuid.NewString(), ScheduleID: schedule.ID,
					ScheduledFor: scheduledFor, Status: "skipped",
				}
				if _, err := tx.ExecContext(ctx, tx.Rebind(`
					INSERT INTO schedule_runs
						(id, schedule_id, scheduled_for, status, finished_at, error)
					VALUES (?, ?, ?, 'skipped', CURRENT_TIMESTAMP, ?)
					ON CONFLICT (schedule_id, scheduled_for) DO NOTHING`),
					resultRun.ID, schedule.ID, scheduledFor, ErrScheduleOverlap.Error()); err != nil {
					return err
				}
				return d.advanceClaimedScheduleTx(ctx, tx, schedule, nextRunAt, advance, owner, claimed.FencingToken)
			}
		}

		resultRun = ScheduleRun{
			ID: uuid.NewString(), ScheduleID: schedule.ID,
			ScheduledFor: scheduledFor, Status: "running",
		}
		insertRun, err := tx.ExecContext(ctx, tx.Rebind(`
			INSERT INTO schedule_runs (id, schedule_id, scheduled_for, status)
			VALUES (?, ?, ?, 'running')
			ON CONFLICT (schedule_id, scheduled_for) DO NOTHING`),
			resultRun.ID, schedule.ID, scheduledFor)
		if err != nil {
			return err
		}
		inserted, _ := insertRun.RowsAffected()
		if inserted == 0 {
			if err := tx.GetContext(ctx, &resultRun, tx.Rebind(
				"SELECT "+scheduleRunColumns+" FROM schedule_runs WHERE schedule_id = ? AND scheduled_for = ?"),
				schedule.ID, scheduledFor); err != nil {
				return err
			}
			return d.advanceClaimedScheduleTx(ctx, tx, schedule, nextRunAt, advance, owner, claimed.FencingToken)
		}

		var subscribers int
		if err := tx.GetContext(ctx, &subscribers, tx.Rebind(`
			SELECT COUNT(*) FROM topic_subscribers
			WHERE topic_id = ? AND deleted_at IS NULL`), schedule.TopicID); err != nil {
			return err
		}
		if subscribers == 0 {
			return ErrNoActiveSubscriber
		}
		topicUUID, err := uuid.Parse(schedule.TopicID)
		if err != nil {
			return err
		}
		idempotencyKey := schedule.ID + ":" + scheduledFor.UTC().Format(time.RFC3339Nano)
		messageID := uuid.NewSHA1(topicUUID, []byte(idempotencyKey)).String()
		insertMessage, err := tx.ExecContext(ctx, tx.Rebind(`
			INSERT INTO messages
				(id, topic_id, message, headers, correlation_id, idempotency_key,
				 priority, scheduled_at, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT DO NOTHING`),
			messageID, schedule.TopicID, schedule.Message, schedule.Headers,
			nullString(schedule.CorrelationID.String), idempotencyKey,
			schedule.Priority, visibleAt, visibleAt)
		if err != nil {
			return err
		}
		messageInserted, _ := insertMessage.RowsAffected()
		if messageInserted == 0 {
			var existing struct {
				ID            string         `db:"id"`
				Message       string         `db:"message"`
				Headers       string         `db:"headers"`
				CorrelationID sql.NullString `db:"correlation_id"`
				Priority      int            `db:"priority"`
			}
			if err := tx.GetContext(ctx, &existing, tx.Rebind(`
				SELECT id, message, headers, correlation_id, priority FROM messages
				WHERE topic_id = ? AND idempotency_key = ?`),
				schedule.TopicID, idempotencyKey); err != nil {
				return err
			}
			if existing.ID != messageID || existing.Message != schedule.Message ||
				!equalJSON(existing.Headers, schedule.Headers) ||
				existing.CorrelationID.String != schedule.CorrelationID.String ||
				existing.Priority != schedule.Priority {
				return fmt.Errorf("%w: schedule occurrence", ErrIdempotencyConflict)
			}
		}
		if _, err := tx.ExecContext(ctx, tx.Rebind(`
			INSERT INTO message_deliveries
				(message_id, subscriber_id, status, delivery_count, failure_count, visible_at, priority, message_created_at)
			SELECT ?, id, 'pending', 0, 0, ?, ?, ? FROM topic_subscribers
			WHERE topic_id = ? AND deleted_at IS NULL
			ON CONFLICT (message_id, subscriber_id) DO NOTHING`),
			messageID, visibleAt, schedule.Priority, visibleAt, schedule.TopicID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, tx.Rebind(
			"UPDATE schedule_runs SET message_id = ? WHERE id = ?"), messageID, resultRun.ID); err != nil {
			return err
		}
		resultRun.MessageID = sql.NullString{String: messageID, Valid: true}
		if err := d.notifyTx(ctx, tx, "delivery:"+schedule.TopicID); err != nil {
			return err
		}
		if err := d.notifyTx(ctx, tx, "scheduler"); err != nil {
			return err
		}
		return d.advanceClaimedScheduleTx(ctx, tx, schedule, nextRunAt, advance, owner, claimed.FencingToken)
	})
	return resultRun, err
}

func (d *db) advanceClaimedScheduleTx(
	ctx context.Context,
	tx *sqlx.Tx,
	schedule Schedule,
	next time.Time,
	advance bool,
	owner string,
	fencingToken int64,
) error {
	if !advance {
		return nil
	}
	result, err := tx.ExecContext(ctx, tx.Rebind(`
		UPDATE schedules
		SET next_run_at = ?, owner_id = NULL, lease_expires_at = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND owner_id = ? AND fencing_token = ?`),
		next, schedule.ID, owner, fencingToken)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrScheduleLeaseLost
	}
	return nil
}
