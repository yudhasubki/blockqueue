package blockqueue

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

const (
	defaultScheduleLease = 30 * time.Second
)

var (
	ErrScheduleNotFound  = errors.New("schedule not found")
	ErrScheduleVersion   = errors.New("stale schedule version")
	ErrScheduleOverlap   = errors.New("previous schedule run is still active")
	ErrScheduleLeaseLost = errors.New("schedule lease lost")
)

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now().UTC() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

func (q *Queue) clock() Clock {
	if q.options.Clock != nil {
		return q.options.Clock
	}
	return realClock{}
}

func validateScheduleInput(input ScheduleInput, now time.Time) (ScheduleInput, cronExpression, time.Time, []byte, error) {
	if input.Name == "" || len(input.Name) > 150 {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: schedule name is required and must be <=150 bytes", ErrInvalidPublish)
	}
	if input.Timezone == "" {
		input.Timezone = "UTC"
	}
	parsed, err := parseCron(input.CronExpression, input.Timezone)
	if err != nil {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: %v", ErrInvalidPublish, err)
	}
	if input.Priority < -1000 || input.Priority > 1000 || len(input.Message) > 1<<20 {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: invalid schedule message or priority", ErrInvalidPublish)
	}
	if len([]byte(input.CorrelationID)) > 255 {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: correlation_id exceeds 255 bytes", ErrInvalidPublish)
	}
	if input.MisfirePolicy == "" {
		input.MisfirePolicy = "fire_once"
	}
	if input.MisfirePolicy != "fire_once" {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: unsupported misfire_policy", ErrInvalidPublish)
	}
	if input.OverlapPolicy == "" {
		input.OverlapPolicy = "skip"
	}
	if input.OverlapPolicy != "skip" {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: unsupported overlap_policy", ErrInvalidPublish)
	}
	headers, err := json.Marshal(input.Headers)
	if err != nil || len(headers) > 16<<10 {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: invalid schedule headers", ErrInvalidPublish)
	}
	if input.Headers == nil {
		headers = []byte("{}")
	}
	next := parsed.Next(now)
	if next.IsZero() {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: cron has no next occurrence", ErrInvalidPublish)
	}
	return input, parsed, next, headers, nil
}

func (q *Queue) CreateSchedule(ctx context.Context, topic Topic, input ScheduleInput) (Schedule, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return Schedule{}, ErrTopicNotFound
	}
	input, _, next, headers, err := validateScheduleInput(input, q.clock().Now())
	if err != nil {
		return Schedule{}, err
	}
	existing, err := q.db.scheduleNameExists(ctx, runtime.id, input.Name)
	if err != nil {
		return Schedule{}, err
	}
	if existing {
		return Schedule{}, ErrResourceConflict
	}
	schedule := Schedule{
		ID: uuid.NewString(), TopicID: runtime.id.String(), Name: input.Name,
		CronExpression: input.CronExpression, Timezone: input.Timezone,
		Message: input.Message, Headers: string(headers),
		CorrelationID: sql.NullString{String: input.CorrelationID, Valid: input.CorrelationID != ""},
		Priority:      input.Priority, MisfirePolicy: input.MisfirePolicy,
		OverlapPolicy: input.OverlapPolicy, Version: 1, NextRunAt: next,
	}
	if err := q.db.createSchedule(ctx, schedule); err != nil {
		return Schedule{}, err
	}
	q.signalScheduler()
	return q.GetSchedule(ctx, topic, schedule.ID)
}

func (q *Queue) GetSchedule(ctx context.Context, topic Topic, scheduleID string) (Schedule, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return Schedule{}, ErrTopicNotFound
	}
	if _, err := uuid.Parse(scheduleID); err != nil {
		return Schedule{}, ErrScheduleNotFound
	}
	return q.db.getSchedule(ctx, runtime.id, scheduleID)
}

func (q *Queue) ListSchedules(ctx context.Context, topic Topic) ([]Schedule, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return nil, ErrTopicNotFound
	}
	return q.db.listSchedules(ctx, runtime.id)
}

func (q *Queue) UpdateSchedule(ctx context.Context, topic Topic, scheduleID string, expectedVersion int, input ScheduleInput) (Schedule, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return Schedule{}, ErrTopicNotFound
	}
	if _, err := uuid.Parse(scheduleID); err != nil {
		return Schedule{}, ErrScheduleNotFound
	}
	input, _, next, headers, err := validateScheduleInput(input, q.clock().Now())
	if err != nil {
		return Schedule{}, err
	}
	updated := Schedule{
		Name: input.Name, CronExpression: input.CronExpression, Timezone: input.Timezone,
		Message: input.Message, Headers: string(headers),
		CorrelationID: sql.NullString{String: input.CorrelationID, Valid: input.CorrelationID != ""},
		Priority:      input.Priority, MisfirePolicy: input.MisfirePolicy,
		OverlapPolicy: input.OverlapPolicy, NextRunAt: next,
	}
	if err := q.db.updateSchedule(ctx, runtime.id, scheduleID, expectedVersion, updated); err != nil {
		return Schedule{}, err
	}
	q.signalScheduler()
	return q.GetSchedule(ctx, topic, scheduleID)
}

func (q *Queue) DeleteSchedule(ctx context.Context, topic Topic, scheduleID string) error {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return ErrTopicNotFound
	}
	if _, err := uuid.Parse(scheduleID); err != nil {
		return ErrScheduleNotFound
	}
	if err := q.db.deleteSchedule(ctx, runtime.id, scheduleID); err != nil {
		return err
	}
	q.signalScheduler()
	return nil
}

func (q *Queue) PauseSchedule(ctx context.Context, topic Topic, scheduleID string, paused bool) error {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return ErrTopicNotFound
	}
	if _, err := uuid.Parse(scheduleID); err != nil {
		return ErrScheduleNotFound
	}
	if err := q.db.setSchedulePaused(ctx, runtime.id, scheduleID, paused); err != nil {
		return err
	}
	q.signalScheduler()
	return nil
}

func (q *Queue) RunScheduleNow(ctx context.Context, topic Topic, scheduleID string, force bool) (ScheduleRun, error) {
	schedule, err := q.GetSchedule(ctx, topic, scheduleID)
	if err != nil {
		return ScheduleRun{}, err
	}
	return q.processScheduleOccurrence(ctx, schedule, q.clock().Now(), force, false)
}

func (q *Queue) ScheduleRunHistory(ctx context.Context, topic Topic, scheduleID string, limit int, cursor string) (ScheduleRunPage, error) {
	if _, err := q.GetSchedule(ctx, topic, scheduleID); err != nil {
		return ScheduleRunPage{}, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var before time.Time
	var beforeID string
	if cursor != "" {
		when, id, err := decodeScheduleCursor(cursor)
		if err != nil {
			return ScheduleRunPage{}, fmt.Errorf("%w: invalid cursor", ErrInvalidPublish)
		}
		before, beforeID = when, id
	}
	runs, err := q.db.listScheduleRuns(ctx, scheduleID, limit+1, before, beforeID)
	if err != nil {
		return ScheduleRunPage{}, err
	}
	page := ScheduleRunPage{Runs: runs}
	if len(runs) > limit {
		last := runs[limit-1]
		page.Runs = runs[:limit]
		page.NextCursor = encodeScheduleCursor(last.ScheduledFor, last.ID)
	}
	return page, nil
}

func (q *Queue) startScheduler() {
	clock := q.clock()
	for {
		next, exists, err := q.db.nextScheduleDue(q.serverCtx, clock.Now())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			q.schedulerHealthy.Store(false)
			slog.Error("scheduler next-run query failed", "error", err)
			if !waitForMaintenanceRetry(q.serverCtx, clock) {
				return
			}
			continue
		}
		q.schedulerHealthy.Store(true)
		// The database is authoritative. The one-second ceiling is the
		// multi-process fallback when LISTEN/NOTIFY is unavailable or lost.
		wait := time.Second
		if exists {
			wait = next.Sub(clock.Now())
			if wait < 0 {
				wait = 0
			}
			if wait > time.Second {
				wait = time.Second
			}
		}
		select {
		case <-q.serverCtx.Done():
			return
		case <-q.schedulerSignal:
			continue
		case <-clock.After(wait):
		}
		claimFailed := false
		for {
			schedule, claimed, err := q.db.claimDueSchedule(q.serverCtx, q.schedulerOwner, clock.Now(), defaultScheduleLease)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				q.schedulerHealthy.Store(false)
				slog.Error("scheduler claim failed", "error", err)
				claimFailed = true
				break
			}
			if !claimed {
				break
			}
			if !q.options.DisableMetrics {
				metric.SchedulerOperations.WithLabelValues("claimed", "success").Inc()
			}
			if _, err := q.processScheduleOccurrence(q.serverCtx, schedule, schedule.NextRunAt, false, true); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if !q.options.DisableMetrics {
					metric.SchedulerOperations.WithLabelValues("published", "failed").Inc()
				}
				q.schedulerHealthy.Store(false)
				slog.Error("scheduler publish failed", "schedule_id", schedule.ID, "error", err)
			} else {
				if !q.options.DisableMetrics {
					metric.SchedulerOperations.WithLabelValues("published", "success").Inc()
				}
			}
		}
		if claimFailed && !waitForMaintenanceRetry(q.serverCtx, clock) {
			return
		}
	}
}

func (q *Queue) processScheduleOccurrence(ctx context.Context, schedule Schedule, scheduledFor time.Time, force, advance bool) (ScheduleRun, error) {
	now := q.clock().Now()
	next := time.Time{}
	if advance {
		parsed, err := parseCron(schedule.CronExpression, schedule.Timezone)
		if err != nil {
			return ScheduleRun{}, err
		}
		next = parsed.Next(now)
	}
	run, err := q.db.persistScheduleOccurrence(ctx, schedule, scheduledFor, next, force, advance, q.schedulerOwner)
	if err != nil {
		return ScheduleRun{}, err
	}
	if topicID, parseErr := uuid.Parse(schedule.TopicID); parseErr == nil {
		q.notify(topicID)
	}
	q.signalScheduler()
	return run, nil
}

func (q *Queue) signalScheduler() {
	select {
	case q.schedulerSignal <- struct{}{}:
	default:
	}
}

func encodeScheduleCursor(when time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(when.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeScheduleCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	when, err := time.Parse(time.RFC3339Nano, parts[0])
	return when, parts[1], err
}
