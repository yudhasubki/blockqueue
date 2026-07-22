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
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

const (
	defaultScheduleLease    = 30 * time.Second
	defaultScheduleTimezone = "UTC"
	schedulerPassLimit      = 100
)

var (
	ErrScheduleNotFound  = persistence.ErrScheduleNotFound
	ErrScheduleVersion   = persistence.ErrScheduleVersion
	ErrScheduleOverlap   = persistence.ErrScheduleOverlap
	ErrScheduleLeaseLost = persistence.ErrScheduleLeaseLost
)

// Clock supplies scheduler time and is injectable for deterministic tests.
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
		input.Timezone = defaultScheduleTimezone
	}
	parsed, err := parseCron(input.CronExpression, input.Timezone)
	if err != nil {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: %v", ErrInvalidPublish, err)
	}
	if input.Priority < MinimumPriority || input.Priority > MaximumPriority || len(input.Message) > MaximumMessageBytes {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: invalid schedule message or priority", ErrInvalidPublish)
	}
	if len([]byte(input.CorrelationID)) > MaximumCorrelationIDBytes {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: correlation_id exceeds %d bytes", ErrInvalidPublish, MaximumCorrelationIDBytes)
	}
	if input.MisfirePolicy == "" {
		input.MisfirePolicy = ScheduleMisfirePolicyFireOnce
	}
	if input.MisfirePolicy != ScheduleMisfirePolicyFireOnce {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: unsupported misfire_policy", ErrInvalidPublish)
	}
	if input.OverlapPolicy == "" {
		input.OverlapPolicy = ScheduleOverlapPolicySkip
	}
	if input.OverlapPolicy != ScheduleOverlapPolicySkip {
		return input, cronExpression{}, time.Time{}, nil, fmt.Errorf("%w: unsupported overlap_policy", ErrInvalidPublish)
	}
	headers, err := json.Marshal(input.Headers)
	if err != nil || len(headers) > MaximumHeadersBytes {
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

// CreateSchedule persists a recurring cron publish and computes its next run.
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

// GetSchedule returns one schedule by ID within topic.
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

// ListSchedules returns every schedule for topic. Prefer ListSchedulesPage for
// bounded operator-facing enumeration.
func (q *Queue) ListSchedules(ctx context.Context, topic Topic) ([]Schedule, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return nil, ErrTopicNotFound
	}
	return q.db.listSchedules(ctx, runtime.id)
}

// ListSchedulesPage returns a bounded cursor page ordered by name and ID.
func (q *Queue) ListSchedulesPage(ctx context.Context, topic Topic, limit int, cursor string) (SchedulePage, error) {
	runtime, ok := q.getTopicRuntime(topic)
	if !ok {
		return SchedulePage{}, ErrTopicNotFound
	}
	limit = normalizedResourcePageLimit(limit)
	var afterName, afterID string
	if cursor != "" {
		var err error
		afterName, afterID, err = decodeResourceCursor(cursor)
		if err != nil {
			return SchedulePage{}, ErrInvalidCursor
		}
		if _, err := uuid.Parse(afterID); err != nil {
			return SchedulePage{}, ErrInvalidCursor
		}
	}
	rows, err := q.db.listSchedulesPage(ctx, runtime.id, limit+1, afterName, afterID)
	if err != nil {
		return SchedulePage{}, err
	}
	page := SchedulePage{Schedules: rows}
	if len(rows) > limit {
		last := rows[limit-1]
		page.Schedules = rows[:limit]
		page.NextCursor = encodeResourceCursor(last.Name, last.ID)
	}
	return page, nil
}

// UpdateSchedule replaces a schedule when expectedVersion matches.
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

// DeleteSchedule removes one recurring schedule.
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

// PauseSchedule changes whether future occurrences may be claimed.
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

// RunScheduleNow publishes an immediate occurrence. force bypasses overlap
// protection but preserves occurrence idempotency.
func (q *Queue) RunScheduleNow(ctx context.Context, topic Topic, scheduleID string, force bool) (ScheduleRun, error) {
	schedule, err := q.GetSchedule(ctx, topic, scheduleID)
	if err != nil {
		return ScheduleRun{}, err
	}
	scheduledFor := q.clock().Now()
	run, err := q.processScheduleOccurrence(ctx, schedule, scheduledFor, force, false)
	if err != nil && permanentScheduleOccurrenceError(err) {
		failed, recordErr := q.recordScheduleFailure(ctx, schedule, scheduledFor, false, err)
		if recordErr != nil {
			return ScheduleRun{}, errors.Join(err, recordErr)
		}
		return failed, err
	}
	return run, err
}

// ScheduleRunHistory returns newest occurrences first using an opaque cursor.
func (q *Queue) ScheduleRunHistory(ctx context.Context, topic Topic, scheduleID string, limit int, cursor string) (ScheduleRunPage, error) {
	if _, err := q.GetSchedule(ctx, topic, scheduleID); err != nil {
		return ScheduleRunPage{}, err
	}
	limit = normalizedResourcePageLimit(limit)
	var before time.Time
	var beforeID string
	if cursor != "" {
		when, id, err := decodeScheduleCursor(cursor)
		if err != nil {
			return ScheduleRunPage{}, ErrInvalidCursor
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
		nowHint := time.Time{}
		if q.options.Clock != nil {
			nowHint = clock.Now()
		}
		next, observedAt, exists, err := q.db.nextScheduleDue(q.serverCtx, nowHint)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			q.setSchedulerHealthy(false)
			slog.Error("scheduler next-run query failed", "error", err)
			if !waitForMaintenanceRetry(q.serverCtx, clock) {
				return
			}
			continue
		}
		q.setSchedulerHealthy(true)
		if !q.options.DisableMetrics {
			lag := 0.0
			if exists && observedAt.After(next) {
				lag = observedAt.Sub(next).Seconds()
			}
			metric.SetSchedulerDueLag(q.runtimeMetricID, lag)
		}
		// The database is authoritative. The one-second ceiling is the
		// multi-process fallback when LISTEN/NOTIFY is unavailable or lost.
		wait := time.Second
		if exists {
			wait = next.Sub(observedAt)
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
		started := time.Now()
		claimFailed := false
		yielded := false
		var claimedCount int64
		for claim := 0; claim < schedulerPassLimit; claim++ {
			claimNow := time.Time{}
			if q.options.Clock != nil {
				claimNow = clock.Now()
			}
			schedule, claimed, err := q.db.claimDueSchedule(q.serverCtx, q.schedulerOwner, claimNow, defaultScheduleLease)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				q.setSchedulerHealthy(false)
				slog.Error("scheduler claim failed", "error", err)
				claimFailed = true
				break
			}
			if !claimed {
				break
			}
			claimedCount++
			if !q.options.DisableMetrics {
				metric.SchedulerOperations.WithLabelValues(
					metric.SchedulerOperationClaimed, metric.OutcomeSuccess,
				).Inc()
			}
			if _, err := q.processScheduleOccurrence(q.serverCtx, schedule, schedule.NextRunAt, false, true); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				ownershipLost := errors.Is(err, ErrScheduleLeaseLost)
				permanentFailureRecorded := false
				if !ownershipLost && permanentScheduleOccurrenceError(err) {
					_, recordErr := q.recordScheduleFailure(
						q.serverCtx, schedule, schedule.NextRunAt, true, err,
					)
					if recordErr == nil {
						permanentFailureRecorded = true
					} else {
						err = errors.Join(err, recordErr)
					}
				}
				if !q.options.DisableMetrics {
					metric.SchedulerOperations.WithLabelValues(
						metric.SchedulerOperationPublished, metric.OutcomeFailed,
					).Inc()
				}
				if scheduleOccurrenceFailureIsUnhealthy(err, permanentFailureRecorded) {
					q.setSchedulerHealthy(false)
				}
				if ownershipLost {
					slog.Debug("scheduler ownership lost before occurrence commit",
						"schedule_id", schedule.ID, "error", err)
				} else {
					slog.Error("scheduler publish failed", "schedule_id", schedule.ID, "error", err)
				}
			} else {
				if !q.options.DisableMetrics {
					metric.SchedulerOperations.WithLabelValues(
						metric.SchedulerOperationPublished, metric.OutcomeSuccess,
					).Inc()
				}
			}
			yielded = claim+1 == schedulerPassLimit
		}
		q.observeMaintenancePass(metric.MaintenanceOperationScheduler, started, claimedCount, claimFailed, yielded)
		if claimFailed && !waitForMaintenanceRetry(q.serverCtx, clock) {
			return
		}
		if yielded && !waitForMaintenanceYield(q.serverCtx, clock) {
			return
		}
	}
}

func permanentScheduleOccurrenceError(err error) bool {
	return errors.Is(err, ErrNoActiveSubscriber) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrInvalidPublish)
}

func scheduleOccurrenceFailureIsUnhealthy(err error, permanentFailureRecorded bool) bool {
	if errors.Is(err, ErrScheduleLeaseLost) {
		return false
	}
	return !permanentFailureRecorded || !permanentScheduleOccurrenceError(err)
}

func (q *Queue) recordScheduleFailure(
	ctx context.Context,
	schedule Schedule,
	scheduledFor time.Time,
	advance bool,
	failure error,
) (ScheduleRun, error) {
	next := time.Time{}
	if advance {
		parsed, err := parseCron(schedule.CronExpression, schedule.Timezone)
		if err != nil {
			return ScheduleRun{}, err
		}
		now := q.clock().Now()
		if q.options.Clock == nil && !schedule.claimedAt.IsZero() {
			now = schedule.claimedAt
		}
		next = parsed.Next(now)
	}
	run, err := q.db.failScheduleOccurrence(
		ctx, schedule, scheduledFor, next, advance, q.schedulerOwner, failure.Error(),
	)
	if err == nil {
		q.signalScheduler()
	}
	return run, err
}

func (q *Queue) processScheduleOccurrence(ctx context.Context, schedule Schedule, scheduledFor time.Time, force, advance bool) (ScheduleRun, error) {
	now := q.clock().Now()
	if q.options.Clock == nil && !schedule.claimedAt.IsZero() {
		now = schedule.claimedAt
	}
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
