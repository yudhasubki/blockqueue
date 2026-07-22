package blockqueue

import (
	"database/sql"
	"encoding/json"
	"time"
)

// ScheduleInput defines a recurring five-field cron publish.
type ScheduleInput struct {
	Name           string            `json:"name"`
	CronExpression string            `json:"cron"`
	Timezone       string            `json:"timezone,omitempty"`
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	MisfirePolicy  string            `json:"misfire_policy,omitempty"`
	OverlapPolicy  string            `json:"overlap_policy,omitempty"`
}

// Schedule is the persisted scheduler definition and its next occurrence.
type Schedule struct {
	ID             string         `db:"id" json:"id"`
	TopicID        string         `db:"topic_id" json:"topic_id"`
	Name           string         `db:"name" json:"name"`
	CronExpression string         `db:"cron_expression" json:"cron"`
	Timezone       string         `db:"timezone" json:"timezone"`
	Message        string         `db:"message" json:"message"`
	Headers        string         `db:"headers" json:"-"`
	CorrelationID  sql.NullString `db:"correlation_id" json:"-"`
	Priority       int            `db:"priority" json:"priority"`
	MisfirePolicy  string         `db:"misfire_policy" json:"misfire_policy"`
	OverlapPolicy  string         `db:"overlap_policy" json:"overlap_policy"`
	Paused         bool           `db:"paused" json:"paused"`
	Version        int            `db:"version" json:"version"`
	NextRunAt      time.Time      `db:"next_run_at" json:"next_run_at"`
	OwnerID        sql.NullString `db:"owner_id" json:"-"`
	LeaseExpiresAt sql.NullTime   `db:"lease_expires_at" json:"-"`
	FencingToken   int64          `db:"fencing_token" json:"-"`
	CreatedAt      time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at" json:"updated_at"`
	claimedAt      time.Time
}

// SchedulePage is one cursor-paginated page of schedules.
type SchedulePage struct {
	Schedules  []Schedule `json:"schedules"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// PublicHeaders decodes the schedule's persisted JSON headers.
func (schedule Schedule) PublicHeaders() map[string]string {
	headers := make(map[string]string)
	_ = json.Unmarshal([]byte(schedule.Headers), &headers)
	return headers
}

// MarshalJSON exposes decoded headers and optional correlation data.
func (schedule Schedule) MarshalJSON() ([]byte, error) {
	type alias Schedule
	return json.Marshal(struct {
		alias
		Headers       map[string]string `json:"headers"`
		CorrelationID string            `json:"correlation_id,omitempty"`
	}{alias: alias(schedule), Headers: schedule.PublicHeaders(), CorrelationID: schedule.CorrelationID.String})
}

// ScheduleRun records one scheduled occurrence and its terminal outcome.
type ScheduleRun struct {
	ID           string         `db:"id" json:"id"`
	ScheduleID   string         `db:"schedule_id" json:"schedule_id"`
	MessageID    sql.NullString `db:"message_id" json:"-"`
	ScheduledFor time.Time      `db:"scheduled_for" json:"scheduled_for"`
	StartedAt    time.Time      `db:"started_at" json:"started_at"`
	FinishedAt   sql.NullTime   `db:"finished_at" json:"-"`
	Status       string         `db:"status" json:"status"`
	Error        sql.NullString `db:"error" json:"-"`
	CreatedAt    time.Time      `db:"created_at" json:"created_at"`
}

// MarshalJSON exposes nullable run fields as optional JSON strings.
func (run ScheduleRun) MarshalJSON() ([]byte, error) {
	type alias ScheduleRun
	result := struct {
		alias
		MessageID  string `json:"message_id,omitempty"`
		FinishedAt string `json:"finished_at,omitempty"`
		Error      string `json:"error,omitempty"`
	}{alias: alias(run), MessageID: run.MessageID.String, Error: run.Error.String}
	if run.FinishedAt.Valid {
		result.FinishedAt = run.FinishedAt.Time.Format(time.RFC3339Nano)
	}
	return json.Marshal(result)
}

// ScheduleRunPage is one cursor-paginated page of occurrence history.
type ScheduleRunPage struct {
	Runs       []ScheduleRun `json:"runs"`
	NextCursor string        `json:"next_cursor,omitempty"`
}
