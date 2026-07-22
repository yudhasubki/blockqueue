package persistence

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/internal/subscriberconfig"
)

const (
	MaximumDeliveryLease = subscriberconfig.MaximumDeliveryLease
	MaxDeliveryTextBytes = 16 << 10
)

type Topic struct {
	ID        uuid.UUID  `db:"id"`
	Name      string     `db:"name"`
	Paused    bool       `db:"paused"`
	CreatedAt time.Time  `db:"created_at"`
	DeletedAt *time.Time `db:"deleted_at"`
}

type Topics []Topic

type TopicFilter struct {
	Names       []string
	WithDeleted bool
}

func (filter TopicFilter) filter(operator string) (string, map[string]any) {
	clauses := make([]string, 0, 2)
	arguments := make(map[string]any)
	if len(filter.Names) > 0 {
		arguments["name"] = filter.Names
		clauses = append(clauses, "name IN(:name)")
	}
	if filter.WithDeleted {
		clauses = append(clauses, "deleted_at IS NOT NULL")
	} else {
		clauses = append(clauses, "deleted_at IS NULL")
	}
	return strings.Join(clauses, " "+operator+" "), arguments
}

type RetryPolicy struct {
	InitialDelay  string  `json:"initial_delay,omitempty"`
	MaxDelay      string  `json:"max_delay,omitempty"`
	Multiplier    float64 `json:"multiplier,omitempty"`
	Jitter        float64 `json:"jitter,omitempty"`
	DisableJitter bool    `json:"disable_jitter,omitempty"`
}

type SubscriberOptions struct {
	MaxAttempts        int         `json:"max_attempts"`
	VisibilityDuration string      `json:"visibility_duration"`
	DequeueBatchSize   int         `json:"dequeue_batch_size,omitempty"`
	RetryPolicy        RetryPolicy `json:"retry_policy,omitempty"`
}

func (options SubscriberOptions) normalized() SubscriberOptions {
	return fromSubscriberConfigOptions(subscriberconfig.Normalize(toSubscriberConfigOptions(options)))
}

func toSubscriberConfigOptions(options SubscriberOptions) subscriberconfig.Options {
	return subscriberconfig.Options{
		MaxAttempts: options.MaxAttempts, VisibilityDuration: options.VisibilityDuration,
		DequeueBatchSize: options.DequeueBatchSize,
		RetryPolicy: subscriberconfig.RetryPolicy{
			InitialDelay: options.RetryPolicy.InitialDelay, MaxDelay: options.RetryPolicy.MaxDelay,
			Multiplier: options.RetryPolicy.Multiplier, Jitter: options.RetryPolicy.Jitter,
			DisableJitter: options.RetryPolicy.DisableJitter,
		},
	}
}

func fromSubscriberConfigOptions(options subscriberconfig.Options) SubscriberOptions {
	return SubscriberOptions{
		MaxAttempts: options.MaxAttempts, VisibilityDuration: options.VisibilityDuration,
		DequeueBatchSize: options.DequeueBatchSize,
		RetryPolicy: RetryPolicy{
			InitialDelay: options.RetryPolicy.InitialDelay, MaxDelay: options.RetryPolicy.MaxDelay,
			Multiplier: options.RetryPolicy.Multiplier, Jitter: options.RetryPolicy.Jitter,
			DisableJitter: options.RetryPolicy.DisableJitter,
		},
	}
}

func (options SubscriberOptions) Value() (driver.Value, error) {
	encoded, err := json.Marshal(options.normalized())
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

func (options *SubscriberOptions) Scan(source any) error {
	if source == nil {
		*options = SubscriberOptions{}.normalized()
		return nil
	}
	var encoded []byte
	switch value := source.(type) {
	case string:
		encoded = []byte(value)
	case []byte:
		encoded = value
	default:
		return fmt.Errorf("scan subscriber options from %T", source)
	}
	if err := json.Unmarshal(encoded, options); err != nil {
		return fmt.Errorf("decode subscriber options: %w", err)
	}
	*options = options.normalized()
	return nil
}

type Subscriber struct {
	ID        uuid.UUID         `db:"id"`
	TopicID   uuid.UUID         `db:"topic_id"`
	TopicName string            `db:"topic_name"`
	Name      string            `db:"name"`
	Options   SubscriberOptions `db:"option"`
	Paused    bool              `db:"paused"`
	CreatedAt time.Time         `db:"created_at"`
	DeletedAt *time.Time        `db:"deleted_at"`
}

type Subscribers []Subscriber

type SubscriberFilter struct {
	TopicIDs    []uuid.UUID
	Names       []string
	WithDeleted bool
}

func (filter SubscriberFilter) filter(operator string) (string, map[string]any) {
	clauses := make([]string, 0, 3)
	arguments := make(map[string]any)
	if len(filter.TopicIDs) > 0 {
		arguments["topic_id"] = filter.TopicIDs
		clauses = append(clauses, "topic_subscribers.topic_id IN(:topic_id)")
	}
	if len(filter.Names) > 0 {
		arguments["name"] = filter.Names
		clauses = append(clauses, "topic_subscribers.name IN(:name)")
	}
	if filter.WithDeleted {
		clauses = append(clauses, "topic_subscribers.deleted_at IS NOT NULL")
	} else {
		clauses = append(clauses, "topic_subscribers.deleted_at IS NULL")
	}
	return strings.Join(clauses, " "+operator+" "), arguments
}

type subscriberOptions = subscriberconfig.Parsed

func parseSubscriberOptions(subscriber Subscriber) (subscriberOptions, error) {
	return subscriberconfig.Parse(toSubscriberConfigOptions(subscriber.Options))
}

type SubscriberQueueStats struct {
	Pending   int `db:"pending"`
	Delivered int `db:"delivered"`
}

type SubscriberStatusRow struct {
	ID        string `db:"id"`
	Name      string `db:"name"`
	Pending   int    `db:"pending"`
	Delivered int    `db:"delivered"`
}

type Schedule struct {
	ID             string         `db:"id"`
	TopicID        string         `db:"topic_id"`
	Name           string         `db:"name"`
	CronExpression string         `db:"cron_expression"`
	Timezone       string         `db:"timezone"`
	Message        string         `db:"message"`
	Headers        string         `db:"headers"`
	CorrelationID  sql.NullString `db:"correlation_id"`
	Priority       int            `db:"priority"`
	MisfirePolicy  string         `db:"misfire_policy"`
	OverlapPolicy  string         `db:"overlap_policy"`
	Paused         bool           `db:"paused"`
	Version        int            `db:"version"`
	NextRunAt      time.Time      `db:"next_run_at"`
	OwnerID        sql.NullString `db:"owner_id"`
	LeaseExpiresAt sql.NullTime   `db:"lease_expires_at"`
	FencingToken   int64          `db:"fencing_token"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
	ClaimedAt      time.Time      `db:"-"`
}

type ScheduleRun struct {
	ID           string         `db:"id"`
	ScheduleID   string         `db:"schedule_id"`
	MessageID    sql.NullString `db:"message_id"`
	ScheduledFor time.Time      `db:"scheduled_for"`
	StartedAt    time.Time      `db:"started_at"`
	FinishedAt   sql.NullTime   `db:"finished_at"`
	Status       string         `db:"status"`
	Error        sql.NullString `db:"error"`
	CreatedAt    time.Time      `db:"created_at"`
}

type BatchAckItem struct {
	MessageID    string
	ReceiptToken string
}

type BatchNackItem struct {
	MessageID    string
	ReceiptToken string
	RetryDelay   time.Duration
	Error        string
}

type DeliveryError struct {
	ID           string    `db:"id"`
	MessageID    string    `db:"message_id"`
	SubscriberID string    `db:"subscriber_id"`
	FailureCount int       `db:"failure_count"`
	Error        string    `db:"error"`
	FailedAt     time.Time `db:"failed_at"`
}

type MessageDeliveryStatus struct {
	SubscriberID  string     `db:"subscriber_id"`
	Subscriber    string     `db:"subscriber"`
	Status        string     `db:"status"`
	DeliveryCount int        `db:"delivery_count"`
	FailureCount  int        `db:"failure_count"`
	VisibleAt     time.Time  `db:"visible_at"`
	ProcessedAt   *time.Time `db:"processed_at"`
	CancelledAt   *time.Time `db:"cancelled_at"`
	CancelReason  string     `db:"cancel_reason"`
}

type MessageStatus struct {
	ID             string
	TopicID        string
	Message        string
	Headers        map[string]string
	CorrelationID  string
	IdempotencyKey string
	Priority       int
	ScheduledAt    time.Time
	CreatedAt      time.Time
	Deliveries     []MessageDeliveryStatus
}

type WriteRequest struct {
	TopicID         uuid.UUID
	MessageID       string
	Message         string
	Headers         []byte
	CorrelationID   string
	IdempotencyKey  string
	IdempotencyHash string
	ScheduleMode    string
	ScheduleDelay   time.Duration
	Priority        int
	VisibleAt       time.Time
	CreatedAt       time.Time
}

type writeRequest = WriteRequest

// PersistWriteResult describes the storage-resolved identity of each request
// in input order. ScheduledAt is calculated from the database clock for
// immediate and relative-delay publishes, and is authoritative even when the
// caller owns the surrounding transaction and has not committed it yet.
type PersistWriteResult struct {
	Duplicates  []bool
	ScheduledAt []time.Time
}
