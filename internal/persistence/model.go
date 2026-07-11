package persistence

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MaximumDeliveryLease = 12 * time.Hour
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
	InitialDelay string  `json:"initial_delay,omitempty"`
	MaxDelay     string  `json:"max_delay,omitempty"`
	Multiplier   float64 `json:"multiplier,omitempty"`
	Jitter       float64 `json:"jitter,omitempty"`
}

type SubscriberOptions struct {
	MaxAttempts        int         `json:"max_attempts"`
	VisibilityDuration string      `json:"visibility_duration"`
	DequeueBatchSize   int         `json:"dequeue_batch_size,omitempty"`
	RetryPolicy        RetryPolicy `json:"retry_policy,omitempty"`
}

func (options SubscriberOptions) normalized() SubscriberOptions {
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.VisibilityDuration == "" {
		options.VisibilityDuration = "5m"
	}
	if options.DequeueBatchSize <= 0 {
		options.DequeueBatchSize = 10
	}
	if options.RetryPolicy.InitialDelay == "" {
		options.RetryPolicy.InitialDelay = "1s"
	}
	if options.RetryPolicy.MaxDelay == "" {
		options.RetryPolicy.MaxDelay = "1h"
	}
	if options.RetryPolicy.Multiplier == 0 {
		options.RetryPolicy.Multiplier = 2
	}
	if options.RetryPolicy.Jitter == 0 {
		options.RetryPolicy.Jitter = 0.2
	}
	return options
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

type subscriberOptions struct {
	MaxAttempts        int
	VisibilityDuration time.Duration
	DequeueBatchSize   int
	RetryInitialDelay  time.Duration
	RetryMaxDelay      time.Duration
	RetryMultiplier    float64
	RetryJitter        float64
}

func parseSubscriberOptions(subscriber Subscriber) (subscriberOptions, error) {
	option := subscriber.Options.normalized()
	visibilityDuration, err := time.ParseDuration(option.VisibilityDuration)
	if err != nil || visibilityDuration <= 0 {
		return subscriberOptions{}, errors.New("subscriber visibility_duration must be positive")
	}
	if visibilityDuration > MaximumDeliveryLease {
		return subscriberOptions{}, errors.New("subscriber visibility_duration cannot exceed 12h")
	}
	if visibilityDuration < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber visibility_duration must be at least 1ms")
	}
	dequeueBatchSize := option.DequeueBatchSize
	if dequeueBatchSize <= 0 {
		dequeueBatchSize = 10
	}
	if dequeueBatchSize > 1000 {
		return subscriberOptions{}, errors.New("subscriber dequeue_batch_size exceeds 1000")
	}
	retryInitialDelay, err := time.ParseDuration(option.RetryPolicy.InitialDelay)
	if err != nil || retryInitialDelay < 0 {
		return subscriberOptions{}, errors.New("subscriber retry initial_delay must be non-negative")
	}
	if retryInitialDelay > 0 && retryInitialDelay < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber retry initial_delay must be zero or at least 1ms")
	}
	retryMaxDelay, err := time.ParseDuration(option.RetryPolicy.MaxDelay)
	if err != nil || retryMaxDelay < retryInitialDelay {
		return subscriberOptions{}, errors.New("subscriber retry max_delay must be at least initial_delay")
	}
	if retryMaxDelay > 0 && retryMaxDelay < time.Millisecond {
		return subscriberOptions{}, errors.New("subscriber retry max_delay must be zero or at least 1ms")
	}
	if math.IsNaN(option.RetryPolicy.Multiplier) || math.IsInf(option.RetryPolicy.Multiplier, 0) || option.RetryPolicy.Multiplier < 1 {
		return subscriberOptions{}, errors.New("subscriber retry multiplier must be at least 1")
	}
	if math.IsNaN(option.RetryPolicy.Jitter) || math.IsInf(option.RetryPolicy.Jitter, 0) || option.RetryPolicy.Jitter < 0 || option.RetryPolicy.Jitter > 1 {
		return subscriberOptions{}, errors.New("subscriber retry jitter must be between 0 and 1")
	}
	return subscriberOptions{
		MaxAttempts: option.MaxAttempts, VisibilityDuration: visibilityDuration, DequeueBatchSize: dequeueBatchSize,
		RetryInitialDelay: retryInitialDelay, RetryMaxDelay: retryMaxDelay,
		RetryMultiplier: option.RetryPolicy.Multiplier, RetryJitter: option.RetryPolicy.Jitter,
	}, nil
}

type SubscriberQueueStats struct {
	Pending   int `db:"pending"`
	Delivered int `db:"delivered"`
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
	TopicID        uuid.UUID
	MessageID      string
	Message        string
	Headers        []byte
	CorrelationID  string
	IdempotencyKey string
	Priority       int
	VisibleAt      time.Time
	CreatedAt      time.Time
}

type writeRequest = WriteRequest
