package blockqueue

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/internal/subscriberconfig"
)

// Topic identifies a fan-out stream. Names are unique among active topics.
type Topic struct {
	ID        uuid.UUID  `db:"id" json:"id"`
	Name      string     `db:"name" json:"name"`
	Paused    bool       `db:"paused" json:"paused"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

// Topics is a collection of queue topics.
type Topics []Topic

// TopicPage is one cursor-paginated page of topics.
type TopicPage struct {
	Topics     Topics `json:"topics"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// TopicFilter selects topics for GetTopics.
type TopicFilter struct {
	Names       []string
	WithDeleted bool
}

// Subscriber defines one independently leased delivery stream for a topic.
type Subscriber struct {
	ID        uuid.UUID         `db:"id" json:"id"`
	TopicID   uuid.UUID         `db:"topic_id" json:"topic_id"`
	TopicName string            `db:"topic_name" json:"topic_name,omitempty"`
	Name      string            `db:"name" json:"name"`
	Options   SubscriberOptions `db:"option" json:"options"`
	Paused    bool              `db:"paused" json:"paused"`
	CreatedAt time.Time         `db:"created_at" json:"created_at"`
	DeletedAt *time.Time        `db:"deleted_at" json:"deleted_at,omitempty"`
}

// Subscribers is a collection of topic subscribers.
type Subscribers []Subscriber

func (subscribers Subscribers) mapByTopic() map[uuid.UUID]Subscribers {
	result := make(map[uuid.UUID]Subscribers)
	for _, subscriber := range subscribers {
		result[subscriber.TopicID] = append(result[subscriber.TopicID], subscriber)
	}
	return result
}

type subscriberFilter struct {
	TopicIDs    []uuid.UUID
	Names       []string
	WithDeleted bool
}

// SubscriberOptions controls retries, lease visibility, and claim batching.
// Zero fields are normalized to safe defaults when the subscriber is created.
type SubscriberOptions struct {
	MaxAttempts        int         `json:"max_attempts"`
	VisibilityDuration string      `json:"visibility_duration"`
	DequeueBatchSize   int         `json:"dequeue_batch_size,omitempty"`
	RetryPolicy        RetryPolicy `json:"retry_policy,omitempty"`
}

// RetryPolicy controls the delay applied after a NACK or expired lease. Empty
// fields use the documented exponential-backoff defaults.
type RetryPolicy struct {
	InitialDelay  string  `json:"initial_delay,omitempty"`
	MaxDelay      string  `json:"max_delay,omitempty"`
	Multiplier    float64 `json:"multiplier,omitempty"`
	Jitter        float64 `json:"jitter,omitempty"`
	DisableJitter bool    `json:"disable_jitter,omitempty"`
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

// Value implements driver.Valuer using the normalized JSON representation.
func (options SubscriberOptions) Value() (driver.Value, error) {
	encoded, err := json.Marshal(options.normalized())
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

// Scan implements sql.Scanner for a persisted JSON options value.
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

// SubscriberStatus summarizes pending and currently leased work.
type SubscriberStatus struct {
	TopicID            uuid.UUID `json:"topic_id"`
	Name               string    `json:"name"`
	UnpublishedMessage int       `json:"unpublished_message"`
	UnackedMessage     int       `json:"unacked_message"`
}

// SubscriberStatuses is a collection of subscriber status summaries.
type SubscriberStatuses []SubscriberStatus

// SubscriberStatusPage is one cursor-paginated subscriber status page.
type SubscriberStatusPage struct {
	Subscribers SubscriberStatuses `json:"subscribers"`
	NextCursor  string             `json:"next_cursor,omitempty"`
}

// Message is a canonical publish request shared by all active subscribers.
type Message struct {
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Delay          string            `json:"delay,omitempty"`
	ScheduleAt     string            `json:"schedule_at,omitempty"`
}

// PublishReceipt reports a stable message identity and persistence state.
type PublishReceipt struct {
	MessageID   string    `json:"message_id"`
	State       string    `json:"state"`
	Duplicate   *bool     `json:"duplicate"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// PublishReceipts contains one receipt per input message, in input order.
type PublishReceipts []PublishReceipt

// MessageStatus is the canonical message and the current state of every
// subscriber delivery created with it.
type MessageStatus struct {
	ID             string                  `json:"id"`
	TopicID        string                  `json:"topic_id"`
	Message        string                  `json:"message"`
	Headers        map[string]string       `json:"headers,omitempty"`
	CorrelationID  string                  `json:"correlation_id,omitempty"`
	IdempotencyKey string                  `json:"idempotency_key,omitempty"`
	Priority       int                     `json:"priority"`
	ScheduledAt    time.Time               `json:"scheduled_at"`
	CreatedAt      time.Time               `json:"created_at"`
	Deliveries     []MessageDeliveryStatus `json:"deliveries"`
}

// MessageDeliveryStatus describes one subscriber's delivery state.
type MessageDeliveryStatus struct {
	SubscriberID  string     `db:"subscriber_id" json:"subscriber_id"`
	Subscriber    string     `db:"subscriber" json:"subscriber"`
	Status        string     `db:"status" json:"status"`
	DeliveryCount int        `db:"delivery_count" json:"delivery_count"`
	FailureCount  int        `db:"failure_count" json:"failure_count"`
	VisibleAt     time.Time  `db:"visible_at" json:"visible_at"`
	ProcessedAt   *time.Time `db:"processed_at" json:"processed_at,omitempty"`
	CancelledAt   *time.Time `db:"cancelled_at" json:"cancelled_at,omitempty"`
	CancelReason  string     `db:"cancel_reason" json:"cancel_reason,omitempty"`
}

// Delivery is one subscriber-specific view of a canonical message.
type Delivery struct {
	ID             string            `json:"id"`
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Status         string            `json:"status,omitempty"`
	DeliveryCount  int               `json:"delivery_count,omitempty"`
	FailureCount   int               `json:"failure_count,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	ReceiptToken   string            `json:"receipt_token,omitempty"`
	LeaseExpiresAt *time.Time        `json:"lease_expires_at,omitempty"`
	VisibleAt      time.Time         `json:"visible_at"`
	CreatedAt      time.Time         `json:"created_at"`
	CancelledAt    *time.Time        `json:"cancelled_at,omitempty"`
	CancelReason   string            `json:"cancel_reason,omitempty"`
}

// Deliveries is a collection of claimed or listed deliveries.
type Deliveries []Delivery

// DeliveryResult reports the per-item outcome of a batch operation.
type DeliveryResult struct {
	MessageID    string `json:"message_id"`
	SubscriberID string `json:"subscriber_id,omitempty"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

// DeliveryPage is one cursor-paginated page of active or dead-letter work.
type DeliveryPage struct {
	Messages   Deliveries `json:"messages"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// BatchAckItem identifies one receipt-fenced acknowledgement.
type BatchAckItem struct {
	MessageID    string
	ReceiptToken string
}

// BatchNackItem identifies one receipt-fenced failure and optional retry delay.
type BatchNackItem struct {
	MessageID    string
	ReceiptToken string
	RetryDelay   time.Duration
	Error        string
}

// DeliveryError is an append-only record of one NACK or lease expiry. A DLQ
// replay resets the delivery failure count but does not erase prior records.
type DeliveryError struct {
	ID           string    `db:"id" json:"id"`
	MessageID    string    `db:"message_id" json:"message_id"`
	SubscriberID string    `db:"subscriber_id" json:"subscriber_id"`
	FailureCount int       `db:"failure_count" json:"failure_count"`
	Error        string    `db:"error" json:"error"`
	FailedAt     time.Time `db:"failed_at" json:"failed_at"`
}

// DeliveryErrorPage is a cursor-paginated delivery failure history.
type DeliveryErrorPage struct {
	Errors     []DeliveryError `json:"errors"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// NewTopic creates a topic value with a new UUID.
func NewTopic(name string) Topic {
	return Topic{ID: uuid.New(), Name: name}
}

// NewSubscriber creates a normalized subscriber value with a new UUID.
func NewSubscriber(topic Topic, name string, options SubscriberOptions) Subscriber {
	return Subscriber{
		ID: uuid.New(), TopicID: topic.ID, Name: name, Options: options.normalized(),
	}
}
