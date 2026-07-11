package blockqueue

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Topic struct {
	ID        uuid.UUID  `db:"id" json:"id"`
	Name      string     `db:"name" json:"name"`
	Paused    bool       `db:"paused" json:"paused"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
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

func (filter subscriberFilter) filter(operator string) (string, map[string]any) {
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

type SubscriberOptions struct {
	MaxAttempts        int    `json:"max_attempts"`
	VisibilityDuration string `json:"visibility_duration"`
	DequeueBatchSize   int    `json:"dequeue_batch_size,omitempty"`
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

type SubscriberStatus struct {
	TopicID            uuid.UUID `json:"topic_id"`
	Name               string    `json:"name"`
	UnpublishedMessage int       `json:"unpublished_message"`
	UnackedMessage     int       `json:"unacked_message"`
}

type SubscriberStatuses []SubscriberStatus

type Message struct {
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Delay          string            `json:"delay,omitempty"`
	ScheduleAt     string            `json:"schedule_at,omitempty"`
}

type PublishReceipt struct {
	MessageID   string    `json:"message_id"`
	State       string    `json:"state"`
	Duplicate   *bool     `json:"duplicate"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type PublishReceipts []PublishReceipt

type Delivery struct {
	ID             string            `json:"id"`
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Status         string            `json:"status,omitempty"`
	RetryCount     int               `json:"retry_count,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	ReceiptToken   string            `json:"receipt_token,omitempty"`
	LeaseExpiresAt *time.Time        `json:"lease_expires_at,omitempty"`
	VisibleAt      time.Time         `json:"visible_at"`
	CreatedAt      time.Time         `json:"created_at"`
}

type Deliveries []Delivery

type DeliveryResult struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

type DeliveryPage struct {
	Messages   Deliveries `json:"messages"`
	NextCursor string     `json:"next_cursor,omitempty"`
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

func NewTopic(name string) Topic {
	return Topic{ID: uuid.New(), Name: name}
}

func NewSubscriber(topic Topic, name string, options SubscriberOptions) Subscriber {
	return Subscriber{
		ID: uuid.New(), TopicID: topic.ID, Name: name, Options: options.normalized(),
	}
}
