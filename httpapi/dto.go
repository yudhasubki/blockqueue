package httpapi

import (
	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue"
)

type PublishRequest struct {
	Message        string            `json:"message"`
	Headers        map[string]string `json:"headers,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Delay          string            `json:"delay,omitempty"`
	ScheduleAt     string            `json:"schedule_at,omitempty"`
}

func (r PublishRequest) command() blockqueue.Message {
	return blockqueue.Message{
		Message:        r.Message,
		Headers:        r.Headers,
		CorrelationID:  r.CorrelationID,
		IdempotencyKey: r.IdempotencyKey,
		Priority:       r.Priority,
		Delay:          r.Delay,
		ScheduleAt:     r.ScheduleAt,
	}
}

type SubscriberOptions struct {
	MaxAttempts        int                    `json:"max_attempts"`
	VisibilityDuration string                 `json:"visibility_duration"`
	DequeueBatchSize   int                    `json:"dequeue_batch_size,omitempty"`
	RetryPolicy        blockqueue.RetryPolicy `json:"retry_policy,omitempty"`
}

type SubscriberRequest struct {
	Name   string            `json:"name"`
	Option SubscriberOptions `json:"option"`
}

type TopicRequest struct {
	Name        string              `json:"name"`
	Subscribers []SubscriberRequest `json:"subscribers"`
}

func (r TopicRequest) domain() (blockqueue.Topic, blockqueue.Subscribers) {
	topic := blockqueue.Topic{ID: uuid.New(), Name: r.Name}
	return topic, subscriberCommands(r.Subscribers, topic.ID)
}

func subscriberCommands(requests []SubscriberRequest, topicID uuid.UUID) blockqueue.Subscribers {
	result := make(blockqueue.Subscribers, 0, len(requests))
	for _, request := range requests {
		options := blockqueue.SubscriberOptions{
			MaxAttempts:        request.Option.MaxAttempts,
			VisibilityDuration: request.Option.VisibilityDuration,
			DequeueBatchSize:   request.Option.DequeueBatchSize,
			RetryPolicy:        request.Option.RetryPolicy,
		}
		result = append(result, blockqueue.Subscriber{ID: uuid.New(), TopicID: topicID, Name: request.Name, Options: options})
	}
	return result
}

type ReceiptRequest struct {
	ReceiptToken string `json:"receipt_token"`
}

type NackRequest struct {
	ReceiptToken string `json:"receipt_token"`
	RetryDelay   string `json:"retry_delay,omitempty"`
	Error        string `json:"error,omitempty"`
}

type LeaseRequest struct {
	ReceiptToken string `json:"receipt_token"`
	Extension    string `json:"extension,omitempty"`
}

type SnoozeRequest struct {
	ReceiptToken string `json:"receipt_token"`
	Delay        string `json:"delay"`
}

type CancelRequest struct {
	Reason string `json:"reason,omitempty"`
}

type BatchReceiptRequest struct {
	MessageID    string `json:"message_id"`
	ReceiptToken string `json:"receipt_token"`
}

type BatchNackRequest struct {
	MessageID    string `json:"message_id"`
	ReceiptToken string `json:"receipt_token"`
	RetryDelay   string `json:"retry_delay,omitempty"`
	Error        string `json:"error,omitempty"`
}

type ReplayRequest struct {
	MessageIDs []string `json:"message_ids"`
}

type ScheduleRequest struct {
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

func (r ScheduleRequest) command() blockqueue.ScheduleInput {
	return blockqueue.ScheduleInput{
		Name:           r.Name,
		CronExpression: r.CronExpression,
		Timezone:       r.Timezone,
		Message:        r.Message,
		Headers:        r.Headers,
		CorrelationID:  r.CorrelationID,
		Priority:       r.Priority,
		MisfirePolicy:  r.MisfirePolicy,
		OverlapPolicy:  r.OverlapPolicy,
	}
}

type ScheduleUpdateRequest struct {
	Version int `json:"version"`
	ScheduleRequest
}
