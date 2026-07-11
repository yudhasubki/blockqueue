package httpapi

import (
	"context"
	"time"

	"github.com/yudhasubki/blockqueue"
)

// Service is the application port consumed by the HTTP transport.
type Service interface {
	ListTopics(context.Context, int, string) (blockqueue.TopicPage, error)
	GetTopic(string) (blockqueue.Topic, bool)
	CreateTopic(context.Context, blockqueue.Topic, blockqueue.Subscribers) error
	DeleteTopic(context.Context, blockqueue.Topic) error
	ListSubscriberStatuses(context.Context, blockqueue.Topic, int, string) (blockqueue.SubscriberStatusPage, error)
	CreateSubscribers(context.Context, blockqueue.Topic, blockqueue.Subscribers) error
	DeleteSubscriber(context.Context, blockqueue.Topic, string) error
	PublishAsync(context.Context, blockqueue.Topic, blockqueue.Message) (blockqueue.PublishReceipt, error)
	PublishDurable(context.Context, blockqueue.Topic, blockqueue.Message) (blockqueue.PublishReceipt, error)
	BatchPublishAsync(context.Context, blockqueue.Topic, []blockqueue.Message) (blockqueue.PublishReceipts, error)
	BatchPublishDurable(context.Context, blockqueue.Topic, []blockqueue.Message) (blockqueue.PublishReceipts, error)
	GetMessageStatus(context.Context, blockqueue.Topic, string) (blockqueue.MessageStatus, error)
	ClaimWait(context.Context, blockqueue.Topic, string, int, time.Duration) (blockqueue.Deliveries, error)
	AckDelivery(context.Context, blockqueue.Topic, string, string, string) error
	NackDelivery(context.Context, blockqueue.Topic, string, string, string, time.Duration, string) error
	ExtendLease(context.Context, blockqueue.Topic, string, string, string, time.Duration) (time.Time, error)
	BatchAckDeliveries(context.Context, blockqueue.Topic, string, []blockqueue.BatchAckItem) []blockqueue.DeliveryResult
	BatchNackDeliveries(context.Context, blockqueue.Topic, string, []blockqueue.BatchNackItem) []blockqueue.DeliveryResult
	SnoozeDelivery(context.Context, blockqueue.Topic, string, string, string, time.Duration) (time.Time, error)
	CancelDelivery(context.Context, blockqueue.Topic, string, string, string) error
	CancelMessage(context.Context, blockqueue.Topic, string, string) ([]blockqueue.DeliveryResult, error)
	DeliveryErrors(context.Context, blockqueue.Topic, string, string, int, string) (blockqueue.DeliveryErrorPage, error)
	ListDeliveries(context.Context, blockqueue.Topic, string, bool, int, string) (blockqueue.DeliveryPage, error)
	ReplayDeadLetters(context.Context, blockqueue.Topic, string, []string) []blockqueue.DeliveryResult
	PauseTopic(context.Context, blockqueue.Topic) error
	ResumeTopic(context.Context, blockqueue.Topic) error
	PauseSubscriber(context.Context, blockqueue.Topic, string) error
	ResumeSubscriber(context.Context, blockqueue.Topic, string) error
	ListSchedulesPage(context.Context, blockqueue.Topic, int, string) (blockqueue.SchedulePage, error)
	CreateSchedule(context.Context, blockqueue.Topic, blockqueue.ScheduleInput) (blockqueue.Schedule, error)
	GetSchedule(context.Context, blockqueue.Topic, string) (blockqueue.Schedule, error)
	UpdateSchedule(context.Context, blockqueue.Topic, string, int, blockqueue.ScheduleInput) (blockqueue.Schedule, error)
	DeleteSchedule(context.Context, blockqueue.Topic, string) error
	PauseSchedule(context.Context, blockqueue.Topic, string, bool) error
	RunScheduleNow(context.Context, blockqueue.Topic, string, bool) (blockqueue.ScheduleRun, error)
	ScheduleRunHistory(context.Context, blockqueue.Topic, string, int, string) (blockqueue.ScheduleRunPage, error)
}

type ErrorMapper func(error) (status int, code, message string)
