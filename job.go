package blockqueue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"gopkg.in/guregu/null.v4"
)

type Job[V chan io.ResponseMessages] struct {
	Id   uuid.UUID
	Name string

	db         *db
	ctx        context.Context
	cancelFunc context.CancelFunc
	mtx        *cas.SpinLock
	listeners  map[uuid.UUID]*Listener[V]
	metric     *jobMetric
}

type jobMetric struct {
	message *prometheus.CounterVec
}

func newJob[V chan io.ResponseMessages](serverCtx context.Context, topic core.Topic, database *db) (*Job[V], error) {
	ctx, cancel := context.WithCancel(serverCtx)
	job := &Job[V]{
		Id:         topic.Id,
		Name:       topic.Name,
		db:         database,
		cancelFunc: cancel,
		ctx:        ctx,
		mtx:        cas.New(),
		metric: &jobMetric{
			message: metric.MessagePublishedTopic(topic.Name),
		},
	}
	prometheus.Register(job.metric.message)

	subscribers, err := database.getSubscribers(serverCtx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return &Job[V]{}, err
	}

	listeners := make(map[uuid.UUID]*Listener[V])
	for _, subscriber := range subscribers {
		subscriberInfo, err := parseSubscriberInfo(subscriber)
		if err != nil {
			return &Job[V]{}, err
		}

		listener, err := newListener[V](serverCtx, topic.Name, subscriberInfo, database)
		if err != nil {
			return &Job[V]{}, err
		}

		listeners[subscriber.Id] = listener
	}
	job.listeners = listeners

	// Start context watcher for graceful shutdown
	go job.watchContext()

	return job, nil
}

// parseSubscriberInfo parses subscriber option into SubscriberInfo
func parseSubscriberInfo(subscriber core.Subscriber) (SubscriberInfo, error) {
	option := core.SubscriberOpt{}
	err := json.Unmarshal(subscriber.Option, &option)
	if err != nil {
		return SubscriberInfo{}, err
	}

	visibilityDuration, err := time.ParseDuration(option.VisibilityDuration)
	if err != nil {
		return SubscriberInfo{}, err
	}

	return SubscriberInfo{
		Id:                 subscriber.Id,
		Name:               subscriber.Name,
		MaxAttempts:        option.MaxAttempts,
		VisibilityDuration: visibilityDuration,
	}, nil
}

func (job *Job[V]) watchContext() {
	<-job.ctx.Done()
	slog.Info(
		"signal cancel received. job entering shutdown status.",
		logPrefixTopic, job.Name,
	)
	job.close()
}

func (job *Job[V]) ackMessage(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return ErrListenerNotFound
	}

	return listener.deleteRetryMessage(messageId)
}

func (job *Job[V]) ackMessages(ctx context.Context, topic core.Topic, subscriberName string, messageIds []string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	_, exist := job.getListeners(subscriber.Id)
	if !exist {
		return ErrListenerNotFound
	}

	return job.db.ackSubscriberMessages(ctx, subscriber.Id, messageIds)
}

func (job *Job[V]) addListener(ctx context.Context, topic core.Topic) error {
	subscribers, err := job.db.getSubscribers(ctx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return err
	}

	for _, subscriber := range subscribers {
		if _, exist := job.listeners[subscriber.Id]; !exist {
			subscriberInfo, err := parseSubscriberInfo(subscriber)
			if err != nil {
				return err
			}

			listener, err := newListener[V](job.ctx, topic.Name, subscriberInfo, job.db)
			if err != nil {
				return err
			}
			job.listeners[subscriber.Id] = listener
		}
	}

	return nil
}

func (job *Job[V]) deleteListener(ctx context.Context, topic core.Topic, subscriberName string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return ErrListenerNotFound
	}

	job.mtx.Lock()
	defer job.mtx.Unlock()

	err = job.db.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return job.db.deleteTxSubscribers(ctx, tx, core.Subscriber{
			Name:      listener.Id,
			TopicId:   job.Id,
			DeletedAt: null.StringFrom(time.Now().Format("2006-01-02 15:04:05")),
		})
	})
	if err != nil {
		return err
	}

	// Invalidate subscriber cache for this topic
	InvalidateSubscriberCache(job.Id)

	listener.remove()
	delete(job.listeners, subscriber.Id)

	return nil
}

func (job *Job[V]) getListenersStatus(ctx context.Context, topic core.Topic) (io.SubscriberMessages, error) {
	subscribers, err := job.db.getSubscribers(ctx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return io.SubscriberMessages{}, err
	}

	subscriberMessages := make(io.SubscriberMessages, 0)
	for _, subscriber := range subscribers {
		listener, exist := job.listeners[subscriber.Id]
		if !exist {
			continue
		}

		message, err := listener.messages()
		if err != nil {
			return io.SubscriberMessages{}, err
		}
		subscriberMessages = append(subscriberMessages, io.SubscriberMessage{
			TopicId:            subscriber.TopicId,
			Name:               message.Name,
			UnpublishedMessage: message.UnpublishMessage,
			UnackedMessage:     message.UnackMessage,
		})
	}

	return subscriberMessages, nil
}

func (job *Job[V]) enqueue(ctx context.Context, topic core.Topic, subscriberName string) (io.ResponseMessages, error) {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return io.ResponseMessages{}, ErrListenerNotFound
	}

	response := make(chan io.ResponseMessages, 1)
	id := listener.enqueue(response)

	select {
	case <-ctx.Done():
		listener.dequeue(id)
		return io.ResponseMessages{}, nil
	case <-listener.ctx.Done():
		listener.dequeue(id)
		return io.ResponseMessages{}, ErrListenerDeleted
	case resp := <-response:
		return resp, nil
	}
}

func (job *Job[V]) getListeners(subscriberId uuid.UUID) (*Listener[V], bool) {
	listener, exist := job.listeners[subscriberId]
	if !exist {
		return listener, false
	}

	return listener, true
}

func (job *Job[V]) getSubscribers(ctx context.Context, topic core.Topic, subscriberName string) (core.Subscriber, error) {
	subscribers, err := job.db.getSubscribers(ctx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
		Name:    []string{subscriberName},
	})
	if err != nil {
		return core.Subscriber{}, err
	}

	if len(subscribers) == 0 {
		return core.Subscriber{}, ErrListenerNotFound
	}

	return subscribers[0], nil
}

func (job *Job[V]) close() {
	job.mtx.Lock()
	for _, listener := range job.listeners {
		listener.shutdown()
	}
	job.mtx.Unlock()
}

func (job *Job[V]) remove() {
	slog.Info(
		"topic is deleted. dispatcher waiting job entered shutdown status",
		logPrefixTopic, job.Name,
	)

	job.cancelFunc()

	for _, listener := range job.listeners {
		listener.remove()
	}

	err := job.delete()
	if err != nil {
		slog.Error(
			"error remove topic and his subscribers",
			logPrefixErr, err,
		)
	}
	prometheus.Unregister(job.metric.message)
}

func (job *Job[V]) delete() error {
	// Delete all subscriber messages for this topic
	err := job.db.deleteTopicMessages(context.TODO(), job.Id)
	if err != nil {
		slog.Error(
			"error delete topic messages",
			logPrefixTopic, job.Name,
			logPrefixErr, err,
		)
	}

	return job.db.tx(context.TODO(), func(ctx context.Context, tx *sqlx.Tx) error {
		err := job.db.deleteTxTopic(ctx, tx, core.Topic{
			Id:        job.Id,
			DeletedAt: null.StringFrom(time.Now().Format("2006-01-02 15:04:05")),
		})
		if err != nil {
			return err
		}

		for _, listener := range job.listeners {
			err := job.db.deleteTxSubscribers(ctx, tx, core.Subscriber{
				Name:      listener.Id,
				TopicId:   job.Id,
				DeletedAt: null.StringFrom(time.Now().Format("2006-01-02 15:04:05")),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (job *Job[V]) getDeadLetterMessages(ctx context.Context, topic core.Topic, subscriberName string, limit, offset int) (io.ResponseMessages, error) {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	messages, err := job.db.getDeadLetterMessages(ctx, subscriber.Id, limit, offset)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	response := make(io.ResponseMessages, 0)
	for _, msg := range messages {
		response = append(response, io.ResponseMessage{
			Id:         msg.MessageId,
			Message:    msg.Message,
			Status:     msg.Status,
			RetryCount: msg.RetryCount,
			VisibleAt:  msg.VisibleAt.Format(time.RFC3339),
			CreatedAt:  msg.CreatedAt.Format(time.RFC3339),
		})
	}

	return response, nil
}

func (job *Job[V]) getUnackedMessages(ctx context.Context, topic core.Topic, subscriberName string, limit, offset int) (io.ResponseMessages, error) {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	messages, err := job.db.getUnackedMessages(ctx, subscriber.Id, limit, offset)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	response := make(io.ResponseMessages, 0)
	for _, msg := range messages {
		response = append(response, io.ResponseMessage{
			Id:         msg.MessageId,
			Message:    msg.Message,
			Status:     msg.Status,
			RetryCount: msg.RetryCount,
			VisibleAt:  msg.VisibleAt.Format(time.RFC3339),
			CreatedAt:  msg.CreatedAt.Format(time.RFC3339),
		})
	}

	return response, nil
}

func (job *Job[V]) restoreDeadLetterMessage(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	return job.db.restoreDeadLetterMessage(ctx, subscriber.Id, messageId)
}

func (job *Job[V]) Notify() {
	job.mtx.Lock()
	defer job.mtx.Unlock()

	for _, listener := range job.listeners {
		listener.Notify()
	}
}
