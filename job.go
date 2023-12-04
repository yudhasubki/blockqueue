package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/eventpool"
	"github.com/yudhasubki/queuestream/pkg/core"
	"github.com/yudhasubki/queuestream/pkg/io"
	"gopkg.in/guregu/null.v4"
)

var (
	ErrListenerNotFound = errors.New("listener not found")
)

type Job[V chan io.ResponseMessages] struct {
	Id        uuid.UUID
	Name      string
	ServerCtx context.Context
	Pool      *eventpool.Eventpool

	mtx       *sync.Mutex
	listeners map[uuid.UUID]*Listener[V]
	message   chan bool
	shutdown  chan bool
	deleted   chan bool
}

func NewJob[V chan io.ResponseMessages](serverCtx context.Context, topic core.Topic) (*Job[V], error) {
	subscribers, err := GetSubscribers(serverCtx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return &Job[V]{}, err
	}

	listeners := make(map[uuid.UUID]*Listener[V])
	for _, subscriber := range subscribers {
		listeners[subscriber.Id] = NewListener[V](serverCtx, topic.Name, subscriber)
	}

	eventpoolListeners := make([]eventpool.EventpoolListener, 0, len(listeners))
	for _, listener := range listeners {
		eventpoolListeners = append(eventpoolListeners, eventpool.EventpoolListener{
			Name:       listener.Id,
			Subscriber: listener.storeJob,
			Opts:       []eventpool.SubscriberConfigFunc{},
		})

		err := listener.createBucket()
		if err != nil {
			return &Job[V]{}, err
		}

		err = listener.partitionBucketLog()
		if err != nil {
			return &Job[V]{}, err
		}
	}

	pool := eventpool.New()
	pool.Submit(eventpoolListeners...)

	job := &Job[V]{
		Id:        topic.Id,
		Name:      topic.Name,
		Pool:      pool,
		ServerCtx: serverCtx,
		message:   make(chan bool, 500),
		shutdown:  make(chan bool, 1),
		deleted:   make(chan bool, 1),
		mtx:       new(sync.Mutex),
		listeners: listeners,
	}

	go job.fetchWaitingJob()

	return job, nil
}

func (job *Job[V]) Trigger() {
	job.message <- true
}

func (job *Job[V]) AddListener(ctx context.Context, topic core.Topic) error {
	subscribers, err := GetSubscribers(ctx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return err
	}

	eventpoolListeners := make([]eventpool.EventpoolListener, 0)
	for _, subscriber := range subscribers {
		if _, exist := job.listeners[subscriber.Id]; !exist {
			listener := NewListener[V](job.ServerCtx, topic.Name, subscriber)
			job.listeners[subscriber.Id] = listener

			eventpoolListeners = append(eventpoolListeners, eventpool.EventpoolListener{
				Name:       listener.Id,
				Subscriber: listener.storeJob,
				Opts:       []eventpool.SubscriberConfigFunc{},
			})
		}
	}

	job.Pool.SubmitOnFlight(eventpoolListeners...)

	return nil
}

func (job *Job[V]) DeleteListener(ctx context.Context, topic core.Topic, subscriberName string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.listeners[subscriber.Id]
	if !exist {
		return ErrListenerNotFound
	}

	listener.delete()
	job.Pool.CloseBy(listener.Id)

	return Tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		subscriber.DeletedAt = null.StringFrom(time.Now().Format("2006-01-02 15:04:05"))
		return DeleteTxSubscribers(ctx, tx, subscriber)
	})
}

func (job *Job[V]) CreateListenerPartition(ctx context.Context, topic core.Topic, subscriberName, partitionId string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.listeners[subscriber.Id]
	if !exist {
		return ErrListenerNotFound
	}

	return listener.CreatePartition(partitionId)
}

func (job *Job[V]) DeleteListenerPartition(ctx context.Context, topic core.Topic, subscriberName, partitionId string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.listeners[subscriber.Id]
	if !exist {
		return ErrListenerNotFound
	}

	return listener.DeletePartition(partitionId)
}

func (job *Job[V]) ReadListenerPartition(ctx context.Context, topic core.Topic, subscriberName, partitionId string) (io.ResponseMessages, error) {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	listener, exist := job.listeners[subscriber.Id]
	if !exist {
		return io.ResponseMessages{}, ErrListenerNotFound
	}

	return listener.ReadPartition(ctx, partitionId)
}

func (job *Job[V]) DeleteListenerPartitionMessage(ctx context.Context, topic core.Topic, request io.RequestDeletePartitionMessage) error {
	subscriber, err := job.getSubscribers(ctx, topic, request.Subscriber)
	if err != nil {
		return err
	}

	listener, exist := job.listeners[subscriber.Id]
	if !exist {
		return ErrListenerNotFound
	}

	return listener.DeletePartitionMessage(ctx, request.PartitionId, request.MessageId)
}

func (job *Job[V]) getSubscribers(ctx context.Context, topic core.Topic, subscriberName string) (core.Subscriber, error) {
	subscribers, err := GetSubscribers(ctx, core.FilterSubscriber{
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

func (job *Job[V]) Close() {
	job.shutdown <- true

	for idx := range job.listeners {
		job.listeners[idx].Close()
	}
}

func (job *Job[V]) Remove() {
	job.deleted <- true
}

func (job *Job[V]) fetchWaitingJob() {
	for {
		select {
		case <-job.ServerCtx.Done():
			slog.Info(
				"signal cancel received. dispatcher waiting job entered shutdown status.",
				LogPrefixTopic, job.Name,
			)

			job.Close()

			return
		case <-job.deleted:
			slog.Info(
				"topic is delete. dispatcher waiting job entered shutdown status",
				LogPrefixTopic, job.Name,
			)

			err := job.remove()
			if err != nil {
				slog.Error(
					"error removing a topic",
					LogPrefixTopic, job.Name,
					LogPrefixErr, err,
				)
			}

			return
		case <-job.shutdown:
			slog.Info(
				"job entered shutdown status",
				LogPrefixTopic, job.Name,
			)

		case <-job.message:
			slog.Debug(
				"push job to the consumer bucket",
				LogPrefixTopic, job.Name,
			)

			err := job.dispatchJob()
			if err != nil {
				slog.Error(
					"error dispatching job to the listener",
					LogPrefixTopic, job.Name,
					LogPrefixErr, err,
				)
			}
		}
	}
}

func (job *Job[V]) dispatchJob() error {
	ctx := context.Background()
	messages, err := GetMessages(ctx, core.FilterMessage{
		TopicId: []uuid.UUID{job.Id},
		Status:  []core.MessageStatus{core.MessageStatusWaiting},
		Offset:  1,
		Limit:   10,
	})
	if err != nil {
		slog.Error(
			"error fetching message",
			LogPrefixTopic, job.Name,
			LogPrefixMessageStatus, core.MessageStatusWaiting,
			LogPrefixErr, err,
		)
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	if len(messages) > 0 {
		err = UpdateStatusMessage(ctx, core.MessageStatusDelivered, messages.Ids()...)
		if err != nil {
			slog.Error(
				"error update status message",
				LogPrefixTopic, job.Name,
				LogPrefixMessageStatus, core.MessageStatusDelivered,
			)
			return nil
		}

		job.Pool.Publish(eventpool.SendJson(messages))
	}

	return nil
}

func (job *Job[V]) remove() error {
	job.Pool.Close()

	for _, listener := range job.listeners {
		listener.delete()
	}

	return Tx(context.TODO(), func(ctx context.Context, tx *sqlx.Tx) error {
		err := DeleteTxTopic(ctx, tx, core.Topic{
			Id:        job.Id,
			DeletedAt: null.StringFrom(time.Now().Format("2006-01-02 15:04:05")),
		})
		if err != nil {
			return err
		}

		for _, listener := range job.listeners {
			err := DeleteTxSubscribers(ctx, tx, core.Subscriber{
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
