package blockqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/eventpool"
	"gopkg.in/guregu/null.v4"
)

const (
	BufferSizeJob = 5000
)

type Job[V chan io.ResponseMessages] struct {
	Id        uuid.UUID
	Name      string
	ServerCtx context.Context

	pool      *eventpool.Eventpool
	mtx       *sync.RWMutex
	listeners map[uuid.UUID]*Listener[V]
	message   chan bool
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
		listener, err := NewListener[V](serverCtx, topic.Name, subscriber)
		if err != nil {
			return &Job[V]{}, err
		}

		listeners[subscriber.Id] = listener
	}

	eventpoolListeners := make([]eventpool.EventpoolListener, 0, len(listeners))
	for _, listener := range listeners {
		eventpoolListeners = append(eventpoolListeners, eventpool.EventpoolListener{
			Name:       listener.Id,
			Subscriber: listener.jobCatcher,
			Opts: []eventpool.SubscriberConfigFunc{
				eventpool.BufferSize(BufferSizeJob),
			},
		})
	}

	job := &Job[V]{
		Id:        topic.Id,
		Name:      topic.Name,
		pool:      eventpool.New(),
		ServerCtx: serverCtx,
		message:   make(chan bool, 20000),
		deleted:   make(chan bool, 1),
		mtx:       new(sync.RWMutex),
		listeners: listeners,
	}
	job.pool.Submit(eventpoolListeners...)
	job.pool.Run()

	err = job.createBucket()
	if err != nil {
		return nil, err
	}

	go job.fetchWaitingJob()

	return job, nil
}

func (job *Job[V]) createBucket() error {
	return UpdateBucketTx(func(tx *nutsdb.Tx) error {
		return CreateTxBucket(tx, nutsdb.DataStructureList, job.Name)
	})
}

func (job *Job[V]) Trigger() {
	job.message <- true
}

func (job *Job[V]) AckMessage(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return ErrListenerNotFound
	}

	return listener.DeleteRetryMessage(messageId)
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
			listener, err := NewListener[V](job.ServerCtx, topic.Name, subscriber)
			if err != nil {
				return err
			}
			job.listeners[subscriber.Id] = listener

			eventpoolListeners = append(eventpoolListeners, eventpool.EventpoolListener{
				Name:       listener.Id,
				Subscriber: listener.jobCatcher,
				Opts:       []eventpool.SubscriberConfigFunc{},
			})
		}
	}

	job.pool.SubmitOnFlight(eventpoolListeners...)

	return nil
}

func (job *Job[V]) DeleteListener(ctx context.Context, topic core.Topic, subscriberName string) error {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return nil
	}

	job.mtx.Lock()
	defer job.mtx.Unlock()

	err = Tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return DeleteTxSubscribers(ctx, tx, core.Subscriber{
			Name:      listener.Id,
			TopicId:   job.Id,
			DeletedAt: null.StringFrom(time.Now().Format("2006-01-02 15:04:05")),
		})
	})
	if err != nil {
		return err
	}

	listener.Remove()
	delete(job.listeners, subscriber.Id)

	return nil
}

func (job *Job[V]) Enqueue(ctx context.Context, topic core.Topic, subscriberName string) (io.ResponseMessages, error) {
	subscriber, err := job.getSubscribers(ctx, topic, subscriberName)
	if err != nil {
		return io.ResponseMessages{}, err
	}

	listener, exist := job.getListeners(subscriber.Id)
	if !exist {
		return io.ResponseMessages{}, ErrListenerNotFound
	}

	response := make(chan io.ResponseMessages, 1)
	id := listener.Enqueue(response)

	select {
	case <-ctx.Done():
		listener.Dequeue(id)
		return io.ResponseMessages{}, nil
	case <-listener.ctx.Done():
		listener.Dequeue(id)
		return io.ResponseMessages{}, ErrListenerDeleted
	case resp := <-response:
		return resp, nil
	}
}

func (job *Job[V]) getListeners(subscriberId uuid.UUID) (*Listener[V], bool) {
	job.mtx.RLock()
	defer job.mtx.RUnlock()

	listener, exist := job.listeners[subscriberId]
	if !exist {
		return listener, false
	}

	return listener, true
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
	job.mtx.Lock()
	for _, listener := range job.listeners {
		listener.Shutdown()
	}
	job.mtx.Unlock()
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
			job.pool.Close()
			job.Close()

			return
		case <-job.deleted:
			slog.Info(
				"topic is deleted. dispatcher waiting job entered shutdown status",
				LogPrefixTopic, job.Name,
			)

			job.pool.Close()

			for _, listener := range job.listeners {
				listener.Remove()
			}

			err := job.remove()
			if err != nil {
				slog.Error(
					"error remove topic and his subscribers",
					LogPrefixErr, err,
				)
			}

			return
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

		job.pool.Publish(eventpool.SendJson(messages))
	}

	return nil
}

func (job *Job[V]) remove() error {
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
