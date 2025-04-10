package blockqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/nutsdb/nutsdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"github.com/yudhasubki/eventpool"
	"gopkg.in/guregu/null.v4"
)

const (
	bufferSizeJob = 20000
)

type Job[V chan io.ResponseMessages] struct {
	Id   uuid.UUID
	Name string

	db         *db
	kv         *kv
	ctx        context.Context
	cancelFunc context.CancelFunc
	pool       *eventpool.EventpoolPartition
	mtx        *cas.SpinLock
	listeners  map[uuid.UUID]*Listener[V]
	message    chan bool
	metric     *jobMetric
	opt        BlockQueueOption
}

type jobMetric struct {
	message *prometheus.CounterVec
}

func newJob[V chan io.ResponseMessages](serverCtx context.Context, topic core.Topic, db *db, kv *kv, opt BlockQueueOption) (*Job[V], error) {
	ctx, cancel := context.WithCancel(serverCtx)
	job := &Job[V]{
		Id:         topic.Id,
		Name:       topic.Name,
		db:         db,
		kv:         kv,
		cancelFunc: cancel,
		ctx:        ctx,
		message:    make(chan bool, 50000),
		mtx:        cas.New(),
		pool:       eventpool.NewPartition(opt.ProducerPartitionNumber),
		metric: &jobMetric{
			message: metric.MessagePublishedTopic(topic.Name),
		},
		opt: opt,
	}
	prometheus.Register(job.metric.message)

	err := job.createBucket()
	if err != nil {
		return nil, err
	}

	subscribers, err := db.getSubscribers(serverCtx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return &Job[V]{}, err
	}

	listeners := make(map[uuid.UUID]*Listener[V])
	for _, subscriber := range subscribers {
		listener, err := newListener[V](serverCtx, topic.Name, subscriber, job.kv)
		if err != nil {
			return &Job[V]{}, err
		}

		listeners[subscriber.Id] = listener
	}
	job.listeners = listeners

	eventpoolListeners := make([]eventpool.EventpoolListener, 0, len(listeners))
	for _, listener := range listeners {
		eventpoolListeners = append(eventpoolListeners, eventpool.EventpoolListener{
			Name:       listener.Id,
			Subscriber: listener.jobCatcher,
			Opts: []eventpool.SubscriberConfigFunc{
				eventpool.BufferSize(bufferSizeJob),
			},
		})
	}

	job.pool.Submit(opt.ConsumerPartitionNumber, eventpoolListeners...)
	job.pool.Run()

	go job.fetchWaitingJob()

	return job, nil
}

func (job *Job[V]) createBucket() error {
	return job.kv.readBucketTx(func(tx *nutsdb.Tx) error {
		return job.kv.createTxBucket(tx, nutsdb.DataStructureList, job.Name)
	})
}

func (job *Job[V]) trigger() {
	job.message <- true
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

func (job *Job[V]) addListener(ctx context.Context, topic core.Topic) error {
	subscribers, err := job.db.getSubscribers(ctx, core.FilterSubscriber{
		TopicId: []uuid.UUID{topic.Id},
	})
	if err != nil {
		return err
	}

	eventpoolListeners := make([]eventpool.EventpoolListener, 0)
	for _, subscriber := range subscribers {
		if _, exist := job.listeners[subscriber.Id]; !exist {
			listener, err := newListener[V](job.ctx, topic.Name, subscriber, job.kv)
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

	job.pool.SubmitOnFlight(10, eventpoolListeners...)

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
		message, err := job.listeners[subscriber.Id].messages()
		if err != nil {
			return io.SubscriberMessages{}, err
		}
		subscriberMessages = append(subscriberMessages, io.SubscriberMessage{
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
	job.pool.Close()

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

func (job *Job[V]) fetchWaitingJob() {
	for {
		ticker := time.NewTicker(1 * time.Second)
		select {
		case <-job.ctx.Done():
			slog.Info(
				"signal cancel received. dispatcher waiting job entered shutdown status.",
				logPrefixTopic, job.Name,
			)
			job.pool.Close()
			job.close()

			return
		case <-ticker.C:
			err := job.dispatchJob()
			if err != nil {
				slog.Error(
					"error dispatching job to the listener",
					logPrefixTopic, job.Name,
					logPrefixErr, err,
				)
			}
		}
	}
}

func (job *Job[V]) dispatchJob() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Determine optimal batch size based on active listeners
	// For low concurrency, we can use larger batches
	batchSize := 100

	// Simple heuristic - if we have few active listeners, we likely have lower concurrency
	if len(job.listeners) < 3 {
		batchSize = 200 // Double batch size for low concurrency scenarios
	}

	messages, err := job.db.getMessages(ctx, core.FilterMessage{
		TopicId: []uuid.UUID{job.Id},
		Status:  []core.MessageStatus{core.MessageStatusWaiting},
		Offset:  1,
		Limit:   batchSize, // Use adaptive batch size
	})
	if err != nil {
		slog.Error(
			"error fetching message",
			logPrefixTopic, job.Name,
			logPrefixMessageStatus, core.MessageStatusWaiting,
			logPrefixErr, err,
		)
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	if len(messages) > 0 {
		updateCtx, updateCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer updateCancel()

		err = job.db.updateStatusMessage(updateCtx, core.MessageStatusDelivered, messages.Ids()...)
		if err != nil {
			slog.Error(
				"error update status message",
				logPrefixTopic, job.Name,
				logPrefixMessageStatus, core.MessageStatusDelivered,
				logPrefixErr, err,
			)
			return nil
		}

		job.pool.Publish("*", job.Id.String(), eventpool.SendJson(messages))

		go func() {
			job.metric.message.WithLabelValues(job.Name).Add(float64(len(messages)))
		}()
	}

	return nil
}

func (job *Job[V]) delete() error {
	err := job.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
		return tx.DeleteBucket(nutsdb.DataStructureList, job.Name)
	})
	if err != nil {
		slog.Error(
			"error remove bucket",
			logPrefixBucket, job.Name,
		)
		return err
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
