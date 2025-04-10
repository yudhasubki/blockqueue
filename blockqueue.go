package blockqueue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"github.com/yudhasubki/eventpool"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

var publishLoad int32 // Atomic counter to track publish concurrency

type BlockQueue[V chan bqio.ResponseMessages] struct {
	mtx       *cas.SpinLock
	serverCtx context.Context
	jobs      map[string]*Job[V]
	kv        *kv
	db        *db
	pool      *eventpool.EventpoolPartition
	Opt       BlockQueueOption
}

func init() {
	prometheus.Register(metric.MessagePublished)
}

type BlockQueueOption struct {
	ProducerPartitionNumber int
	ConsumerPartitionNumber int
}

func New[V chan bqio.ResponseMessages](db Driver, kv *etcd.Etcd, opt BlockQueueOption) *BlockQueue[V] {
	blockqueue := &BlockQueue[V]{
		db:   newDb(db),
		mtx:  cas.New(),
		jobs: make(map[string]*Job[V]),
		kv:   newKv(kv),
		Opt:  opt,
	}
	pool := eventpool.NewPartition(opt.ProducerPartitionNumber)
	pool.Submit(opt.ConsumerPartitionNumber, eventpool.EventpoolListener{
		Name:       "store_job",
		Subscriber: blockqueue.storeJob,
		Opts: []eventpool.SubscriberConfigFunc{
			eventpool.BufferSize(50000),
		},
	})
	pool.Run()
	blockqueue.pool = pool

	return blockqueue
}

func (q *BlockQueue[V]) Run(ctx context.Context) error {
	topics, err := q.db.getTopics(ctx, core.FilterTopic{})
	if err != nil {
		return err
	}

	for _, topic := range topics {
		job, err := newJob[V](ctx, topic, q.db, q.kv, q.Opt)
		if err != nil {
			return err
		}
		q.jobs[topic.Name] = job
	}
	q.serverCtx = ctx

	return nil
}

func (q *BlockQueue[V]) GetTopics(ctx context.Context, filter core.FilterTopic) (core.Topics, error) {
	return q.getTopics(ctx, filter)
}

func (q *BlockQueue[V]) AddJob(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	return q.addJob(ctx, topic, subscribers)
}

func (q *BlockQueue[V]) DeleteJob(topic core.Topic) error {
	return q.deleteJob(topic)
}

func (q *BlockQueue[V]) Ack(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	return q.ackMessage(ctx, topic, subscriberName, messageId)
}

func (q *BlockQueue[V]) Publish(ctx context.Context, topic core.Topic, request bqio.Publish) error {
	return q.publish(ctx, topic, request)
}

func (q *BlockQueue[V]) GetSubscribersStatus(ctx context.Context, topic core.Topic) (bqio.SubscriberMessages, error) {
	return q.getSubscribersStatus(ctx, topic)
}

func (q *BlockQueue[V]) AddSubscriber(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	return q.addSubscriber(ctx, topic, subscribers)
}

func (q *BlockQueue[V]) DeleteSubscriber(ctx context.Context, topic core.Topic, subcriber string) error {
	return q.deleteSubscriber(ctx, topic, subcriber)
}

func (q *BlockQueue[V]) Read(ctx context.Context, topic core.Topic, subscriber string) (bqio.ResponseMessages, error) {
	return q.readSubscriberMessage(ctx, topic, subscriber)
}

func (q *BlockQueue[V]) addJob(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	err := q.db.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := q.db.createTxTopic(ctx, tx, topic)
		if err != nil {
			return err
		}

		err = q.db.createTxSubscribers(ctx, tx, subscribers)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error(
			"[CreateTopic] error create tx topic",
			"error",
			err,
		)
		return err
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, err := newJob[V](q.serverCtx, topic, q.db, q.kv, q.Opt)
	if err != nil {
		return err
	}
	q.jobs[topic.Name] = job

	return nil
}

func (q *BlockQueue[V]) deleteJob(topic core.Topic) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	job.remove()
	delete(q.jobs, topic.Name)

	return nil
}

func (q *BlockQueue[V]) ackMessage(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	return job.ackMessage(ctx, topic, subscriberName, messageId)
}

func (q *BlockQueue[V]) publish(ctx context.Context, topic core.Topic, request bqio.Publish) error {
	_, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	// Pre-generate UUID to avoid overhead
	messageID := uuid.New()

	message := core.Message{
		Id:      messageID,
		TopicId: topic.Id,
		Message: request.Message,
		Status:  core.MessageStatusWaiting,
	}

	// Fast path for small concurrent loads - direct publish
	// This helps with low VU count scenarios by avoiding pool overhead
	load := atomic.LoadInt32(&publishLoad)
	if load < 20 { // Fast path for low concurrency
		atomic.AddInt32(&publishLoad, 1)
		err := q.db.createMessages(ctx, message)
		atomic.AddInt32(&publishLoad, -1)
		if err == nil {
			// Still increment metric
			metric.MessagePublished.Inc()
			return nil
		}
		// Fall back to pool on error
	}

	// Use pool for higher concurrency - this scales better with many VUs
	q.pool.Publish("*", messageID.String(), eventpool.SendJson(message))

	// Increment metric asynchronously without blocking the publish flow
	go metric.MessagePublished.Inc()

	return nil
}

func (q *BlockQueue[V]) getSubscribersStatus(ctx context.Context, topic core.Topic) (bqio.SubscriberMessages, error) {
	job, exist := q.getJob(topic)
	if !exist {
		return bqio.SubscriberMessages{}, ErrJobNotFound
	}

	return job.getListenersStatus(ctx, topic)
}

func (q *BlockQueue[V]) addSubscriber(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	err := q.db.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return q.db.createTxSubscribers(ctx, tx, subscribers)
	})
	if err != nil {
		return err
	}

	err = job.addListener(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func (q *BlockQueue[V]) deleteSubscriber(ctx context.Context, topic core.Topic, subcriber string) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	return job.deleteListener(ctx, topic, subcriber)
}

func (q *BlockQueue[V]) readSubscriberMessage(ctx context.Context, topic core.Topic, subscriber string) (bqio.ResponseMessages, error) {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return bqio.ResponseMessages{}, ErrJobNotFound
	}

	return job.enqueue(ctx, topic, subscriber)
}

func (q *BlockQueue[V]) getJob(topic core.Topic) (*Job[V], bool) {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return &Job[V]{}, false
	}

	return job, true
}

func (q *BlockQueue[V]) storeJob(name string, message []byte) error {
	var request core.Message

	err := json.Unmarshal(message, &request)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return q.db.createMessages(ctx, request)
}

func (q *BlockQueue[V]) Close() {
	q.pool.Close()
}

func (q *BlockQueue[V]) getTopics(ctx context.Context, filter core.FilterTopic) (core.Topics, error) {
	return q.db.getTopics(ctx, filter)
}
