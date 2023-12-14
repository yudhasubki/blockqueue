package blockqueue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/core"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"github.com/yudhasubki/eventpool"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type BlockQueue[V chan bqio.ResponseMessages] struct {
	mtx       *cas.SpinLock
	serverCtx context.Context
	jobs      map[string]*Job[V]
	kv        *kv
	db        *db
	pool      *eventpool.Eventpool
}

func init() {
	prometheus.Register(metric.MessagePublished)
}

func New[V chan bqio.ResponseMessages](db *db, bucket *kv) *BlockQueue[V] {
	blockqueue := &BlockQueue[V]{
		db:   db,
		mtx:  cas.New(),
		jobs: make(map[string]*Job[V]),
		kv:   bucket,
	}
	pool := eventpool.New()
	pool.Submit(eventpool.EventpoolListener{
		Name:       "store_job",
		Subscriber: blockqueue.storeJob,
		Opts: []eventpool.SubscriberConfigFunc{
			eventpool.BufferSize(20000),
			eventpool.MaxWorker(50),
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
		job, err := newJob[V](ctx, topic, q.db, q.kv)
		if err != nil {
			return err
		}
		q.jobs[topic.Name] = job
	}
	q.serverCtx = ctx

	return nil
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

	job, err := newJob[V](q.serverCtx, topic, q.db, q.kv)
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

	q.pool.Publish(eventpool.SendJson(core.Message{
		Id:      uuid.New(),
		TopicId: topic.Id,
		Message: request.Message,
		Status:  core.MessageStatusWaiting,
	}))

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

func (q *BlockQueue[V]) storeJob(name string, message io.Reader) error {
	var request core.Message
	err := json.NewDecoder(message).Decode(&request)
	if err != nil {
		return err
	}

	go metric.MessagePublished.Inc()

	return q.db.tx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
		return q.db.createMessages(ctx, request)
	})
}

func (q *BlockQueue[V]) Close() {
	q.pool.Close()
}
