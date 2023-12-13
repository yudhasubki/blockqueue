package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type BlockQueue[V chan io.ResponseMessages] struct {
	mtx       *sync.RWMutex
	serverCtx context.Context
	jobs      map[string]*Job[V]
}

func New[V chan io.ResponseMessages]() *BlockQueue[V] {
	return &BlockQueue[V]{
		mtx:  new(sync.RWMutex),
		jobs: make(map[string]*Job[V]),
	}
}

func (q *BlockQueue[V]) Run(ctx context.Context) error {
	topics, err := getTopics(ctx, core.FilterTopic{})
	if err != nil {
		return err
	}

	for _, topic := range topics {
		job, err := newJob[V](ctx, topic)
		if err != nil {
			return err
		}
		q.jobs[topic.Name] = job
	}
	q.serverCtx = ctx

	return nil
}

func (q *BlockQueue[V]) addJob(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	err := tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := createTxTopic(ctx, tx, topic)
		if err != nil {
			return err
		}

		err = createTxSubscribers(ctx, tx, subscribers)
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

	job, err := newJob[V](q.serverCtx, topic)
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

func (q *BlockQueue[V]) publish(ctx context.Context, topic core.Topic, request io.Publish) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	err := tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return createMessages(ctx, core.Message{
			Id:      uuid.New(),
			TopicId: topic.Id,
			Message: request.Message,
			Status:  core.MessageStatusWaiting,
		})
	})
	if err != nil {
		return err
	}

	job.trigger()

	return nil
}

func (q *BlockQueue[V]) getSubscribersStatus(ctx context.Context, topic core.Topic) (io.SubscriberMessages, error) {
	job, exist := q.getJob(topic)
	if !exist {
		return io.SubscriberMessages{}, ErrJobNotFound
	}

	return job.getListenersStatus(ctx, topic)
}

func (q *BlockQueue[V]) addSubscriber(ctx context.Context, topic core.Topic) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	err := job.addListener(ctx, topic)
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

func (q *BlockQueue[V]) readSubscriberMessage(ctx context.Context, topic core.Topic, subscriber string) (io.ResponseMessages, error) {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return io.ResponseMessages{}, ErrJobNotFound
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
