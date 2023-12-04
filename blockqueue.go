package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/queuestream/pkg/core"
	"github.com/yudhasubki/queuestream/pkg/io"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type BlockQueue[V chan io.ResponseMessages] struct {
	mtx       *sync.Mutex
	serverCtx context.Context
	jobs      map[string]*Job[V]
}

func New[V chan io.ResponseMessages]() *BlockQueue[V] {
	return &BlockQueue[V]{
		mtx:  new(sync.Mutex),
		jobs: make(map[string]*Job[V]),
	}
}

func (q *BlockQueue[V]) Run(ctx context.Context) error {
	topics, err := GetTopics(ctx, core.FilterTopic{})
	if err != nil {
		return err
	}

	for _, topic := range topics {
		job, err := NewJob[V](ctx, topic)
		if err != nil {
			return err
		}
		q.jobs[topic.Name] = job
	}
	q.serverCtx = ctx

	return nil
}

func (q *BlockQueue[V]) AddJob(ctx context.Context, topic core.Topic, subscribers core.Subscribers) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	err := Tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := CreateTxTopic(ctx, tx, topic)
		if err != nil {
			return err
		}

		err = CreateTxSubscribers(ctx, tx, subscribers)
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

	job, err := NewJob[V](q.serverCtx, topic)
	if err != nil {
		return err
	}
	q.jobs[topic.Name] = job

	return nil
}

func (q *BlockQueue[V]) DeleteJob(topic core.Topic) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	job.Remove()

	return nil
}

func (q *BlockQueue[V]) Publish(ctx context.Context, topic core.Topic, request io.Publish) error {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	err := Tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return CreateMessages(ctx, core.Message{
			Id:      uuid.New(),
			TopicId: topic.Id,
			Message: request.Message,
			Status:  core.MessageStatusWaiting,
		})
	})
	if err != nil {
		return err
	}

	job.Trigger()

	return nil
}

func (q *BlockQueue[V]) AddSubscribers(ctx context.Context, topic core.Topic) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	err := job.AddListener(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func (q *BlockQueue[V]) DeleteSubscriber(ctx context.Context, topic core.Topic, subcriber string) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	return job.DeleteListener(ctx, topic, subcriber)
}

func (q *BlockQueue[V]) CreateSubscriberPartition(ctx context.Context, topic core.Topic, request io.RequestCreateSubscriberPartition) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	return job.CreateListenerPartition(ctx, topic, request.Subscriber, request.PartitionId)
}

func (q *BlockQueue[V]) DeleteSubscriberPartition(ctx context.Context, topic core.Topic, request io.RequestCreateSubscriberPartition) error {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	return job.DeleteListenerPartition(ctx, topic, request.Subscriber, request.PartitionId)
}

func (q *BlockQueue[V]) ReadSubscriberPartition(ctx context.Context, topic core.Topic, subscriber, partitionId string) (io.ResponseMessages, error) {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return io.ResponseMessages{}, ErrJobNotFound
	}

	return job.ReadListenerPartition(ctx, topic, subscriber, partitionId)
}

func (q *BlockQueue[V]) DeletePartitionMessage(ctx context.Context, topic core.Topic, request io.RequestDeletePartitionMessage) error {
	job, exist := q.jobs[topic.Name]
	if !exist {
		return ErrJobNotFound
	}

	return job.DeleteListenerPartitionMessage(ctx, topic, request)
}