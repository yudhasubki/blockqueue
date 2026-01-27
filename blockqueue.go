package blockqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/core"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type BlockQueue[V chan bqio.ResponseMessages] struct {
	mtx         *cas.SpinLock
	serverCtx   context.Context
	cancel      context.CancelFunc
	jobs        map[string]*Job[V]
	db          *db
	writeBuffer *WriteBuffer
	Opt         BlockQueueOption
}

func init() {
	prometheus.Register(metric.MessagePublished)
}

type BlockQueueOption struct {
	WriteBufferConfig  WriteBufferConfig
	CheckpointInterval time.Duration // Default: 30s
}

func New[V chan bqio.ResponseMessages](driver Driver, opt BlockQueueOption) *BlockQueue[V] {
	blockqueue := &BlockQueue[V]{
		db:   newDb(driver),
		mtx:  cas.New(),
		jobs: make(map[string]*Job[V]),
		Opt:  opt,
	}

	return blockqueue
}

func (q *BlockQueue[V]) Run(ctx context.Context) error {
	topics, err := q.db.getTopics(ctx, core.FilterTopic{})
	if err != nil {
		return err
	}

	// Create context with cancel for graceful shutdown
	qCtx, cancel := context.WithCancel(ctx)
	q.serverCtx = qCtx
	q.cancel = cancel

	// Initialize write buffer
	wbConfig := q.Opt.WriteBufferConfig
	if wbConfig.BatchSize == 0 && wbConfig.FlushInterval == 0 && wbConfig.BufferSize == 0 {
		wbConfig = DefaultWriteBufferConfig()
	}
	q.writeBuffer = NewWriteBuffer(qCtx, q.db, wbConfig)

	// Start checkpoint goroutine to prevent WAL bloat
	go q.startCheckpointer()

	for _, topic := range topics {
		job, err := newJob[V](qCtx, topic, q.db)
		if err != nil {
			return err
		}
		q.jobs[topic.Name] = job
	}

	return nil
}

// startCheckpointer runs periodic WAL checkpoints (SQLite only)
func (q *BlockQueue[V]) startCheckpointer() {
	// Only run for SQLite - PostgreSQL handles this automatically
	if q.db.Database.Conn().DriverName() != "sqlite3" {
		return
	}

	interval := q.Opt.CheckpointInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-q.serverCtx.Done():
			// Final checkpoint on shutdown
			_, err := q.db.Database.Conn().Exec("PRAGMA wal_checkpoint(TRUNCATE)")
			if err != nil {
				slog.Error("error final checkpoint", logPrefixErr, err)
			}
			return
		case <-ticker.C:
			// Passive checkpoint - doesn't block readers/writers
			_, err := q.db.Database.Conn().Exec("PRAGMA wal_checkpoint(PASSIVE)")
			if err != nil {
				slog.Error("error checkpoint", logPrefixErr, err)
			}
		}
	}
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

func (q *BlockQueue[V]) BatchPublish(ctx context.Context, topic core.Topic, request []bqio.Publish) error {
	return q.batchPublish(ctx, topic, request)
}

func (q *BlockQueue[V]) BatchAck(ctx context.Context, topic core.Topic, subscriberName string, messageIds []string) error {
	return q.batchAck(ctx, topic, subscriberName, messageIds)
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

	job, err := newJob[V](q.serverCtx, topic, q.db)
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

	// Calculate delay
	var delay time.Duration
	if request.Delay != "" {
		parsed, err := time.ParseDuration(request.Delay)
		if err != nil {
			return err
		}
		delay = parsed
	}

	// Generate unique message ID with timestamp prefix
	messageId := fmt.Sprintf("%d_%s", time.Now().UnixNano(), uuid.NewString()[:8])

	// Use write buffer for batched inserts (better throughput)
	q.writeBuffer.Enqueue(topic.Id, messageId, request.Message, delay)

	go metric.MessagePublished.Inc()

	return nil
}

func (q *BlockQueue[V]) batchAck(ctx context.Context, topic core.Topic, subscriberName string, messageIds []string) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	return job.ackMessages(ctx, topic, subscriberName, messageIds)
}

func (q *BlockQueue[V]) batchPublish(ctx context.Context, topic core.Topic, requests []bqio.Publish) error {
	_, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}

	for _, req := range requests {
		// Calculate delay
		var delay time.Duration
		if req.Delay != "" {
			parsed, err := time.ParseDuration(req.Delay)
			if err != nil {
				return err
			}
			delay = parsed
		}

		// Generate unique message ID with timestamp prefix
		messageId := fmt.Sprintf("%d_%s", time.Now().UnixNano(), uuid.NewString()[:8])
		q.writeBuffer.Enqueue(topic.Id, messageId, req.Message, delay)
		go metric.MessagePublished.Inc()
	}

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

func (q *BlockQueue[V]) Close() {
	if q.cancel != nil {
		q.cancel()
	}
	if q.writeBuffer != nil {
		q.writeBuffer.Close()
	}
}

func (q *BlockQueue[V]) getTopics(ctx context.Context, filter core.FilterTopic) (core.Topics, error) {
	return q.db.getTopics(ctx, filter)
}

func (q *BlockQueue[V]) GetDeadLetterMessages(ctx context.Context, topic core.Topic, subscriberName string, limit, offset int) (bqio.ResponseMessages, error) {
	job, exist := q.getJob(topic)
	if !exist {
		return bqio.ResponseMessages{}, ErrJobNotFound
	}
	return job.getDeadLetterMessages(ctx, topic, subscriberName, limit, offset)
}

func (q *BlockQueue[V]) RestoreDeadLetterMessage(ctx context.Context, topic core.Topic, subscriberName, messageId string) error {
	job, exist := q.getJob(topic)
	if !exist {
		return ErrJobNotFound
	}
	return job.restoreDeadLetterMessage(ctx, topic, subscriberName, messageId)
}
