package blockqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/queuestream/pkg/bucket"
	"github.com/yudhasubki/queuestream/pkg/core"
	blockio "github.com/yudhasubki/queuestream/pkg/io"
	"github.com/yudhasubki/queuestream/pkg/pqueue"
)

var (
	ErrListenerShutdown  = errors.New("listener going to shutdown")
	ErrPartitionConflict = errors.New("partition was exist")
	ErrPartitionNotFound = errors.New("partition not found")
)

type Listener[V chan blockio.ResponseMessages] struct {
	Id            string
	JobId         string
	Partitions    map[string]*Partition
	ServerCtx     context.Context
	PriorityQueue *pqueue.PriorityQueue[V]
	mtx           *sync.RWMutex
	shutdown      bool
	deleted       chan bool
	message       chan bool
}

func NewListener[V chan blockio.ResponseMessages](serverCtx context.Context, jobId string, subscriber core.Subscriber) *Listener[V] {
	listener := &Listener[V]{
		Id:            subscriber.Name,
		JobId:         jobId,
		ServerCtx:     serverCtx,
		PriorityQueue: pqueue.New[V](),
		Partitions:    make(map[string]*Partition),
		shutdown:      false,
		deleted:       make(chan bool, 1),
		mtx:           new(sync.RWMutex),
		message:       make(chan bool, 20000),
	}

	go listener.claim()

	return listener
}

func (listener Listener[V]) createBucket() error {
	return BucketTx(func(tx *nutsdb.Tx) error {
		err := CreateTxBucket(tx, listener.Bucket())
		if err != nil {
			return err
		}

		return CreateTxBucket(tx, listener.BucketPartitionLog())
	})
}

func (listener Listener[V]) Bucket() string {
	return fmt.Sprintf("%s:%s", listener.JobId, listener.Id)
}

func (listener Listener[V]) BucketPartitionLog() string {
	return fmt.Sprintf("%s:%s:partition_log", listener.JobId, listener.Id)
}

func (listener *Listener[V]) Close() {
	listener.shutdown = true
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) CreatePartition(partitionId string) error {
	listener.mtx.Lock()
	defer listener.mtx.Unlock()

	_, exist := listener.Partitions[partitionId]
	if exist {
		return ErrPartitionConflict
	}

	listener.Partitions[partitionId] = NewPartition(
		partitionId,
		listener.JobId,
		listener.Id,
	)

	err := BucketTx(func(tx *nutsdb.Tx) error {
		return tx.Put(listener.BucketPartitionLog(), []byte(partitionId), []byte(partitionId), nutsdb.Persistent)
	})
	if err != nil {
		return err
	}

	return listener.Partitions[partitionId].createBucket()
}

func (listener *Listener[V]) DeletePartitionMessage(ctx context.Context, partitionId, messageId string) error {
	listener.mtx.Lock()
	defer listener.mtx.Unlock()

	partition, ok := listener.Partitions[partitionId]
	if !ok {
		return ErrPartitionNotFound
	}

	return partition.deleteMessage(messageId)
}

func (listener *Listener[V]) ReadPartition(ctx context.Context, partitionId string) (blockio.ResponseMessages, error) {
	partition, ok := listener.Partitions[partitionId]
	if !ok {
		return blockio.ResponseMessages{}, ErrPartitionNotFound
	}

	bucketMessages, err := partition.read(ctx)
	if err != nil {
		slog.Error(
			"error read partition messages",
			LogPrefixErr, err,
		)
		return blockio.ResponseMessages{}, err
	}

	messages := make(blockio.ResponseMessages, 0)
	if len(bucketMessages) > 0 {
		for _, message := range bucketMessages {
			messages = append(messages, blockio.ResponseMessage{
				Id:      message.Id,
				Message: message.Message,
			})
		}

		return messages, nil
	}

	messagesChan := make(chan blockio.ResponseMessages, 1)
	listener.Enqueue(partitionId, messagesChan)

	select {
	case <-ctx.Done():
		listener.Dequeue(partitionId)
	case msgsChan := <-messagesChan:
		messages = msgsChan
	}

	return messages, nil
}

func (listener *Listener[V]) Enqueue(partitionId string, messages chan blockio.ResponseMessages) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	listener.PriorityQueue.Push(&pqueue.Item[V]{
		Id:    partitionId,
		Value: messages,
	})

	listener.PriorityQueue.Cond.Signal()
}

func (listener *Listener[V]) Dequeue(partitionId string) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for i := 0; i < listener.PriorityQueue.Len(); i++ {
		if listener.PriorityQueue.At(i).Id == partitionId {
			listener.PriorityQueue.At(i).Value <- blockio.ResponseMessages{}
			listener.PriorityQueue.Remove(i)
			break
		}
	}

	listener.PriorityQueue.Cond.Signal()
}
func (listener *Listener[V]) DeletePartition(partitionId string) error {
	listener.mtx.Lock()
	defer listener.mtx.Unlock()

	partition, exist := listener.Partitions[partitionId]
	if !exist {
		return ErrPartitionNotFound
	}

	delete(listener.Partitions, partitionId)
	DeleteMessageBucket(listener.BucketPartitionLog(), partitionId)
	partition.delete()

	return nil
}

func (listener *Listener[V]) claim() {
	for {
		select {
		case <-listener.ServerCtx.Done():
			slog.Info(
				"[claim] watcher entering to shutdown state",
				"listener", listener.Bucket(),
			)
			return
		case <-listener.deleted:
			slog.Info(
				"[claim] watcher entering to delete state",
				"listener", listener.Bucket(),
			)

			for _, partition := range listener.Partitions {
				partition.delete()
			}

			DeleteBuckets(
				listener.Bucket(),
				listener.BucketPartitionLog(),
			)

			return
		case <-listener.message:
			err := listener.read()
			if err != nil {
				if errors.Is(err, ErrListenerShutdown) {
					break
				}
				slog.Debug(
					"error read",
					LogPrefixErr, err,
				)
			}
		}
	}
}

func (listener *Listener[V]) count() int {
	total, err := CountBucketMessage(listener.Bucket())
	if err != nil {
		slog.Error(
			"error count bucket message",
			"error", err,
			"bucket", listener.Bucket(),
		)
		return 0
	}

	return total
}

func (listener *Listener[V]) read() error {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for listener.PriorityQueue.Len() == 0 && !listener.shutdown && listener.count() >= 0 {
		slog.Debug(
			"waiting priority queue from request",
			"total_request", listener.PriorityQueue.Len(),
			"listener", listener.Id,
		)
		listener.PriorityQueue.Cond.Wait()
	}

	if listener.shutdown {
		return ErrListenerShutdown
	}

	bucketMessages := make(bucket.Messages, 0)
	err := GetBucketMessage(listener.Bucket(), func(position int, val []byte) error {
		message := bucket.Message{}

		err := json.Unmarshal(val, &message)
		if err != nil {
			return err
		}

		bucketMessages = append(bucketMessages, message)

		return nil
	})
	if err != nil {
		return err
	}

	if len(bucketMessages) == 0 {
		return nil
	}

	messages := make(blockio.ResponseMessages, 0)
	for _, message := range bucketMessages {
		messages = append(messages, blockio.ResponseMessage{
			Id:      message.Id,
			Message: message.Message,
		})
	}

	err = BucketTx(func(tx *nutsdb.Tx) error {
		_, exist := listener.Partitions[listener.PriorityQueue.Peek().Id]
		if !exist {
			return ErrPartitionNotFound
		}
		return listener.Partitions[listener.PriorityQueue.Peek().Id].storeTxBucketMessage(tx, bucketMessages)
	})
	if err != nil {
		slog.Debug(
			"error store tx bucket message",
			LogPrefixErr, err,
		)
		return err
	}

	err = BucketTx(func(tx *nutsdb.Tx) error {
		return listener.deleteTxBucketMessage(tx, bucketMessages)
	})
	if err != nil {
		slog.Debug(
			"error store tx bucket message",
			LogPrefixErr, err,
		)
		return err
	}

	listener.PriorityQueue.Peek().Value <- messages
	listener.PriorityQueue.Pop()

	return nil
}

func (listener *Listener[V]) deleteTxBucketMessage(tx *nutsdb.Tx, messages bucket.Messages) error {
	for _, message := range messages {
		err := tx.Delete(listener.Bucket(), []byte(message.Id))
		if err != nil {
			return err
		}
	}

	return nil
}

func (listener *Listener[V]) partitionBucketLog() error {
	ids := make([]string, 0)
	err := GetBucketMessage(listener.BucketPartitionLog(), func(position int, val []byte) error {
		ids = append(ids, string(val))
		return nil
	})
	if err != nil {
		return err
	}

	slog.Debug(
		"current partition buckets",
		LogPrefixBucket, ids,
		LogPrefixConsumer, listener.Id,
	)

	for _, id := range ids {
		err := listener.CreatePartition(id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (listener *Listener[V]) delete() {
	listener.shutdown = true
	listener.deleted <- true
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) storeJob(name string, message io.Reader) error {
	var messages core.Messages

	err := json.NewDecoder(message).Decode(&messages)
	if err != nil {
		slog.Error(
			"error decode message",
			LogPrefixErr, err,
			LogPrefixConsumer, name,
		)
		return err
	}

	var (
		prefix = time.Now().UnixNano()
	)

	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	err = backoff.Retry(func() error {
		return BucketTx(func(tx *nutsdb.Tx) error {
			for idx, message := range messages {
				var (
					id = fmt.Sprintf("%d_%d", prefix, idx)
				)

				b, err := json.Marshal(bucket.Message{
					Id:      id,
					Message: message.Message,
				})
				if err != nil {
					return err
				}

				err = tx.Put(listener.Bucket(), []byte(id), b, nutsdb.Persistent)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}, backoff.NewExponentialBackOff())
	if err != nil {
		slog.Error(
			"error insert bucket",
			LogPrefixErr, err,
			LogPrefixConsumer, name,
		)
		return err
	}

	listener.message <- true

	return nil
}
