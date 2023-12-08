package blockqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/queuestream/pkg/bucket"
	"github.com/yudhasubki/queuestream/pkg/core"
	blockio "github.com/yudhasubki/queuestream/pkg/io"
	"github.com/yudhasubki/queuestream/pkg/pqueue"
)

var (
	ErrListenerShutdown = errors.New("listener shutdown")
	ErrListenerNotFound = errors.New("listener not found")
	ErrListenerDeleted  = errors.New("listener was deleted")
)

type ListenerStatus string

const (
	ShutdownListenerStatus       ListenerStatus = "shutdown"
	RemovedListenerStatus        ListenerStatus = "remove"
	FailedDeliveryListenerStatus ListenerStatus = "failed_delivery"
	DeliveryListenerStatus       ListenerStatus = "delivery"
)

type Listener[V chan blockio.ResponseMessages] struct {
	Id            string
	JobId         string
	PriorityQueue *pqueue.PriorityQueue[V]
	ServerCtx     context.Context

	ctx        context.Context
	cancelFunc context.CancelFunc
	shutdown   bool
	remove     bool
	response   chan chan ListenerStatus
}

func NewListener[V chan blockio.ResponseMessages](serverCtx context.Context, jobId string, subscriber core.Subscriber) *Listener[V] {
	ctx, cancel := context.WithCancel(serverCtx)
	listener := &Listener[V]{
		ctx:           ctx,
		cancelFunc:    cancel,
		Id:            subscriber.Name,
		JobId:         jobId,
		PriorityQueue: pqueue.New[V](),
		shutdown:      false,
		remove:        false,
		response:      make(chan chan ListenerStatus),
	}

	go listener.watcher()
	go listener.dispatcher()

	return listener
}

func (listener *Listener[V]) Shutdown() {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	listener.shutdown = true
	listener.cancelFunc()
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) Remove() {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	listener.remove = true
	listener.cancelFunc()
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener Listener[V]) Bucket() []byte {
	return []byte(fmt.Sprintf("%s:%s", listener.JobId, listener.Id))
}

func (listener *Listener[V]) Enqueue(messages chan blockio.ResponseMessages) string {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	id := uuid.New().String()
	listener.PriorityQueue.Push(&pqueue.Item[V]{
		Id:    id,
		Value: messages,
	})

	listener.PriorityQueue.Cond.Broadcast()

	return id
}

func (listener *Listener[V]) Dequeue(id string) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for i := 0; i < listener.PriorityQueue.Len(); i++ {
		if listener.PriorityQueue.At(i).Id == id {
			listener.PriorityQueue.At(i).Value <- blockio.ResponseMessages{}
			listener.PriorityQueue.Remove(i)
			break
		}
	}

	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) dispatcher() {
	for response := range listener.response {
		listener.PriorityQueue.Cond.L.Lock()

		for listener.PriorityQueue.Len() == 0 && !listener.shutdown && !listener.remove {
			slog.Info(
				"wait priority queue",
				"size", listener.PriorityQueue.Len(),
			)
			listener.PriorityQueue.Cond.Wait()
		}

		var (
			err      error
			messages = make(blockio.ResponseMessages, 0)
		)

		if listener.shutdown {
			response <- ShutdownListenerStatus
			goto Unlock
		}

		if listener.remove {
			response <- RemovedListenerStatus
			goto Unlock
		}

		err = ReadBucketTx(func(tx *nutsdb.Tx) error {
			items, err := tx.LRange(listener.JobId, listener.Bucket(), 0, 10)
			if err != nil {
				return err
			}

			for _, item := range items {
				message := bucket.Message{}
				err := json.Unmarshal(item, &message)
				if err != nil {
					return err
				}

				messages = append(messages, blockio.ResponseMessage{
					Id:      message.Id,
					Message: message.Message,
				})
			}

			return nil
		})
		if err != nil {
			slog.Error(
				"[watcher] error reading the unclaimed message on the bucket",
				LogPrefixErr, err,
				LogPrefixBucket, listener.JobId,
				LogPrefixConsumer, string(listener.Bucket()),
			)

			response <- FailedDeliveryListenerStatus
			goto Unlock
		}

		err = UpdateBucketTx(func(tx *nutsdb.Tx) error {
			for i := 0; i < len(messages); i++ {
				_, err := tx.LPop(listener.JobId, listener.Bucket())
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			slog.Error(
				"[watcher] error remove the claimed message on the bucket",
				LogPrefixErr, err,
				LogPrefixBucket, listener.JobId,
				LogPrefixConsumer, string(listener.Bucket()),
			)
			response <- FailedDeliveryListenerStatus
			goto Unlock
		}

		listener.PriorityQueue.Peek().Value <- messages
		listener.PriorityQueue.Pop()

		response <- DeliveryListenerStatus

	Unlock:
		listener.PriorityQueue.Cond.L.Unlock()
	}
}

func (listener *Listener[V]) watcher() {
	for {
		ticker := time.NewTicker(500 * time.Microsecond)
		select {
		case <-listener.ctx.Done():
			slog.Debug(
				"listener entering shutdown status",
				"removed", listener.remove,
				"shutdown", listener.shutdown,
			)
			return
		case <-ticker.C:
			messageLength := 0
			err := UpdateBucketTx(func(tx *nutsdb.Tx) error {
				size, err := tx.LSize(listener.JobId, listener.Bucket())
				if err != nil {
					return err
				}

				messageLength = size

				return nil
			})
			if err != nil {
				continue
			}

			if messageLength > 0 {
				kind := make(chan ListenerStatus)
				listener.response <- kind

				k := <-kind
				switch k {
				case RemovedListenerStatus:
					slog.Debug(
						"[watcher] listener entering shutdown status, listener removed",
						LogPrefixConsumer, listener.Id,
					)
					return
				case ShutdownListenerStatus:
					slog.Debug(
						"[watcher] listener entering shutdown status, server receive signal shutdown",
						LogPrefixConsumer, listener.Id,
					)
					return
				case FailedDeliveryListenerStatus:
					slog.Error(
						"[watcher] error sent to the incoming request",
					)
				case DeliveryListenerStatus:
					slog.Debug(
						"[watcher] success deliver message to the client",
						LogPrefixConsumer, listener.Id,
					)
				}
			}
		}
	}
}

func (listener *Listener[V]) storeJob(name string, message io.Reader) error {
	var (
		messages core.Messages
		prefix   = time.Now().UnixNano()
	)

	err := json.NewDecoder(message).Decode(&messages)
	if err != nil {
		slog.Error(
			"error decode message",
			LogPrefixErr, err,
			LogPrefixConsumer, name,
		)
		return err
	}

	err = backoff.Retry(func() error {
		return UpdateBucketTx(func(tx *nutsdb.Tx) error {
			messageBytes := make([][]byte, 0)
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

				messageBytes = append(messageBytes, b)
			}

			err = tx.RPush(listener.JobId, []byte(listener.Bucket()), messageBytes...)
			if err != nil {
				return err
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

	return nil
}
