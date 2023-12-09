package blockqueue

import (
	"bytes"
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
	"github.com/yudhasubki/eventpool"
	"github.com/yudhasubki/queuestream/pkg/bucket"
	"github.com/yudhasubki/queuestream/pkg/core"
	blockio "github.com/yudhasubki/queuestream/pkg/io"
	"github.com/yudhasubki/queuestream/pkg/pqueue"
)

var (
	ErrListenerShutdown             = errors.New("listener shutdown")
	ErrListenerNotFound             = errors.New("listener not found")
	ErrListenerDeleted              = errors.New("listener was deleted")
	ErrListenerRetryMessageNotFound = errors.New("error ack message. message_id not found")
)

type KindEventListener string

const (
	ShutdownKindEventListener       KindEventListener = "shutdown"
	RemovedKindEventListener        KindEventListener = "remove"
	FailedDeliveryKindEventListener KindEventListener = "failed_delivery"
	DeliveryKindEventListener       KindEventListener = "delivery"
)

const (
	BufferSizeRetryListener = 100
)

type EventListener struct {
	Kind  KindEventListener
	Error error
}

type Listener[V chan blockio.ResponseMessages] struct {
	Id            string
	JobId         string
	PriorityQueue *pqueue.PriorityQueue[V]

	pool       *eventpool.Eventpool
	ctx        context.Context
	cancelFunc context.CancelFunc
	shutdown   bool
	remove     bool
	response   chan chan EventListener
	option     listenerOption
}

type listenerOption struct {
	MaxAttempts        int
	VisibilityDuration time.Duration
}

func NewListener[V chan blockio.ResponseMessages](serverCtx context.Context, jobId string, subscriber core.Subscriber) (*Listener[V], error) {
	option := core.SubscriberOpt{}
	err := json.Unmarshal(subscriber.Option, &option)
	if err != nil {
		return &Listener[V]{}, err
	}

	parse, err := time.ParseDuration(option.VisibilityDuration)
	if err != nil {
		return &Listener[V]{}, err
	}

	ctx, cancel := context.WithCancel(serverCtx)

	listener := &Listener[V]{
		ctx:           ctx,
		cancelFunc:    cancel,
		Id:            subscriber.Name,
		JobId:         jobId,
		PriorityQueue: pqueue.New[V](),
		option: listenerOption{
			MaxAttempts:        option.MaxAttempts,
			VisibilityDuration: parse,
		},
		pool:     eventpool.New(),
		shutdown: false,
		remove:   false,
		response: make(chan chan EventListener),
	}

	listener.pool.Submit(eventpool.EventpoolListener{
		Name:       string(listener.RetryBucket()),
		Subscriber: listener.retryCatcher,
		Opts: []eventpool.SubscriberConfigFunc{
			eventpool.BufferSize(BufferSizeRetryListener),
		},
	})
	listener.pool.Run()

	go listener.watcher()
	go listener.dispatcher()
	go listener.retryWatcher()

	return listener, nil
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

func (listener Listener[V]) RetryBucket() []byte {
	return []byte(fmt.Sprintf("%s:%s:temp", listener.JobId, listener.Id))
}

func (listener *Listener[V]) DeleteRetryMessage(id string) error {
	position := 0

	err := ReadBucketTx(func(tx *nutsdb.Tx) error {
		items, err := tx.LRange(listener.JobId, listener.RetryBucket(), 0, -1)
		if err != nil {
			return err
		}

		for idx, item := range items {
			bucket := bucket.MessageVisibility{}
			err := json.Unmarshal(item, &bucket)
			if err != nil {
				return err
			}

			if bucket.Message.Id == id {
				slog.Debug(
					"[DeleteRetryMessage] found message id",
					"message_id", id,
					"position", idx,
				)
				position = idx + 1
				break
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if position > 0 {
		return UpdateBucketTx(func(tx *nutsdb.Tx) error {
			return tx.LRemByIndex(listener.JobId, listener.RetryBucket(), position-1)
		})
	}

	return ErrListenerRetryMessageNotFound
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
		listener.notify(response)
	}
}

func (listener *Listener[V]) notify(response chan EventListener) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for listener.PriorityQueue.Len() == 0 && !listener.shutdown && !listener.remove {
		slog.Info(
			"wait priority queue",
			"size", listener.PriorityQueue.Len(),
		)
		listener.PriorityQueue.Cond.Wait()
	}

	if listener.shutdown {
		response <- EventListener{
			Kind: ShutdownKindEventListener,
		}
		return
	}

	if listener.remove {
		response <- EventListener{
			Kind: RemovedKindEventListener,
		}
		return
	}

	messages := make(blockio.ResponseMessages, 0)
	err := ReadBucketTx(func(tx *nutsdb.Tx) error {
		items, err := tx.LRange(listener.JobId, listener.Bucket(), 0, 10)
		if err != nil {
			return err
		}

		for _, item := range items {
			message := bucket.MessageVisibility{}
			err := json.Unmarshal(item, &message)
			if err != nil {
				return err
			}

			messages = append(messages, blockio.ResponseMessage{
				Id:      message.Message.Id,
				Message: message.Message.Message,
			})

			listener.pool.Publish(eventpool.SendJson(message))
		}

		return nil
	})
	if err != nil {
		response <- EventListener{
			Kind:  FailedDeliveryKindEventListener,
			Error: err,
		}
		return
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
		response <- EventListener{
			Kind:  FailedDeliveryKindEventListener,
			Error: err,
		}
		return
	}

	listener.PriorityQueue.Peek().Value <- messages
	listener.PriorityQueue.Pop()

	response <- EventListener{
		Kind: DeliveryKindEventListener,
	}
}

func (listener *Listener[V]) watcher() {
	for {
		ticker := time.NewTicker(500 * time.Millisecond)
		select {
		case <-listener.ctx.Done():
			slog.Debug(
				"[watcher] listener entering shutdown status",
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
				eventListener := make(chan EventListener)
				listener.response <- eventListener

				switch event := <-eventListener; event.Kind {
				case RemovedKindEventListener:
					slog.Debug(
						"[watcher] listener entering shutdown status, listener removed",
						LogPrefixConsumer, listener.Id,
					)
					return
				case ShutdownKindEventListener:
					slog.Debug(
						"[watcher] listener entering shutdown status, server receive signal shutdown",
						LogPrefixConsumer, listener.Id,
					)
					return
				case FailedDeliveryKindEventListener:
					slog.Error(
						"[watcher] error sent to the incoming request",
						LogPrefixErr, event.Error,
					)
				case DeliveryKindEventListener:
					slog.Debug(
						"[watcher] success deliver message to the client",
						LogPrefixConsumer, listener.Id,
					)
				}
			}
		}
	}
}

func (listener *Listener[V]) jobCatcher(name string, message io.Reader) error {
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

				b, err := json.Marshal(bucket.MessageVisibility{
					Message: bucket.Message{
						Id:      id,
						Message: message.Message,
					},
					RetryPolicy: bucket.MessageVisibilityRetryPolicy{
						MaxAttempts: 0,
						NextIter:    time.Now().Add(listener.option.VisibilityDuration),
					},
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

func (listener *Listener[V]) retryWatcher() {
	for {
		message := bucket.MessageVisibility{}
		err := ReadBucketTx(func(tx *nutsdb.Tx) error {
			b, err := tx.LPeek(listener.JobId, listener.RetryBucket())
			if err != nil {
				return err
			}

			err = json.Unmarshal(b, &message)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			continue
		}

		if (message == bucket.MessageVisibility{}) {
			time.Sleep(1 * time.Second)
			continue
		}

		skipRetry := false
		if message.RetryPolicy.MaxAttempts >= listener.option.MaxAttempts {
			skipRetry = true
			goto Update
		}

		if time.Now().Before(message.RetryPolicy.NextIter) {
			time.Sleep(time.Until(message.RetryPolicy.NextIter))
			continue
		}

	Update:
		message.RetryPolicy.MaxAttempts++
		if !skipRetry {
			err = UpdateBucketTx(func(tx *nutsdb.Tx) error {
				message.RetryPolicy.NextIter = message.RetryPolicy.NextIter.Add(listener.option.VisibilityDuration)

				b, _ := json.Marshal(message)
				err := tx.RPush(listener.JobId, listener.Bucket(), b)
				if err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				slog.Error(
					"[retryWatcher] error RPush retry message on bucket",
					LogPrefixConsumer, string(listener.Bucket()),
					LogPrefixConsumer, string(listener.RetryBucket()),
				)
				continue
			}
		}
		err = UpdateBucketTx(func(tx *nutsdb.Tx) error {
			item, err := tx.LPop(listener.JobId, listener.RetryBucket())
			if err != nil {
				return err
			}

			slog.Debug(
				"[retryWatcher] success pop the first element",
				LogPrefixConsumer, string(listener.RetryBucket()),
				LogPrefixMessage, string(item),
			)

			return nil
		})
		if err != nil {
			slog.Error(
				"[retryWatcher] error LPop retry message on bucket",
				LogPrefixConsumer, string(listener.Bucket()),
				LogPrefixConsumer, string(listener.RetryBucket()),
			)
			continue
		}

		slog.Debug(
			"[retryWatcher] success send retry message",
			LogPrefixConsumer, string(listener.Bucket()),
			LogPrefixConsumer, string(listener.RetryBucket()),
			LogPrefixMessage, message,
		)
	}
}

func (listener *Listener[V]) retryCatcher(name string, message io.Reader) error {
	var buf bytes.Buffer

	_, err := io.Copy(&buf, message)
	if err != nil {
		return err
	}

	return backoff.Retry(func() error {
		return UpdateBucketTx(func(tx *nutsdb.Tx) error {
			return tx.RPush(listener.JobId, listener.RetryBucket(), buf.Bytes())
		})
	}, backoff.NewExponentialBackOff())
}
