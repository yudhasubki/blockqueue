package blockqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/nutsdb/nutsdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yudhasubki/blockqueue/pkg/bucket"
	"github.com/yudhasubki/blockqueue/pkg/core"
	blockio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/metric"
	"github.com/yudhasubki/blockqueue/pkg/pqueue"
)

var (
	ErrListenerShutdown             = errors.New("listener shutdown")
	ErrListenerNotFound             = errors.New("listener not found")
	ErrListenerDeleted              = errors.New("listener was deleted")
	ErrListenerRetryMessageNotFound = errors.New("error ack message. message_id not found")
)

type kindEventListener string

const (
	shutdownKindEventListener       kindEventListener = "shutdown"
	removedKindEventListener        kindEventListener = "remove"
	failedDeliveryKindEventListener kindEventListener = "failed_delivery"
	deliveryKindEventListener       kindEventListener = "delivery"
)

const (
	bufferSizeRetryListener = 100
)

type eventListener struct {
	Kind  kindEventListener
	Error error
}

type Listener[V chan blockio.ResponseMessages] struct {
	Id            string
	JobId         string
	PriorityQueue *pqueue.PriorityQueue[V]

	kv         *kv
	cancelFunc context.CancelFunc
	ctx        context.Context
	onRemove   *atomic.Bool
	onShutdown *atomic.Bool
	option     listenerOption
	response   chan chan eventListener
	metric     *listenerMetric
}

type listenerOption struct {
	MaxAttempts        int
	VisibilityDuration time.Duration
}

type listenerMetric struct {
	totalConsumedMessage prometheus.Counter
	totalEnqueue         prometheus.Gauge
}

func newListener[V chan blockio.ResponseMessages](serverCtx context.Context, jobId string, subscriber core.Subscriber, bucket *kv) (*Listener[V], error) {
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
		kv:            bucket,
		ctx:           ctx,
		cancelFunc:    cancel,
		Id:            subscriber.Name,
		JobId:         jobId,
		PriorityQueue: pqueue.New[V](),
		option: listenerOption{
			MaxAttempts:        option.MaxAttempts,
			VisibilityDuration: parse,
		},
		onShutdown: &atomic.Bool{},
		onRemove:   &atomic.Bool{},
		response:   make(chan chan eventListener),
		metric: &listenerMetric{
			totalEnqueue:         metric.TotalFlightRequestQueueSubscriber(jobId, subscriber.Name),
			totalConsumedMessage: metric.TotalConsumedMessage(jobId, subscriber.Name),
		},
	}
	prometheus.Register(listener.metric.totalEnqueue)
	prometheus.Register(listener.metric.totalConsumedMessage)

	go listener.watcher()
	go listener.dispatcher()
	go listener.retryWatcher()

	return listener, nil
}

func (listener *Listener[V]) shutdown() {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	listener.onShutdown.Swap(true)
	listener.cancelFunc()
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) remove() {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	prometheus.Unregister(listener.metric.totalEnqueue)
	prometheus.Unregister(listener.metric.totalConsumedMessage)

	listener.onRemove.Swap(true)
	listener.reset()
	listener.cancelFunc()
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) reset() {
	go listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
		size, err := tx.LSize(listener.JobId, listener.messageBucket())
		if err != nil {
			return err
		}

		for i := 0; i < size; i++ {
			item, err := tx.LPop(listener.JobId, listener.messageBucket())
			if err != nil {
				return err
			}

			slog.Debug(
				"reset bucket",
				logPrefixBucket, listener.messageBucket(),
				logPrefixMessage, string(item),
			)
		}

		return nil
	})

	go listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
		size, err := tx.LSize(listener.JobId, listener.retryBucket())
		if err != nil {
			return err
		}

		for i := 0; i < size; i++ {
			item, err := tx.LPop(listener.JobId, listener.retryBucket())
			if err != nil {
				return err
			}

			slog.Debug(
				"reset bucket",
				logPrefixBucket, listener.retryBucket(),
				logPrefixMessage, string(item),
			)
		}

		return nil
	})
}

func (listener Listener[V]) messageBucket() []byte {
	return []byte(fmt.Sprintf("%s:%s", listener.JobId, listener.Id))
}

func (listener Listener[V]) retryBucket() []byte {
	return []byte(fmt.Sprintf("%s:%s:temp", listener.JobId, listener.Id))
}

func (listener *Listener[V]) deleteRetryMessage(id string) error {
	position := 0

	err := listener.kv.readBucketTx(func(tx *nutsdb.Tx) error {
		items, err := tx.LRange(listener.JobId, listener.retryBucket(), 0, -1)
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
		return listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
			return tx.LRemByIndex(listener.JobId, listener.retryBucket(), position-1)
		})
	}

	return ErrListenerRetryMessageNotFound
}

func (listener *Listener[V]) messages() (counter bucket.MessageCounter, err error) {
	counter.Name = listener.Id
	err = listener.kv.readBucketTx(func(tx *nutsdb.Tx) error {
		size, err := tx.LSize(listener.JobId, listener.messageBucket())
		if err != nil {
			if errors.Is(err, nutsdb.ErrListNotFound) {
				return nil
			}
			return err
		}

		counter.UnpublishMessage = size
		return nil
	})
	if err != nil {
		return bucket.MessageCounter{}, err
	}

	return counter, listener.kv.readBucketTx(func(tx *nutsdb.Tx) error {
		size, err := tx.LSize(listener.JobId, listener.retryBucket())
		if err != nil {
			if errors.Is(err, nutsdb.ErrListNotFound) {
				return nil
			}

			return err
		}

		counter.UnackMessage = size
		return nil
	})
}

func (listener *Listener[V]) enqueue(messages chan blockio.ResponseMessages) string {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	id := uuid.New().String()
	listener.PriorityQueue.Push(&pqueue.Item[V]{
		Id:    id,
		Value: messages,
	})
	go listener.metric.totalEnqueue.Inc()

	listener.PriorityQueue.Cond.Broadcast()

	return id
}

func (listener *Listener[V]) dequeue(id string) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for i := 0; i < listener.PriorityQueue.Len(); i++ {
		if listener.PriorityQueue.At(i).Id == id {
			listener.PriorityQueue.At(i).Value <- blockio.ResponseMessages{}
			listener.PriorityQueue.Remove(i)
			break
		}
	}

	go listener.metric.totalEnqueue.Dec()
	listener.PriorityQueue.Cond.Broadcast()
}

func (listener *Listener[V]) dispatcher() {
	for response := range listener.response {
		listener.notify(response)
	}
}

func (listener *Listener[V]) notify(response chan eventListener) {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	for listener.PriorityQueue.Len() == 0 && !listener.onShutdown.Load() && !listener.onRemove.Load() {
		slog.Debug(
			"wait request from queue",
			"size", listener.PriorityQueue.Len(),
		)
		listener.PriorityQueue.Cond.Wait()
	}

	if listener.onShutdown.Load() {
		response <- eventListener{
			Kind: shutdownKindEventListener,
		}
		return
	}

	if listener.onRemove.Load() {
		response <- eventListener{
			Kind: removedKindEventListener,
		}
		return
	}

	messages := make(blockio.ResponseMessages, 0)
	messageVisibilities := make(bucket.MessageVisibilities, 0)
	err := listener.kv.readBucketTx(func(tx *nutsdb.Tx) error {
		items, err := tx.LRange(listener.JobId, listener.messageBucket(), 0, 9)
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

			message.RetryPolicy.NextIter = time.Now().Add(listener.option.VisibilityDuration)
			messageVisibilities = append(messageVisibilities, message)
		}

		return nil
	})
	if err != nil {
		response <- eventListener{
			Kind:  failedDeliveryKindEventListener,
			Error: err,
		}
		return
	}

	err = listener.retryCatcher(messageVisibilities)
	if err != nil {
		response <- eventListener{
			Kind:  failedDeliveryKindEventListener,
			Error: err,
		}
		return
	}

	err = listener.popBucketMessage(len(messages))
	if err != nil {
		response <- eventListener{
			Kind:  failedDeliveryKindEventListener,
			Error: err,
		}
		return
	}

	listener.PriorityQueue.Peek().Value <- messages
	listener.PriorityQueue.Pop()

	response <- eventListener{
		Kind: deliveryKindEventListener,
	}

	go listener.metric.totalEnqueue.Dec()
	go listener.metric.totalConsumedMessage.Add(float64(len(messages)))
}

func (listener *Listener[V]) watcher() {
	for {
		ticker := time.NewTicker(500 * time.Millisecond)
		select {
		case <-listener.ctx.Done():
			slog.Debug(
				"[watcher] listener entering shutdown status",
				"removed", listener.onRemove,
				"shutdown", listener.onShutdown,
			)
			return
		case <-ticker.C:
			messageLength := 0
			err := listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
				size, err := tx.LSize(listener.JobId, listener.messageBucket())
				if err != nil {
					if errors.Is(err, nutsdb.ErrListNotFound) {
						return nil
					}
					return err
				}

				messageLength = size

				return nil
			})
			if err != nil {
				continue
			}

			if messageLength > 0 {
				eventListener := make(chan eventListener)
				listener.response <- eventListener

				switch event := <-eventListener; event.Kind {
				case removedKindEventListener:
					slog.Debug(
						"[watcher] listener entering shutdown status, listener removed",
						logPrefixConsumer, listener.Id,
					)
					return
				case shutdownKindEventListener:
					slog.Debug(
						"[watcher] listener entering shutdown status, server receive signal shutdown",
						logPrefixConsumer, listener.Id,
					)
					return
				case failedDeliveryKindEventListener:
					slog.Error(
						"[watcher] error sent to the incoming request",
						logPrefixErr, event.Error,
					)
				case deliveryKindEventListener:
					slog.Debug(
						"[watcher] success deliver message to the client",
						logPrefixConsumer, listener.Id,
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
			logPrefixErr, err,
			logPrefixConsumer, name,
		)
		return err
	}

	err = backoff.Retry(func() error {
		return listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
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

				err = tx.RPush(listener.JobId, listener.messageBucket(), b)
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
			logPrefixErr, err,
			logPrefixConsumer, name,
		)
		return err
	}

	return nil
}

func (listener *Listener[V]) retryWatcher() {
	for {
		select {
		case <-listener.ctx.Done():
			slog.Debug(
				"[retryWatcher] listener entering shutdown status",
				"removed", listener.onRemove,
				"shutdown", listener.onShutdown,
			)
			return
		default:
			message := bucket.MessageVisibility{}
			err := listener.kv.readBucketTx(func(tx *nutsdb.Tx) error {
				b, err := tx.LPeek(listener.JobId, listener.retryBucket())
				if err != nil {
					if errors.Is(err, nutsdb.ErrEmptyList) {
						return nil
					}
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
				err = listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
					b, _ := json.Marshal(message)
					err := tx.RPush(listener.JobId, listener.messageBucket(), b)
					if err != nil {
						return err
					}

					return nil
				})
				if err != nil {
					slog.Error(
						"[retryWatcher] error RPush retry message on bucket",
						logPrefixConsumer, string(listener.messageBucket()),
						logPrefixConsumer, string(listener.retryBucket()),
					)
					continue
				}
			}
			err = listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
				item, err := tx.LPop(listener.JobId, listener.retryBucket())
				if err != nil {
					return err
				}

				slog.Debug(
					"[retryWatcher] success pop the first element",
					logPrefixConsumer, string(listener.retryBucket()),
					logPrefixMessage, string(item),
				)

				return nil
			})
			if err != nil {
				slog.Error(
					"[retryWatcher] error LPop retry message on bucket",
					logPrefixErr, err,
					logPrefixConsumer, string(listener.messageBucket()),
					logPrefixConsumer, string(listener.retryBucket()),
				)
				continue
			}
		}
	}
}

func (listener *Listener[V]) retryCatcher(messageVisibilities bucket.MessageVisibilities) error {
	return backoff.Retry(func() error {
		return listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
			for _, message := range messageVisibilities {
				b, err := json.Marshal(message)
				if err != nil {
					return err
				}
				err = tx.RPush(listener.JobId, listener.retryBucket(), b)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}, backoff.NewExponentialBackOff())
}

func (listener *Listener[V]) popBucketMessage(size int) error {
	return listener.kv.updateBucketTx(func(tx *nutsdb.Tx) error {
		for i := 0; i < size; i++ {
			_, err := tx.LPop(listener.JobId, listener.messageBucket())
			if err != nil {
				return err
			}
		}

		return nil
	})
}
