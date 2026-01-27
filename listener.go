package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
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

// Helper functions for default values
func defaultInt(val, def int) int {
	if val <= 0 {
		return def
	}
	return val
}

func defaultDuration(val, def time.Duration) time.Duration {
	if val <= 0 {
		return def
	}
	return val
}

type kindEventListener string

const (
	shutdownKindEventListener       kindEventListener = "shutdown"
	removedKindEventListener        kindEventListener = "remove"
	failedDeliveryKindEventListener kindEventListener = "failed_delivery"
	deliveryKindEventListener       kindEventListener = "delivery"
)

type eventListener struct {
	Kind  kindEventListener
	Error error
}

type Listener[V chan blockio.ResponseMessages] struct {
	Id            string
	SubscriberId  uuid.UUID
	JobId         string
	PriorityQueue *pqueue.PriorityQueue[V]

	db         *db
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
	DequeueBatchSize   int           // Number of messages to dequeue at once (default: 10)
	PollInterval       time.Duration // Base polling interval (default: 500ms)
	MaxBackoff         time.Duration // Max backoff on empty queue (default: 5s)
}

type listenerMetric struct {
	totalConsumedMessage *prometheus.CounterVec
	totalEnqueue         *prometheus.GaugeVec
}

func newListener[V chan blockio.ResponseMessages](serverCtx context.Context, jobId string, subscriber SubscriberInfo, database *db) (*Listener[V], error) {
	ctx, cancel := context.WithCancel(serverCtx)

	listener := &Listener[V]{
		db:            database,
		ctx:           ctx,
		cancelFunc:    cancel,
		Id:            subscriber.Name,
		SubscriberId:  subscriber.Id,
		JobId:         jobId,
		PriorityQueue: pqueue.New[V](),
		option: listenerOption{
			MaxAttempts:        subscriber.MaxAttempts,
			VisibilityDuration: subscriber.VisibilityDuration,
			DequeueBatchSize:   defaultInt(subscriber.DequeueBatchSize, 10),
			PollInterval:       defaultDuration(subscriber.PollInterval, 500*time.Millisecond),
			MaxBackoff:         defaultDuration(subscriber.MaxBackoff, 5*time.Second),
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

// SubscriberInfo holds parsed subscriber information
type SubscriberInfo struct {
	Id                 uuid.UUID
	Name               string
	MaxAttempts        int
	VisibilityDuration time.Duration
	DequeueBatchSize   int
	PollInterval       time.Duration
	MaxBackoff         time.Duration
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
	// Delete all messages for this subscriber from SQLite
	go func() {
		err := listener.db.deleteSubscriberMessages(context.Background(), listener.SubscriberId)
		if err != nil {
			slog.Error(
				"error reset subscriber messages",
				logPrefixErr, err,
				logPrefixConsumer, listener.Id,
			)
		}
	}()
}

func (listener *Listener[V]) deleteRetryMessage(id string) error {
	err := listener.db.ackSubscriberMessage(context.Background(), listener.SubscriberId, id)
	if err != nil {
		if errors.Is(err, ErrMessageNotFound) {
			return ErrListenerRetryMessageNotFound
		}
		return err
	}
	return nil
}

func (listener *Listener[V]) messages() (MessageCounter, error) {
	stats, err := listener.db.getSubscriberQueueStats(context.Background(), listener.SubscriberId)
	if err != nil {
		return MessageCounter{}, err
	}

	return MessageCounter{
		Name:             listener.Id,
		UnpublishMessage: stats.Pending,
		UnackMessage:     stats.Delivered,
	}, nil
}

// MessageCounter holds message statistics for a subscriber
type MessageCounter struct {
	Name             string
	UnpublishMessage int
	UnackMessage     int
}

func (listener *Listener[V]) enqueue(messages chan blockio.ResponseMessages) string {
	listener.PriorityQueue.Cond.L.Lock()
	defer listener.PriorityQueue.Cond.L.Unlock()

	id := uuid.New().String()
	listener.PriorityQueue.Push(&pqueue.Item[V]{
		Id:    id,
		Value: messages,
	})
	listener.metric.totalEnqueue.WithLabelValues(listener.JobId, listener.Id).Inc()

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

	listener.metric.totalEnqueue.WithLabelValues(listener.JobId, listener.Id).Dec()
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

	// Dequeue messages from SQLite using configurable batch size
	ctx := context.Background()
	dbMessages, err := listener.db.dequeueMessages(ctx, listener.SubscriberId, listener.option.DequeueBatchSize, listener.option.VisibilityDuration)
	if err != nil {
		response <- eventListener{
			Kind:  failedDeliveryKindEventListener,
			Error: err,
		}
		return
	}

	// Convert to response messages
	messages := make(blockio.ResponseMessages, 0, len(dbMessages))
	for _, m := range dbMessages {
		messages = append(messages, blockio.ResponseMessage{
			Id:      m.MessageId,
			Message: m.Message,
		})
	}

	listener.PriorityQueue.Peek().Value <- messages
	listener.PriorityQueue.Pop()

	response <- eventListener{
		Kind: deliveryKindEventListener,
	}

	listener.metric.totalEnqueue.WithLabelValues(listener.JobId, listener.Id).Dec()
	listener.metric.totalConsumedMessage.WithLabelValues(listener.JobId, listener.Id).Add(float64(len(messages)))
}

func (listener *Listener[V]) watcher() {
	// Start with base poll interval
	currentInterval := listener.option.PollInterval

	for {
		ticker := time.NewTicker(currentInterval)
		select {
		case <-listener.ctx.Done():
			ticker.Stop()
			slog.Debug(
				"[watcher] listener entering shutdown status",
				"removed", listener.onRemove,
				"shutdown", listener.onShutdown,
			)
			return
		case <-ticker.C:
			ticker.Stop()

			// Check if there are pending messages in SQLite
			stats, err := listener.db.getSubscriberQueueStats(context.Background(), listener.SubscriberId)
			if err != nil {
				continue
			}

			if stats.Pending > 0 {
				// Reset to base interval when there are messages
				currentInterval = listener.option.PollInterval

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
			} else {
				// Exponential backoff when queue is empty (reduces CPU usage)
				currentInterval = min(currentInterval*2, listener.option.MaxBackoff)
			}
		}
	}
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
			// Requeue expired messages using SQLite
			err := listener.db.requeueExpiredMessages(
				context.Background(),
				listener.SubscriberId,
				listener.option.MaxAttempts,
				listener.option.VisibilityDuration,
			)
			if err != nil {
				slog.Error(
					"[retryWatcher] error requeue expired messages",
					logPrefixErr, err,
					logPrefixConsumer, listener.Id,
				)
			}

			time.Sleep(1 * time.Second)
		}
	}
}
