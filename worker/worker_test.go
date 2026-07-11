package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/metric"
)

type claimResult struct {
	deliveries blockqueue.Deliveries
	err        error
}

type ackCall struct {
	messageID string
	err       error
}

type nackCall struct {
	messageID string
	delay     time.Duration
	errorText string
	err       error
}

type cancelCall struct {
	messageID string
	reason    string
	err       error
}

type fakeClient struct {
	claims      chan claimResult
	claimLimits chan int
	acks        chan ackCall
	nacks       chan nackCall
	cancels     chan cancelCall
	extensions  chan string
	txAcks      chan string
	txCancels   chan string
	txCalls     atomic.Int64

	mu         sync.Mutex
	ackErrors  []error
	nackErrors []error
	cancelErr  error
	extendErr  error
}

type batchingClient struct {
	*fakeClient
	ackBatches  chan int
	nackBatches chan int
	failAck     atomic.Bool
}

func newBatchingClient() *batchingClient {
	return &batchingClient{
		fakeClient:  newFakeClient(),
		ackBatches:  make(chan int, 16),
		nackBatches: make(chan int, 16),
	}
}

func (client *batchingClient) BatchAckDeliveries(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	items []blockqueue.BatchAckItem,
) []blockqueue.DeliveryResult {
	client.ackBatches <- len(items)
	results := make([]blockqueue.DeliveryResult, len(items))
	for index, item := range items {
		results[index] = blockqueue.DeliveryResult{MessageID: item.MessageID, Status: "processed"}
		if client.failAck.Load() {
			results[index].Status = "failed"
			results[index].Error = "injected batch failure"
		}
	}
	return results
}

func (client *batchingClient) BatchNackDeliveries(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	items []blockqueue.BatchNackItem,
) []blockqueue.DeliveryResult {
	client.nackBatches <- len(items)
	results := make([]blockqueue.DeliveryResult, len(items))
	for index, item := range items {
		results[index] = blockqueue.DeliveryResult{MessageID: item.MessageID, Status: "pending"}
	}
	return results
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		claims:      make(chan claimResult, 16),
		claimLimits: make(chan int, 32),
		acks:        make(chan ackCall, 32),
		nacks:       make(chan nackCall, 32),
		cancels:     make(chan cancelCall, 32),
		extensions:  make(chan string, 32),
		txAcks:      make(chan string, 32),
		txCancels:   make(chan string, 32),
	}
}

func (client *fakeClient) ClaimWait(
	ctx context.Context,
	_ blockqueue.Topic,
	_ string,
	limit int,
	_ time.Duration,
) (blockqueue.Deliveries, error) {
	client.claimLimits <- limit
	select {
	case <-ctx.Done():
		return blockqueue.Deliveries{}, nil
	case result := <-client.claims:
		if len(result.deliveries) > limit {
			remainder := append(blockqueue.Deliveries(nil), result.deliveries[limit:]...)
			client.claims <- claimResult{deliveries: remainder, err: result.err}
			result.deliveries = result.deliveries[:limit]
			result.err = nil
		}
		return result.deliveries, result.err
	}
}

func (client *fakeClient) AckDelivery(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
) error {
	err := client.popAckError()
	client.acks <- ackCall{messageID: messageID, err: err}
	return err
}

func (client *fakeClient) NackDelivery(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
	delay time.Duration,
	errorText string,
) error {
	err := client.popNackError()
	client.nacks <- nackCall{messageID: messageID, delay: delay, errorText: errorText, err: err}
	return err
}

func (client *fakeClient) ExtendLease(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
	extension time.Duration,
) (time.Time, error) {
	client.extensions <- messageID
	client.mu.Lock()
	err := client.extendErr
	client.mu.Unlock()
	return time.Now().Add(extension), err
}

func (client *fakeClient) CancelClaimedDelivery(
	_ context.Context,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
	reason string,
) error {
	client.mu.Lock()
	err := client.cancelErr
	client.mu.Unlock()
	client.cancels <- cancelCall{messageID: messageID, reason: reason, err: err}
	return err
}

func (client *fakeClient) WithTx(_ context.Context, _ *sql.TxOptions, fn func(*sql.Tx) error) error {
	client.txCalls.Add(1)
	return fn(nil)
}

func (client *fakeClient) AckDeliveryTx(
	_ context.Context,
	_ *sql.Tx,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
) error {
	client.txAcks <- messageID
	return nil
}

func (client *fakeClient) CancelClaimedDeliveryTx(
	_ context.Context,
	_ *sql.Tx,
	_ blockqueue.Topic,
	_ string,
	messageID string,
	_ string,
	_ string,
) error {
	client.txCancels <- messageID
	return nil
}

func (client *fakeClient) popAckError() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.ackErrors) == 0 {
		return nil
	}
	err := client.ackErrors[0]
	client.ackErrors = client.ackErrors[1:]
	return err
}

func (client *fakeClient) popNackError() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.nackErrors) == 0 {
		return nil
	}
	err := client.nackErrors[0]
	client.nackErrors = client.nackErrors[1:]
	return err
}

func testDelivery(message string) blockqueue.Delivery {
	expires := time.Now().Add(time.Minute)
	return blockqueue.Delivery{
		ID:             uuid.NewString(),
		Message:        message,
		ReceiptToken:   uuid.NewString(),
		LeaseExpiresAt: &expires,
	}
}

func testWorkerOptions() Options {
	return Options{
		DisableHeartbeat: true,
		DisableMetrics:   true,
		OperationTimeout: time.Second,
		DrainTimeout:     time.Second,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestWorkerMetricsRecordBoundedOutcomes(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("worker-metrics-" + uuid.NewString())
	options := testWorkerOptions()
	options.DisableMetrics = false
	options.MetricRegisterer = prometheus.NewRegistry()
	options.Concurrency = 3
	options.BatchSize = 3
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(_ context.Context, job *Job) error {
		switch job.Message {
		case "failed":
			return errors.New("temporary")
		case "cancelled":
			return CancelJob(errors.New("permanent"))
		default:
			return nil
		}
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{
		testDelivery("processed"), testDelivery("failed"), testDelivery("cancelled"),
	}}
	<-client.acks
	<-client.nacks
	<-client.cancels
	cancel()
	require.NoError(t, <-result)

	require.Equal(t, float64(1), testutil.ToFloat64(
		metric.WorkerJobs.WithLabelValues(topic.Name, "fulfillment", "processed")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metric.WorkerJobs.WithLabelValues(topic.Name, "fulfillment", "nacked")))
	require.Equal(t, float64(1), testutil.ToFloat64(
		metric.WorkerJobs.WithLabelValues(topic.Name, "fulfillment", "cancelled")))
	require.Zero(t, testutil.ToFloat64(
		metric.WorkerActiveHandlers.WithLabelValues(topic.Name, "fulfillment")))
}

func TestWorkerMetricsRecordHeartbeatLeaseLoss(t *testing.T) {
	client := newFakeClient()
	client.extendErr = blockqueue.ErrLeaseLost
	topic := blockqueue.NewTopic("worker-heartbeat-metrics-" + uuid.NewString())
	options := testWorkerOptions()
	options.DisableMetrics = false
	options.MetricRegisterer = prometheus.NewRegistry()
	options.DisableHeartbeat = false
	options.LeaseDuration = 60 * time.Millisecond
	options.HeartbeatInterval = 10 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, _ *Job) error {
		<-ctx.Done()
		return ctx.Err()
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("lease loss")}}
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metric.WorkerHeartbeats.WithLabelValues(
			topic.Name, "fulfillment", "lease_lost")) == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-result)
	require.Equal(t, float64(1), testutil.ToFloat64(
		metric.WorkerJobs.WithLabelValues(topic.Name, "fulfillment", "lease_lost")))
}

func TestWorkerMetricsRecordHeartbeatFailureAndRecovery(t *testing.T) {
	client := newFakeClient()
	client.extendErr = errors.New("temporary heartbeat failure")
	topic := blockqueue.NewTopic("worker-heartbeat-recovery-" + uuid.NewString())
	release := make(chan struct{})
	options := testWorkerOptions()
	options.DisableMetrics = false
	options.MetricRegisterer = prometheus.NewRegistry()
	options.DisableHeartbeat = false
	options.LeaseDuration = 100 * time.Millisecond
	options.HeartbeatInterval = 10 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		<-release
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("heartbeat recovery")}}
	<-client.extensions
	client.mu.Lock()
	client.extendErr = nil
	client.mu.Unlock()
	<-client.extensions
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metric.WorkerHeartbeats.WithLabelValues(
			topic.Name, "fulfillment", "success")) == 1
	}, time.Second, time.Millisecond)
	close(release)
	<-client.acks
	cancel()
	require.NoError(t, <-result)
	require.Equal(t, float64(1), testutil.ToFloat64(
		metric.WorkerHeartbeats.WithLabelValues(topic.Name, "fulfillment", "failed")))
}

func TestWorkerMetricsDisabledDoesNotRegisterCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	options := testWorkerOptions()
	options.MetricRegisterer = registry
	_, err := New(
		newFakeClient(), blockqueue.NewTopic("metrics-disabled"), "worker",
		HandlerFunc(func(context.Context, *Job) error { return nil }), options,
	)
	require.NoError(t, err)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func runTestWorker(t *testing.T, worker *Worker) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- worker.Run(ctx) }()
	return cancel, result
}

func TestWorkerAutoAcknowledgesSuccessfulHandler(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery(`{"order_id":"one"}`)
	handled := make(chan string, 1)
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(_ context.Context, job *Job) error {
		handled <- job.ID
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	require.Equal(t, delivery.ID, <-handled)
	require.Equal(t, delivery.ID, (<-client.acks).messageID)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.nacks)
}

func TestWorkerNacksHandlerErrorWithRequestedDelay(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("failure")
	handlerErr := errors.New("inventory unavailable")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return RetryAfter(3*time.Second, handlerErr)
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	nack := <-client.nacks
	require.Equal(t, delivery.ID, nack.messageID)
	require.Equal(t, 3*time.Second, nack.delay)
	require.Equal(t, handlerErr.Error(), nack.errorText)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
}

func TestWorkerRecoversPanicAndNacks(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		panic("broken handler")
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("panic")}}
	nack := <-client.nacks
	require.Contains(t, nack.errorText, "worker handler panic: broken handler")
	cancel()
	require.NoError(t, <-result)
}

func TestManualNackPreventsAutomaticCompletion(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("manual nack")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, job *Job) error {
		return job.Nack(ctx, 2*time.Second, errors.New("manual retry"))
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	nack := <-client.nacks
	require.Equal(t, delivery.ID, nack.messageID)
	require.Equal(t, 2*time.Second, nack.delay)
	require.Equal(t, "manual retry", nack.errorText)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
}

func TestWorkerRetriesAmbiguousAck(t *testing.T) {
	client := newFakeClient()
	client.ackErrors = []error{errors.New("connection reset after commit"), nil}
	topic := blockqueue.NewTopic("orders")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("retry ack")}}
	require.Error(t, (<-client.acks).err)
	require.NoError(t, (<-client.acks).err)
	cancel()
	require.NoError(t, <-result)
}

func TestWorkerBatchesAutomaticAcks(t *testing.T) {
	client := newBatchingClient()
	topic := blockqueue.NewTopic("orders")
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	options := testWorkerOptions()
	options.Concurrency = 2
	options.BatchSize = 2
	options.CompletionBatchSize = 2
	options.CompletionFlushInterval = 50 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		ready <- struct{}{}
		<-release
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("one"), testDelivery("two")}}
	<-ready
	<-ready
	close(release)
	require.Equal(t, 2, <-client.ackBatches)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
}

func TestWorkerRetriesFailedBatchItemIndividually(t *testing.T) {
	client := newBatchingClient()
	client.failAck.Store(true)
	topic := blockqueue.NewTopic("orders")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("fallback")}}
	require.Equal(t, 1, <-client.ackBatches)
	require.NoError(t, (<-client.acks).err)
	cancel()
	require.NoError(t, <-result)
}

func TestWorkerHeartbeatsLongHandler(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	release := make(chan struct{})
	started := make(chan struct{})
	options := testWorkerOptions()
	options.DisableHeartbeat = false
	options.LeaseDuration = 60 * time.Millisecond
	options.HeartbeatInterval = 10 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		close(started)
		<-release
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("slow")}}
	<-started
	select {
	case <-client.extensions:
	case <-time.After(time.Second):
		require.Fail(t, "worker did not extend the active lease")
	}
	close(release)
	<-client.acks
	cancel()
	require.NoError(t, <-result)
}

func TestHeartbeatLeaseLossCancelsHandlerWithoutCompletion(t *testing.T) {
	client := newFakeClient()
	client.extendErr = blockqueue.ErrLeaseLost
	topic := blockqueue.NewTopic("orders")
	cause := make(chan error, 1)
	options := testWorkerOptions()
	options.DisableHeartbeat = false
	options.LeaseDuration = 60 * time.Millisecond
	options.HeartbeatInterval = 10 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, _ *Job) error {
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("lease lost")}}
	require.ErrorIs(t, <-cause, blockqueue.ErrLeaseLost)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestJobCompleteTxCommitsBusinessEffectAndAckOnce(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("transactional")
	var effects atomic.Int64
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, job *Job) error {
		return job.CompleteTx(ctx, nil, func(*sql.Tx) error {
			effects.Add(1)
			return nil
		})
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	require.Equal(t, delivery.ID, <-client.txAcks)
	require.Eventually(t, func() bool { return effects.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-result)
	require.EqualValues(t, 1, client.txCalls.Load())
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestJobCompleteTxFailureFallsBackToNack(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("transaction rollback")
	rollbackErr := errors.New("business transaction rolled back")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, job *Job) error {
		return job.CompleteTx(ctx, nil, func(*sql.Tx) error { return rollbackErr })
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	nack := <-client.nacks
	require.Equal(t, rollbackErr.Error(), nack.errorText)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.txAcks)
	require.Empty(t, client.acks)
}

func TestJSONWorkerDecodesTypedArguments(t *testing.T) {
	type orderArgs struct {
		OrderID string `json:"order_id"`
	}
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	received := make(chan orderArgs, 1)
	runner, err := NewJSON(client, topic, "fulfillment", TypedHandlerFunc[orderArgs](
		func(_ context.Context, job *TypedJob[orderArgs]) error {
			received <- job.Args
			return nil
		},
	), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery(`{"order_id":"order-42"}`)}}
	require.Equal(t, "order-42", (<-received).OrderID)
	<-client.acks
	cancel()
	require.NoError(t, <-result)
}

func TestWorkerLimitsConcurrentHandlers(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	release := make(chan struct{})
	var active atomic.Int64
	var maximum atomic.Int64
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		return nil
	}), Options{
		Concurrency:      2,
		BatchSize:        2,
		DisableHeartbeat: true,
		OperationTimeout: time.Second,
		DrainTimeout:     time.Second,
		Logger:           testWorkerOptions().Logger,
	})
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("one"), testDelivery("two")}}
	require.Eventually(t, func() bool { return active.Load() == 2 }, time.Second, time.Millisecond)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("three"), testDelivery("four")}}
	time.Sleep(20 * time.Millisecond)
	require.EqualValues(t, 2, maximum.Load())
	close(release)
	for range 4 {
		<-client.acks
	}
	cancel()
	require.NoError(t, <-result)
	require.EqualValues(t, 2, maximum.Load())
	for len(client.claimLimits) > 0 {
		require.LessOrEqual(t, <-client.claimLimits, 2)
	}
}

func TestWorkerPauseIsNormalAndLoggedOnce(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	var logs bytes.Buffer
	options := testWorkerOptions()
	options.PausePollInterval = time.Millisecond
	options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{err: blockqueue.ErrResourcePaused}
	client.claims <- claimResult{err: blockqueue.ErrResourcePaused}
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("resumed")}}
	<-client.acks
	cancel()
	require.NoError(t, <-result)
	require.Equal(t, 1, strings.Count(logs.String(), "blockqueue worker paused"))
	require.Equal(t, 1, strings.Count(logs.String(), "blockqueue worker resumed"))
	require.NotContains(t, logs.String(), "claim failed")
}

func TestClaimErrorWithDeliveriesReleasesEveryReservedSlot(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	options := testWorkerOptions()
	options.OperationTimeout = time.Second
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{
		deliveries: blockqueue.Deliveries{testDelivery("partial result")},
		err:        errors.New("injected mixed claim result"),
	}
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("after error")}}
	select {
	case <-client.acks:
	case <-time.After(time.Second):
		require.Fail(t, "claim slot leaked after mixed delivery/error result")
	}
	cancel()
	require.NoError(t, <-result)
}

func TestWorkerDrainsActiveHandlerAfterRunCancellation(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	started := make(chan struct{})
	release := make(chan struct{})
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		close(started)
		<-release
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("drain")}}
	<-started
	cancel()
	select {
	case err := <-result:
		require.Failf(t, "worker returned before active handler drained", "error=%v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	<-client.acks
	require.NoError(t, <-result)
}

func TestWorkerCancelsHandlerAtDrainDeadline(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	started := make(chan struct{})
	cause := make(chan error, 1)
	options := testWorkerOptions()
	options.DrainTimeout = 30 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("timeout")}}
	<-started
	cancel()
	require.ErrorIs(t, <-result, ErrDrainTimeout)
	require.ErrorIs(t, <-cause, ErrDrainTimeout)
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestWorkerHardWaitIsBoundedWhenHandlerIgnoresCancellation(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	options := testWorkerOptions()
	options.DrainTimeout = 30 * time.Millisecond
	options.HardStopTimeout = 30 * time.Millisecond
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		close(started)
		<-release // Deliberately violates the handler context contract.
		close(finished)
		return nil
	}), options)
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("ignore cancellation")}}
	<-started
	startedAt := time.Now()
	cancel()
	require.ErrorIs(t, <-result, ErrDrainTimeout)
	require.GreaterOrEqual(t, time.Since(startedAt), 50*time.Millisecond)
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		require.Fail(t, "test handler did not exit after release")
	}
}

func TestJSONDecodeFailureFollowsNackAndDLQPolicy(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	runner, err := NewJSON(client, topic, "fulfillment", TypedHandlerFunc[map[string]string](
		func(context.Context, *TypedJob[map[string]string]) error {
			require.Fail(t, "handler must not receive malformed JSON")
			return nil
		},
	), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("not-json")}}
	nack := <-client.nacks
	require.Contains(t, nack.errorText, "decode worker message:")
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.cancels)
}

func TestCancelJobSentinelCancelsPermanentFailure(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("poison")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return CancelJob(errors.New("unsupported schema version"))
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	cancellation := <-client.cancels
	require.Equal(t, delivery.ID, cancellation.messageID)
	require.Equal(t, "unsupported schema version", cancellation.reason)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestWorkerBoundsFailureAndCancellationTextForCustomClients(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	longText := strings.Repeat("😀", blockqueue.MaxDeliveryTextBytes)
	var attempts atomic.Int64
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		if attempts.Add(1) == 1 {
			return errors.New(longText)
		}
		return CancelJob(errors.New(longText))
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)

	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("bounded failure")}}
	nack := <-client.nacks
	require.LessOrEqual(t, len([]byte(nack.errorText)), blockqueue.MaxDeliveryTextBytes)
	require.True(t, utf8.ValidString(nack.errorText))
	require.True(t, strings.HasSuffix(nack.errorText, "…[truncated]"))

	client.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("bounded cancellation")}}
	cancellation := <-client.cancels
	require.LessOrEqual(t, len([]byte(cancellation.reason)), blockqueue.MaxDeliveryTextBytes)
	require.True(t, utf8.ValidString(cancellation.reason))
	require.True(t, strings.HasSuffix(cancellation.reason, "…[truncated]"))

	cancel()
	require.NoError(t, <-result)
}

func TestManualJobCancelPreventsAutomaticCompletion(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("manual cancel")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, job *Job) error {
		return job.Cancel(ctx, "operator policy")
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	cancellation := <-client.cancels
	require.Equal(t, "operator policy", cancellation.reason)
	cancel()
	require.NoError(t, <-result)
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestJobCancelTxCommitsCancellationOnce(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	delivery := testDelivery("transactional cancel")
	var effects atomic.Int64
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(ctx context.Context, job *Job) error {
		return job.CancelTx(ctx, nil, "permanent", func(*sql.Tx) error {
			effects.Add(1)
			return nil
		})
	}), testWorkerOptions())
	require.NoError(t, err)
	cancel, result := runTestWorker(t, runner)
	client.claims <- claimResult{deliveries: blockqueue.Deliveries{delivery}}
	require.Equal(t, delivery.ID, <-client.txCancels)
	cancel()
	require.NoError(t, <-result)
	require.EqualValues(t, 1, effects.Load())
	require.Empty(t, client.acks)
	require.Empty(t, client.nacks)
}

func TestWorkerRejectsInvalidConfigurationAndSecondRun(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	_, err := New(client, topic, "fulfillment", nil, Options{})
	require.ErrorIs(t, err, ErrInvalidConfiguration)
	_, err = New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error { return nil }), Options{
		LeaseDuration:     time.Second,
		HeartbeatInterval: time.Second,
	})
	require.ErrorIs(t, err, ErrInvalidConfiguration)

	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error { return nil }), testWorkerOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, runner.Run(ctx))
	require.ErrorIs(t, runner.Run(context.Background()), ErrAlreadyStarted)
}

func TestHeartbeatJitterIsDeterministicAndBounded(t *testing.T) {
	interval := 20 * time.Second
	first := jitterHeartbeat(interval, "receipt-one")
	require.Equal(t, first, jitterHeartbeat(interval, "receipt-one"))
	require.GreaterOrEqual(t, first, 16*time.Second)
	require.LessOrEqual(t, first, interval)
	require.NotEqual(t, first, jitterHeartbeat(interval, "receipt-two"))
}

func TestWorkerReturnsTerminalClaimError(t *testing.T) {
	client := newFakeClient()
	topic := blockqueue.NewTopic("orders")
	runner, err := New(client, topic, "fulfillment", HandlerFunc(func(context.Context, *Job) error { return nil }), testWorkerOptions())
	require.NoError(t, err)
	client.claims <- claimResult{err: blockqueue.ErrSubscriberDeleted}
	require.ErrorIs(t, runner.Run(context.Background()), blockqueue.ErrSubscriberDeleted)
}
