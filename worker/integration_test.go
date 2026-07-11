package worker

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func setupIntegrationQueue(t *testing.T) (*blockqueue.Queue, store.Driver, blockqueue.Topic, blockqueue.Subscriber) {
	t.Helper()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "worker.db"), sqlite.Config{BusyTimeout: 5000})
	require.NoError(t, err)
	return setupIntegrationQueueWithDriver(t, driver)
}

func setupIntegrationQueueWithDriver(
	t *testing.T,
	driver store.Driver,
) (*blockqueue.Queue, store.Driver, blockqueue.Topic, blockqueue.Subscriber) {
	t.Helper()
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
	require.NoError(t, queue.Run(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, queue.Shutdown(ctx))
	})
	topic := blockqueue.NewTopic("orders")
	subscriber := blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "200ms",
		DequeueBatchSize:   2,
		RetryPolicy: blockqueue.RetryPolicy{
			InitialDelay: "1ms",
			MaxDelay:     "10ms",
			Multiplier:   1,
			Jitter:       0.01,
		},
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{subscriber}))
	return queue, driver, topic, subscriber
}

func integrationWorkerOptions() Options {
	options := testWorkerOptions()
	options.DisableHeartbeat = false
	options.LeaseDuration = 200 * time.Millisecond
	options.HeartbeatInterval = 50 * time.Millisecond
	return options
}

func TestWorkerSQLiteCompleteTxIsAtomicWithAck(t *testing.T) {
	queue, driver, topic, subscriber := setupIntegrationQueue(t)
	runCompleteTxIntegration(t, queue, driver, topic, subscriber, "?")
	runCancelTxIntegration(t, queue, driver, topic, subscriber, "?")
}

func TestWorkerPostgreSQLCompleteTxIsAtomicWithAck(t *testing.T) {
	rawURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
	if rawURL == "" {
		t.Skip("BLOCKQUEUE_TEST_POSTGRES_URL is not set")
	}
	schema, err := testdb.OpenPostgreSQLSchema(context.Background(), rawURL, "_test", "worker")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, schema.Close()) })
	queue, driver, topic, subscriber := setupIntegrationQueueWithDriver(t, schema.Driver)
	runCompleteTxIntegration(t, queue, driver, topic, subscriber, "$1")
	runCancelTxIntegration(t, queue, driver, topic, subscriber, "$1")
}

func runCompleteTxIntegration(
	t *testing.T,
	queue *blockqueue.Queue,
	driver store.Driver,
	topic blockqueue.Topic,
	subscriber blockqueue.Subscriber,
	placeholder string,
) {
	t.Helper()
	_, err := driver.DB().ExecContext(context.Background(), `
		CREATE TABLE order_effects (
			message_id TEXT PRIMARY KEY,
			processed_at TIMESTAMP NOT NULL
		)
	`)
	require.NoError(t, err)

	completed := make(chan struct{})
	runner, err := New(queue, topic, subscriber.Name, HandlerFunc(func(ctx context.Context, job *Job) error {
		err := job.CompleteTx(ctx, nil, func(tx *sql.Tx) error {
			_, execErr := tx.ExecContext(ctx,
				"INSERT INTO order_effects (message_id, processed_at) VALUES ("+placeholder+", "+nextPlaceholder(placeholder)+")",
				job.ID, time.Now().UTC())
			return execErr
		})
		if err == nil {
			close(completed)
		}
		return err
	}), integrationWorkerOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runner.Run(ctx) }()

	receipt, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{Message: `{"order_id":"42"}`})
	require.NoError(t, err)
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "transactional worker did not complete")
	}
	cancel()
	require.NoError(t, <-runResult)

	var effects int
	require.NoError(t, driver.DB().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM order_effects WHERE message_id = "+placeholder, receipt.MessageID).Scan(&effects))
	require.Equal(t, 1, effects)
	status, err := queue.GetMessageStatus(context.Background(), topic, receipt.MessageID)
	require.NoError(t, err)
	require.Len(t, status.Deliveries, 1)
	require.Equal(t, "processed", status.Deliveries[0].Status)
}

func nextPlaceholder(first string) string {
	if first == "$1" {
		return "$2"
	}
	return "?"
}

func runCancelTxIntegration(
	t *testing.T,
	queue *blockqueue.Queue,
	driver store.Driver,
	topic blockqueue.Topic,
	subscriber blockqueue.Subscriber,
	placeholder string,
) {
	t.Helper()
	_, err := driver.DB().ExecContext(context.Background(), `
		CREATE TABLE cancellation_effects (
			message_id TEXT PRIMARY KEY,
			reason TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	completed := make(chan struct{})
	runner, err := New(queue, topic, subscriber.Name, HandlerFunc(func(ctx context.Context, job *Job) error {
		err := job.CancelTx(ctx, nil, "unsupported payload", func(tx *sql.Tx) error {
			_, execErr := tx.ExecContext(ctx,
				"INSERT INTO cancellation_effects (message_id, reason) VALUES ("+placeholder+", "+nextPlaceholder(placeholder)+")",
				job.ID, "unsupported payload")
			return execErr
		})
		if err == nil {
			close(completed)
		}
		return err
	}), integrationWorkerOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runner.Run(ctx) }()

	receipt, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{Message: "poison"})
	require.NoError(t, err)
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "transactional worker cancellation did not complete")
	}
	cancel()
	require.NoError(t, <-runResult)

	var effects int
	require.NoError(t, driver.DB().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM cancellation_effects WHERE message_id = "+placeholder,
		receipt.MessageID).Scan(&effects))
	require.Equal(t, 1, effects)
	status, err := queue.GetMessageStatus(context.Background(), topic, receipt.MessageID)
	require.NoError(t, err)
	require.Len(t, status.Deliveries, 1)
	require.Equal(t, "cancelled", status.Deliveries[0].Status)
	require.Equal(t, "unsupported payload", status.Deliveries[0].CancelReason)
}

func TestWorkerSQLiteRetriesThenProcesses(t *testing.T) {
	queue, _, topic, subscriber := setupIntegrationQueue(t)
	var attempts atomic.Int64
	runner, err := New(queue, topic, subscriber.Name, HandlerFunc(func(context.Context, *Job) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary downstream failure")
		}
		return nil
	}), integrationWorkerOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runner.Run(ctx) }()

	receipt, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{Message: "retry"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, statusErr := queue.GetMessageStatus(context.Background(), topic, receipt.MessageID)
		return statusErr == nil && len(status.Deliveries) == 1 && status.Deliveries[0].Status == "processed"
	}, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-runResult)

	status, err := queue.GetMessageStatus(context.Background(), topic, receipt.MessageID)
	require.NoError(t, err)
	require.Equal(t, 2, status.Deliveries[0].DeliveryCount)
	require.Equal(t, 1, status.Deliveries[0].FailureCount)
	require.EqualValues(t, 2, attempts.Load())
}
