package blockqueue

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type countingLogHandler struct {
	message string
	count   *atomic.Int64
}

func (handler countingLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler countingLogHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == handler.message {
		handler.count.Add(1)
	}
	return nil
}
func (handler countingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler countingLogHandler) WithGroup(string) slog.Handler      { return handler }

func TestCheckpointDeferralIsNilSafeAndBounded(t *testing.T) {
	queue := &Queue{}
	require.False(t, queue.shouldDeferCheckpoint(time.Minute))

	queue.writer = &writer{batchSize: 10}
	queue.writer.pendingMsgs.Store(11)
	require.True(t, queue.shouldDeferCheckpoint(time.Minute))
	require.False(t, queue.shouldDeferCheckpoint(2*time.Minute))
}

func TestMaintenanceRetryWaitsForBackoffOrCancellation(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- waitForMaintenanceRetry(ctx, clock) }()

	select {
	case <-done:
		t.Fatal("maintenance retry returned before the backoff elapsed")
	case <-time.After(10 * time.Millisecond):
	}
	clock.Advance(time.Second)
	require.True(t, <-done)

	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	require.False(t, waitForMaintenanceRetry(cancelled, clock))
	cancel()
}

func TestDeliveryReaperWriteFailureDoesNotHotSpin(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "reaper-backoff.db"), sqlite.Config{BusyTimeout: 1})
	require.NoError(t, err)
	queue := New(driver, Options{Clock: clock})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("reaper-backoff")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "10ms",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "expire"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, subscriber.Name, 1, 10*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	_, err = testDB(driver).Exec(`
		CREATE TRIGGER reject_reaper_update
		BEFORE UPDATE OF status ON message_deliveries
		WHEN OLD.status = 'delivered'
		BEGIN SELECT RAISE(ABORT, 'injected reaper failure'); END`)
	require.NoError(t, err)

	var failures atomic.Int64
	previous := slog.Default()
	slog.SetDefault(slog.New(countingLogHandler{message: "delivery reaper failed", count: &failures}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	time.Sleep(15 * time.Millisecond)
	queue.signalReaper()
	require.Eventually(t, func() bool { return failures.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, failures.Load(), "reaper retried before its backoff elapsed")
	clock.Advance(time.Second)
	require.Eventually(t, func() bool { return failures.Load() == 2 }, time.Second, time.Millisecond)
}

func TestSchedulerClaimFailureDoesNotHotSpin(t *testing.T) {
	clock := &testClock{now: time.Now().UTC().Truncate(time.Minute)}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "scheduler-backoff.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{Clock: clock})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("scheduler-backoff")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	_, err = queue.CreateSchedule(context.Background(), topic, ScheduleInput{
		Name: "each-minute", CronExpression: "* * * * *", Timezone: "UTC", Message: "tick",
	})
	require.NoError(t, err)
	_, err = testDB(driver).Exec(`
		CREATE TRIGGER reject_scheduler_claim
		BEFORE UPDATE OF owner_id ON schedules
		BEGIN SELECT RAISE(ABORT, 'injected scheduler claim failure'); END`)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})

	var failures atomic.Int64
	previous := slog.Default()
	slog.SetDefault(slog.New(countingLogHandler{message: "scheduler claim failed", count: &failures}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	clock.Advance(time.Minute)
	require.Eventually(t, func() bool { return failures.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, failures.Load(), "scheduler retried before its backoff elapsed")
	clock.Advance(time.Second)
	require.Eventually(t, func() bool { return failures.Load() == 2 }, time.Second, time.Millisecond)
}
