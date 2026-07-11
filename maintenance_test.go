package blockqueue

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestMaintenanceYieldUsesBoundedClockDelay(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	done := make(chan bool, 1)
	go func() { done <- waitForMaintenanceYield(context.Background(), clock) }()

	select {
	case <-done:
		t.Fatal("maintenance pass continued before yielding")
	case <-time.After(10 * time.Millisecond):
	}
	clock.Advance(maintenanceYieldInterval)
	require.True(t, <-done)
}

func TestDeliveryReaperWriteFailureDoesNotHotSpin(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "reaper-backoff.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{Clock: clock})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("reaper-backoff")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "expire"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, subscriber.Name, 1, time.Minute)
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
	_, err = testDB(driver).Exec(
		"UPDATE message_deliveries SET lease_expires_at = ? WHERE message_id = ? AND subscriber_id = ?",
		time.Now().UTC().Add(-time.Second), claimed[0].ID, subscriber.ID,
	)
	require.NoError(t, err)
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

func TestDeliveryReaperYieldsAfterBoundedPass(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "reaper-fairness.db"), sqlite.Config{BusyTimeout: 5000})
	require.NoError(t, err)
	queue := New(driver, Options{Clock: clock, Writer: WriterOptions{BatchSize: 1, FlushInterval: time.Millisecond}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("reaper-fairness")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})

	rows := deliveryReaperBatchSize*deliveryReaperPassLimit + 1
	expired := time.Now().UTC().Add(-time.Minute)
	_, err = testDB(driver).Exec(`
		WITH RECURSIVE sequence(value) AS (
			SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < ?
		)
		INSERT INTO messages (id, topic_id, message, scheduled_at)
		SELECT printf('reaper-%05d', value), ?, 'payload', ? FROM sequence`, rows, topic.ID, expired)
	require.NoError(t, err)
	_, err = testDB(driver).Exec(`
		WITH RECURSIVE sequence(value) AS (
			SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < ?
		)
		INSERT INTO message_deliveries
			(message_id, subscriber_id, status, delivery_count, visible_at, receipt_token,
			 lease_expires_at, message_created_at)
		SELECT printf('reaper-%05d', value), ?, 'delivered', 1, ?,
		       printf('receipt-%05d', value), ?, ? FROM sequence`,
		rows, subscriber.ID, expired, expired, expired)
	require.NoError(t, err)
	queue.signalReaper()

	require.Eventually(t, func() bool {
		var delivered int
		return testDB(driver).Get(&delivered,
			"SELECT COUNT(*) FROM message_deliveries WHERE status = 'delivered'") == nil && delivered == 1
	}, 15*time.Second, 10*time.Millisecond)
	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "writer-still-progresses"})
	require.NoError(t, err)
	clock.Advance(maintenanceYieldInterval)
	require.Eventually(t, func() bool {
		var delivered int
		return testDB(driver).Get(&delivered,
			"SELECT COUNT(*) FROM message_deliveries WHERE status = 'delivered'") == nil && delivered == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSchedulerYieldsAfterBoundedPass(t *testing.T) {
	clock := &testClock{now: time.Now().UTC().Truncate(time.Minute)}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "scheduler-fairness.db"), sqlite.Config{BusyTimeout: 5000})
	require.NoError(t, err)
	queue := New(driver, Options{Clock: clock, Writer: WriterOptions{BatchSize: 1, FlushInterval: time.Millisecond}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("scheduler-fairness")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "1m"})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})

	tx, err := testDB(driver).Beginx()
	require.NoError(t, err)
	for index := 0; index < schedulerPassLimit+1; index++ {
		_, err = tx.Exec(`
			INSERT INTO schedules (id, topic_id, name, cron_expression, message, next_run_at)
			VALUES (?, ?, ?, '* * * * *', 'tick', ?)`,
			uuid.NewString(), topic.ID, fmt.Sprintf("due-%03d", index), clock.Now())
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	queue.signalScheduler()

	require.Eventually(t, func() bool {
		var runs int
		return testDB(driver).Get(&runs, "SELECT COUNT(*) FROM schedule_runs") == nil && runs == schedulerPassLimit
	}, 5*time.Second, 10*time.Millisecond)
	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "writer-still-progresses"})
	require.NoError(t, err)
	clock.Advance(maintenanceYieldInterval)
	require.Eventually(t, func() bool {
		var runs int
		return testDB(driver).Get(&runs, "SELECT COUNT(*) FROM schedule_runs") == nil && runs == schedulerPassLimit+1
	}, 3*time.Second, 10*time.Millisecond)
}
