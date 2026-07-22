package blockqueue

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func setupQueue(t *testing.T) (*Queue, *sqlite.Driver, Topic) {
	t.Helper()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "queue.db"), sqlite.Config{BusyTimeout: 5000})
	require.NoError(t, err)
	queue := New(driver, Options{
		Writer: WriterOptions{BatchSize: 10, FlushInterval: 5 * time.Millisecond},
	})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("queue-topic")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts:        5,
		VisibilityDuration: "100ms",
		RetryPolicy:        RetryPolicy{InitialDelay: "0s", MaxDelay: "0s"},
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	return queue, driver, topic
}

func TestDurableIdempotency(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	request := Message{Message: "same", IdempotencyKey: "order-42", Headers: map[string]string{"kind": "order"}}
	first, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	require.Equal(t, "persisted", first.State)
	require.NotNil(t, first.Duplicate)
	require.False(t, *first.Duplicate)

	second, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	require.Equal(t, first.MessageID, second.MessageID)
	require.True(t, *second.Duplicate)

	var messages, deliveries int
	require.NoError(t, testDB(driver).Get(&messages, "SELECT COUNT(*) FROM messages"))
	require.NoError(t, testDB(driver).Get(&deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, deliveries)

	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "different", IdempotencyKey: "order-42"})
	require.ErrorIs(t, err, ErrIdempotencyConflict)
}

func TestDurableBatchIdempotencyConflictRollsBackEveryItem(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	_, err := queue.PublishDurable(context.Background(), topic, Message{
		Message:        "original",
		IdempotencyKey: "existing-key",
	})
	require.NoError(t, err)

	_, err = queue.BatchPublishDurable(context.Background(), topic, []Message{
		{Message: "conflict", IdempotencyKey: "existing-key"},
		{Message: "must-rollback", IdempotencyKey: "new-key"},
	})
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	var count int
	require.NoError(t, testDB(driver).Get(&count,
		"SELECT COUNT(*) FROM messages WHERE idempotency_key = ?", "new-key"))
	require.Zero(t, count, "a permanent conflict must roll back the complete admission")
}

func TestIdempotencyComparesCorrelationAndExplicitSchedule(t *testing.T) {
	queue, _, topic := setupQueue(t)
	scheduledAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	request := Message{
		Message:        "scheduled",
		IdempotencyKey: "scheduled-key",
		CorrelationID:  "correlation-a",
		ScheduleAt:     scheduledAt.Format(time.RFC3339),
	}
	first, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	duplicate, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	require.Equal(t, first.MessageID, duplicate.MessageID)
	require.True(t, *duplicate.Duplicate)

	request.CorrelationID = "correlation-b"
	_, err = queue.PublishDurable(context.Background(), topic, request)
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	request.CorrelationID = "correlation-a"
	request.ScheduleAt = scheduledAt.Add(time.Minute).Format(time.RFC3339)
	_, err = queue.PublishDurable(context.Background(), topic, request)
	require.ErrorIs(t, err, ErrIdempotencyConflict)
}

func TestIdempotencyRetryPreservesRelativeDelay(t *testing.T) {
	queue, _, topic := setupQueue(t)
	request := Message{Message: "delayed", IdempotencyKey: "delay-key", Delay: "5m"}
	first, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)
	request.Delay = "300s"
	duplicate, err := queue.PublishDurable(context.Background(), topic, request)
	require.NoError(t, err)
	require.Equal(t, first.MessageID, duplicate.MessageID)
	require.True(t, *duplicate.Duplicate)
	require.Equal(t, first.ScheduledAt, duplicate.ScheduledAt,
		"an idempotent retry must report the original persisted schedule")

	request.Delay = "10m"
	_, err = queue.PublishDurable(context.Background(), topic, request)
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	receipts, err := queue.BatchPublishDurable(context.Background(), topic, []Message{
		{Message: "same batch", IdempotencyKey: "normalized-batch-delay", Delay: "1m"},
		{Message: "same batch", IdempotencyKey: "normalized-batch-delay", Delay: "60s"},
	})
	require.NoError(t, err)
	require.Len(t, receipts, 2)
	require.Equal(t, receipts[0].MessageID, receipts[1].MessageID)
	require.False(t, *receipts[0].Duplicate)
	require.True(t, *receipts[1].Duplicate)
	require.Equal(t, receipts[0].ScheduledAt, receipts[1].ScheduledAt)
}

func TestClaimWaitTreatsPollDeadlineAsEmptyResult(t *testing.T) {
	queue, _, topic := setupQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deliveries, err := queue.ClaimWait(ctx, topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Empty(t, deliveries)
}

func TestPermanentAdmissionDoesNotPoisonValidNeighbour(t *testing.T) {
	logs := make(chan slog.Record, 8)
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(testLogHandler{
		message: "writer permanently rejected admission",
		records: logs,
	}))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	driver, cleanup := setupTestDB(t, "admission_isolation_"+time.Now().Format("150405.000000000"))
	defer cleanup()
	database := newDb(driver)
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(driver).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "isolation")
	require.NoError(t, err)
	_, err = testDB(driver).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`,
	)
	require.NoError(t, err)

	buffer := newWriter(context.Background(), database, WriterOptions{
		BatchSize:     2,
		FlushInterval: time.Hour,
		RetryMin:      time.Millisecond,
		RetryMax:      5 * time.Millisecond,
	})
	t.Cleanup(func() { buffer.Close() })
	now := time.Now().UTC()
	require.NoError(t, buffer.EnqueueBatchContext(context.Background(), []writeRequest{{
		TopicID:   topicID,
		MessageID: newMessageID(),
		Message:   "invalid",
		Headers:   []byte("{"),
		VisibleAt: now,
		CreatedAt: now,
	}}))
	duplicates, err := buffer.EnqueueBatchDurable(context.Background(), []writeRequest{{
		TopicID:   topicID,
		MessageID: newMessageID(),
		Message:   "valid",
		Headers:   []byte("{}"),
		VisibleAt: now,
		CreatedAt: now,
	}})
	require.NoError(t, err)
	require.Equal(t, []bool{false}, duplicates)

	var count int
	require.NoError(t, testDB(driver).Get(&count, "SELECT COUNT(*) FROM messages WHERE message = 'valid'"))
	require.Equal(t, 1, count)
	require.Eventually(t, func() bool {
		select {
		case record := <-logs:
			return record.Message == "writer permanently rejected admission"
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	messages, bytes := buffer.Pending()
	require.Zero(t, messages)
	require.Zero(t, bytes)
}

type testLogHandler struct {
	message string
	records chan<- slog.Record
}

func (handler testLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler testLogHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message != handler.message {
		return nil
	}
	select {
	case handler.records <- record.Clone():
	default:
	}
	return nil
}
func (handler testLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler testLogHandler) WithGroup(string) slog.Handler      { return handler }

func TestDeleteSubscriberWakesLongPollWithoutDeadlock(t *testing.T) {
	queue, _, topic := setupQueue(t)
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_, err := queue.ClaimWait(ctx, topic, "worker", 1, time.Second)
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, queue.DeleteSubscriber(context.Background(), topic, "worker"))
	select {
	case err := <-result:
		require.True(t, errors.Is(err, ErrSubscriberNotFound) || errors.Is(err, ErrSubscriberDeleted), err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber deletion did not wake the long poll")
	}
}

func TestReceiptProtectsRedelivery(t *testing.T) {
	queue, _, topic := setupQueue(t)
	_, err := queue.PublishDurable(context.Background(), topic, Message{Message: "lease"})
	require.NoError(t, err)

	first, err := queue.Claim(context.Background(), topic, "worker", 1, 40*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 1)
	time.Sleep(60 * time.Millisecond)
	second, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.NotEqual(t, first[0].ReceiptToken, second[0].ReceiptToken)

	err = queue.AckDelivery(context.Background(), topic, "worker", first[0].ID, first[0].ReceiptToken)
	require.ErrorIs(t, err, ErrLeaseLost)
	require.NoError(t, queue.AckDelivery(context.Background(), topic, "worker", second[0].ID, second[0].ReceiptToken))
	// ACK retry with the same receipt is idempotent.
	require.NoError(t, queue.AckDelivery(context.Background(), topic, "worker", second[0].ID, second[0].ReceiptToken))
}

func TestNackDelayAndLeaseExtension(t *testing.T) {
	queue, _, topic := setupQueue(t)
	_, err := queue.PublishDurable(context.Background(), topic, Message{Message: "retry"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NotNil(t, claimed[0].LeaseExpiresAt)
	originalExpiry := *claimed[0].LeaseExpiresAt

	expires, err := queue.ExtendLease(context.Background(), topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, time.Second)
	require.NoError(t, err)
	require.WithinDuration(t, originalExpiry.Add(time.Second), expires, time.Millisecond)
	require.NoError(t, queue.NackDelivery(context.Background(), topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, 80*time.Millisecond, "temporary"))

	immediate, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Empty(t, immediate)
	time.Sleep(100 * time.Millisecond)
	retried, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, retried, 1)
}

func TestBatchAckAndNackReturnOneResultPerItem(t *testing.T) {
	queue, _, topic := setupQueue(t)
	_, err := queue.BatchPublish(context.Background(), topic, []Message{
		{Message: "ack-valid"}, {Message: "ack-stale"},
	})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	ackResults := queue.BatchAckDeliveries(context.Background(), topic, "worker", []BatchAckItem{
		{MessageID: claimed[0].ID, ReceiptToken: claimed[0].ReceiptToken},
		{MessageID: claimed[1].ID, ReceiptToken: "stale-receipt"},
	})
	require.Equal(t, []string{"processed", "failed"}, []string{ackResults[0].Status, ackResults[1].Status})
	require.Equal(t, ErrLeaseLost.Error(), ackResults[1].Error)
	require.NoError(t, queue.AckDelivery(context.Background(), topic, "worker", claimed[1].ID, claimed[1].ReceiptToken))

	_, err = queue.BatchPublish(context.Background(), topic, []Message{
		{Message: "nack-valid"}, {Message: "nack-stale"},
	})
	require.NoError(t, err)
	claimed, err = queue.Claim(context.Background(), topic, "worker", 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	nackResults := queue.BatchNackDeliveries(context.Background(), topic, "worker", []BatchNackItem{
		{MessageID: claimed[0].ID, ReceiptToken: claimed[0].ReceiptToken, RetryDelay: time.Second},
		{MessageID: claimed[1].ID, ReceiptToken: "stale-receipt"},
	})
	require.Equal(t, []string{"pending", "failed"}, []string{nackResults[0].Status, nackResults[1].Status})
	require.Equal(t, ErrLeaseLost.Error(), nackResults[1].Error)
}

func TestPriorityAndAbsoluteSchedule(t *testing.T) {
	queue, _, topic := setupQueue(t)
	future := time.Now().UTC().Add(120 * time.Millisecond)
	_, err := queue.BatchPublishDurable(context.Background(), topic, []Message{
		{Message: "low", Priority: -10},
		{Message: "high", Priority: 20},
		{Message: "future", Priority: 100, ScheduleAt: future.Format(time.RFC3339Nano)},
	})
	require.NoError(t, err)
	first, err := queue.Claim(context.Background(), topic, "worker", 2, time.Second)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "high", first[0].Message)
	require.Equal(t, "low", first[1].Message)
	time.Sleep(time.Until(future) + 30*time.Millisecond)
	later, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, later, 1)
	require.Equal(t, "future", later[0].Message)
}

func TestClaimWaitUsesStoredVisibilityDeadline(t *testing.T) {
	queue, _, topic := setupQueue(t)
	visibleAt := time.Now().UTC().Add(80 * time.Millisecond)
	_, err := queue.PublishDurable(context.Background(), topic, Message{Message: "wake", ScheduleAt: visibleAt.Format(time.RFC3339Nano)})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	messages, err := queue.ClaimWait(ctx, topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.GreaterOrEqual(t, time.Since(started), 50*time.Millisecond)
	require.Less(t, time.Since(started), 300*time.Millisecond)
}

func TestSchedulerRunNowAndOverlap(t *testing.T) {
	queue, _, topic := setupQueue(t)
	schedule, err := queue.CreateSchedule(context.Background(), topic, ScheduleInput{
		Name:           "heartbeat",
		CronExpression: "* * * * *",
		Timezone:       "UTC",
		Message:        "scheduled",
	})
	require.NoError(t, err)
	run, err := queue.RunScheduleNow(context.Background(), topic, schedule.ID, false)
	require.NoError(t, err)
	require.True(t, run.MessageID.Valid)
	second, err := queue.RunScheduleNow(context.Background(), topic, schedule.ID, false)
	require.NoError(t, err)
	require.Equal(t, "skipped", second.Status)

	messages, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.NoError(t, queue.AckDelivery(context.Background(), topic, "worker", messages[0].ID, messages[0].ReceiptToken))
	history, err := queue.ScheduleRunHistory(context.Background(), topic, schedule.ID, 10, "")
	require.NoError(t, err)
	require.NotEmpty(t, history.Runs)
}

func TestSchedulerOverlapTreatsCancelledDeliveryAsTerminal(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	schedule, err := queue.CreateSchedule(context.Background(), topic, ScheduleInput{
		Name:           "cancel-recovery",
		CronExpression: "* * * * *",
		Timezone:       "UTC",
		Message:        "scheduled",
	})
	require.NoError(t, err)
	first, err := queue.RunScheduleNow(context.Background(), topic, schedule.ID, false)
	require.NoError(t, err)
	require.Equal(t, ScheduleRunStatusRunning, first.Status)

	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, queue.CancelClaimedDelivery(
		context.Background(), topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, "permanent",
	))

	// Simulate recovery observing a stale running marker after every delivery
	// already reached a terminal state. Overlap protection is authoritative on
	// delivery state and must not block the next occurrence.
	_, err = testDB(driver).Exec(
		"UPDATE schedule_runs SET status = 'running', finished_at = NULL WHERE id = ?", first.ID,
	)
	require.NoError(t, err)
	second, err := queue.RunScheduleNow(context.Background(), topic, schedule.ID, false)
	require.NoError(t, err)
	require.Equal(t, ScheduleRunStatusRunning, second.Status)
}

func TestLifecycleAndWeightedBudget(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "lifecycle.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{})
	_, err = queue.Publish(context.Background(), Topic{Name: "missing"}, Message{Message: "x"})
	require.ErrorIs(t, err, ErrQueueNotRunning)
	require.NoError(t, driver.Close())

	driver, err = sqlite.Open(filepath.Join(t.TempDir(), "budget.db"), sqlite.Config{})
	require.NoError(t, err)
	require.NoError(t, Migrate(context.Background(), driver))
	buffer := newWriter(context.Background(), newDb(driver), WriterOptions{MaxPendingMessages: 1, MaxPendingBytes: 16, FlushInterval: time.Hour})
	err = buffer.EnqueueBatchContext(context.Background(), []writeRequest{{Message: "this is larger than the cap"}})
	require.ErrorIs(t, err, ErrPendingBudgetExceeded)
	buffer.Close()
	require.NoError(t, driver.Close())
}

func TestEmbeddedMigrationRerun(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "migration.db"), sqlite.Config{})
	require.NoError(t, err)
	defer func() { require.NoError(t, driver.Close()) }()
	require.NoError(t, Migrate(context.Background(), driver))
	require.NoError(t, Migrate(context.Background(), driver))
	var applied int
	require.NoError(t, testDB(driver).Get(&applied, "SELECT COUNT(*) FROM schema_migrations"))
	require.Greater(t, applied, 0)
}

func TestWriterAbortsFailedFlushAfterShutdownDeadline(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "failure.db"), sqlite.Config{BusyTimeout: 1})
	require.NoError(t, err)
	require.NoError(t, Migrate(context.Background(), driver))
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err = testDB(driver).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "failure")
	require.NoError(t, err)
	_, err = testDB(driver).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`)
	require.NoError(t, err)

	lock, err := testDB(driver).Connx(context.Background())
	require.NoError(t, err)
	requireSQLiteWriteLock(t, lock)
	defer func() { _ = lock.Close() }()

	buffer := newWriter(context.Background(), newDb(driver), WriterOptions{BatchSize: 1, RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond})
	now := time.Now().UTC()
	require.NoError(t, buffer.EnqueueBatchContext(context.Background(), []writeRequest{{
		TopicID:   topicID,
		MessageID: newMessageID(),
		Message:   "retained",
		VisibleAt: now,
		CreatedAt: now,
	}}))
	require.Eventually(t, func() bool { return !buffer.Healthy() }, time.Second, 5*time.Millisecond)
	messages, _ := buffer.Pending()
	require.EqualValues(t, 1, messages)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = buffer.CloseContext(ctx)
	require.True(t, errors.Is(err, ErrWriterDrainTimeout))
	require.Eventually(t, func() bool {
		select {
		case <-buffer.done:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
	messages, _ = buffer.Pending()
	require.Zero(t, messages)
	_, err = lock.ExecContext(context.Background(), "ROLLBACK")
	require.NoError(t, err)
	require.NoError(t, driver.Close())
}

type testClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []testClockWaiter
}

type testClockWaiter struct {
	at time.Time
	ch chan time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(delay time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	at := c.now.Add(delay)
	if delay <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, testClockWaiter{at: at, ch: ch})
	return ch
}

func (c *testClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	remaining := c.waiters[:0]
	for _, waiter := range c.waiters {
		if !waiter.at.After(c.now) {
			waiter.ch <- c.now
		} else {
			remaining = append(remaining, waiter)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()
}

func TestSchedulerWakesAtExactFakeClockDeadline(t *testing.T) {
	clock := &testClock{now: time.Now().UTC().Truncate(time.Minute)}
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "scheduler-clock.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{
		Clock:  clock,
		Writer: WriterOptions{BatchSize: 10, FlushInterval: time.Millisecond},
	})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("clock-topic")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "1s"})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	_, err = queue.CreateSchedule(context.Background(), topic, ScheduleInput{Name: "each-minute", CronExpression: "* * * * *", Timezone: "UTC", Message: "tick"})
	require.NoError(t, err)

	clock.Advance(59 * time.Second)
	time.Sleep(10 * time.Millisecond)
	var runs int
	require.NoError(t, testDB(driver).Get(&runs, "SELECT COUNT(*) FROM schedule_runs"))
	require.Zero(t, runs)
	clock.Advance(time.Second)
	require.Eventually(t, func() bool {
		return testDB(driver).Get(&runs, "SELECT COUNT(*) FROM schedule_runs") == nil && runs == 1
	}, time.Second, 5*time.Millisecond)
}

func TestCronTimezoneDSTTransitions(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	spring, err := parseCron("30 2 * * *", "America/New_York")
	require.NoError(t, err)
	next := spring.Next(time.Date(2026, time.March, 7, 3, 0, 0, 0, location))
	localNext := next.In(location)
	require.Equal(t, 9, localNext.Day())
	require.Equal(t, 2, localNext.Hour())
	require.Equal(t, 30, localNext.Minute())

	fall, err := parseCron("30 1 * * *", "America/New_York")
	require.NoError(t, err)
	first := fall.Next(time.Date(2026, time.November, 1, 0, 0, 0, 0, location))
	second := fall.Next(first)
	require.Equal(t, 1, first.In(location).Hour())
	require.Equal(t, 1, second.In(location).Hour())
	require.NotEqual(t, first, second)
}
