package blockqueue

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestGlobalReaperRequeuesWithoutAnotherClaim(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	receipt, err := queue.Publish(context.Background(), topic, Message{Message: "expire"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, 30*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	oldReceipt := claimed[0].ReceiptToken

	require.Eventually(t, func() bool {
		var state struct {
			Status  string  `db:"status"`
			Receipt *string `db:"receipt_token"`
		}
		err := testDB(driver).Get(&state,
			"SELECT status, receipt_token FROM message_deliveries WHERE message_id = ?", receipt.MessageID)
		return err == nil && state.Status == "pending" && state.Receipt == nil
	}, 2*time.Second, 20*time.Millisecond)

	redelivered, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, redelivered, 1)
	require.NotEqual(t, oldReceipt, redelivered[0].ReceiptToken)
}

func TestGlobalReaperMovesMaxAttemptToDLQ(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "reaper.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{})
	require.NoError(t, queue.Run(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	topic := Topic{ID: uuid.New(), Name: "reaper"}
	subscriber := Subscriber{
		ID: uuid.New(), TopicID: topic.ID, Name: "worker",
		Options: SubscriberOptions{MaxAttempts: 1, VisibilityDuration: "30ms"},
	}
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	receipt, err := queue.Publish(context.Background(), topic, Message{Message: "dead"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, 30*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	require.Eventually(t, func() bool {
		var count int
		err := testDB(driver).Get(&count, `
			SELECT COUNT(*) FROM message_deliveries
			WHERE message_id = ? AND status = 'dead_letter' AND processed_at IS NOT NULL`, receipt.MessageID)
		return err == nil && count == 1
	}, 2*time.Second, 20*time.Millisecond)
}

func TestClaimDoesNotSweepUnrelatedScheduleRuns(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx := context.Background()
	completed, err := queue.Publish(ctx, topic, Message{Message: "completed schedule candidate"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, queue.AckDelivery(ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken))

	schedule, err := queue.CreateSchedule(ctx, topic, ScheduleInput{
		Name: "claim-no-sweep", CronExpression: "0 0 * * *", Timezone: "UTC", Message: "scheduled",
	})
	require.NoError(t, err)
	now := time.Now().UTC()
	_, err = testDB(driver).Exec(`
		INSERT INTO schedule_runs (id, schedule_id, message_id, scheduled_for, status)
		VALUES (?, ?, ?, ?, 'running')`, uuid.NewString(), schedule.ID, completed.MessageID, now)
	require.NoError(t, err)
	_, err = testDB(driver).Exec(`
		CREATE TRIGGER reject_schedule_run_hot_path
		BEFORE UPDATE ON schedule_runs
		BEGIN
			SELECT RAISE(ABORT, 'schedule run sweep reached claim hot path');
		END`)
	require.NoError(t, err)

	_, err = queue.Publish(ctx, topic, Message{Message: "ordinary claim"})
	require.NoError(t, err)
	claimed, err = queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
}

func TestConcurrentClaimsHaveSingleOwner(t *testing.T) {
	queue, _, topic := setupQueue(t)
	const messages = 500
	batch := make([]Message, messages)
	for index := range batch {
		batch[index].Message = "concurrent"
	}
	_, err := queue.BatchPublish(context.Background(), topic, batch)
	require.NoError(t, err)

	const workers = 25
	start := make(chan struct{})
	results := make(chan Deliveries, workers)
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			claimed, err := queue.Claim(context.Background(), topic, "worker", messages/workers, time.Minute)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- claimed
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	owned := make(map[string]string, messages)
	for claimed := range results {
		for _, message := range claimed {
			if previous, duplicate := owned[message.ID]; duplicate {
				t.Fatalf("message %s claimed twice with receipts %s and %s", message.ID, previous, message.ReceiptToken)
			}
			owned[message.ID] = message.ReceiptToken
		}
	}
	require.Len(t, owned, messages)
}

func TestClaimAtAPILimitStaysBelowSQLiteBindLimit(t *testing.T) {
	queue, _, topic := setupQueue(t)
	batch := make([]Message, 1000)
	for index := range batch {
		batch[index].Message = "bind-limit"
	}
	_, err := queue.BatchPublish(context.Background(), topic, batch)
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1000, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1000)
	unique := make(map[string]struct{}, len(claimed))
	for _, delivery := range claimed {
		unique[delivery.ID] = struct{}{}
	}
	require.Len(t, unique, 1000)
}

func TestPreCanceledAdmissionReleasesWeightedBudget(t *testing.T) {
	driver, cleanup := setupTestDB(t, "canceled_budget")
	defer cleanup()
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(driver).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "budget")
	require.NoError(t, err)
	_, err = testDB(driver).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`)
	require.NoError(t, err)

	writer := newWriter(context.Background(), newDb(driver), WriterOptions{
		BatchSize: 1, MaxPendingMessages: 1, MaxPendingBytes: 1024,
	})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	now := time.Now().UTC()
	err = writer.EnqueueBatchContext(canceled, []writeRequest{{
		TopicID: topicID, MessageID: newMessageID(), Message: "canceled", VisibleAt: now, CreatedAt: now,
	}})
	require.ErrorIs(t, err, context.Canceled)
	messages, bytes := writer.Pending()
	require.Zero(t, messages)
	require.Zero(t, bytes)

	_, err = writer.EnqueueBatchDurable(context.Background(), []writeRequest{{
		TopicID: topicID, MessageID: newMessageID(), Message: "committed", VisibleAt: now, CreatedAt: now,
	}})
	require.NoError(t, err)
	messages, bytes = writer.Pending()
	require.Zero(t, messages)
	require.Zero(t, bytes)
	require.NoError(t, writer.CloseContext(context.Background()))
}

func TestShutdownDrainsEverySuccessfulConcurrentAdmission(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "shutdown.db"), sqlite.Config{})
	require.NoError(t, err)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 25, FlushInterval: 5 * time.Millisecond,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("shutdown")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "30s",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))

	var successful atomic.Int64
	for index := 0; index < 100; index++ {
		_, err := queue.PublishAsync(context.Background(), topic, Message{Message: "before-shutdown"})
		require.NoError(t, err)
		successful.Add(1)
	}
	start := make(chan struct{})
	var publishers sync.WaitGroup
	for index := 0; index < 100; index++ {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			<-start
			if _, publishErr := queue.PublishAsync(context.Background(), topic, Message{Message: "during-shutdown"}); publishErr == nil {
				successful.Add(1)
			}
		}()
	}
	close(start)
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, queue.shutdown(shutdownContext, false))
	publishers.Wait()

	var persisted int64
	require.NoError(t, testDB(driver).Get(&persisted, "SELECT COUNT(*) FROM messages"))
	require.Equal(t, successful.Load(), persisted)
	require.NoError(t, driver.Close())
}

func TestShutdownDeadlineAbortsWriterAndReachesStopped(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "shutdown-abort.db"), sqlite.Config{BusyTimeout: 1})
	require.NoError(t, err)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 1, FlushInterval: time.Millisecond, RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("shutdown-abort")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "1m"})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))

	lock, err := testDB(driver).Connx(context.Background())
	require.NoError(t, err)
	requireSQLiteWriteLock(t, lock)
	lockHeld := true
	defer func() {
		if lockHeld {
			_, _ = lock.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = lock.Close()
	}()

	publishResult := make(chan error, 1)
	go func() {
		_, publishErr := queue.PublishDurable(context.Background(), topic, Message{Message: "must-not-hang"})
		publishResult <- publishErr
	}()
	require.Eventually(t, func() bool {
		messages, _ := queue.writer.Pending()
		return messages == 1 && !queue.writer.Healthy()
	}, time.Second, 5*time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	shutdownErr := queue.Shutdown(shutdownCtx)
	require.ErrorIs(t, shutdownErr, ErrWriterDrainTimeout)
	require.Equal(t, LifecycleStopped, queue.State())
	require.Eventually(t, func() bool {
		select {
		case publishErr := <-publishResult:
			return errors.Is(publishErr, ErrWriterDrainTimeout)
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
	_, err = lock.ExecContext(context.Background(), "ROLLBACK")
	require.NoError(t, err)
	lockHeld = false
}

func TestScheduleUpdateRejectsStaleVersion(t *testing.T) {
	queue, _, topic := setupQueue(t)
	input := ScheduleInput{
		Name: "versioned", CronExpression: "*/5 * * * *", Timezone: "UTC", Message: "first",
	}
	schedule, err := queue.CreateSchedule(context.Background(), topic, input)
	require.NoError(t, err)
	input.Message = "second"
	updated, err := queue.UpdateSchedule(context.Background(), topic, schedule.ID, schedule.Version, input)
	require.NoError(t, err)
	require.Equal(t, schedule.Version+1, updated.Version)
	_, err = queue.UpdateSchedule(context.Background(), topic, schedule.ID, schedule.Version, input)
	require.ErrorIs(t, err, ErrScheduleVersion)
}

func TestScheduleLeaseTakeoverFencesStaleOwner(t *testing.T) {
	driver, cleanup := setupTestDB(t, "schedule_takeover")
	defer cleanup()
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(driver).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "takeover")
	require.NoError(t, err)
	_, err = testDB(driver).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	database := newDb(driver)
	schedule := Schedule{
		ID: "018f0000-0000-7000-8000-000000001001", TopicID: topicID.String(), Name: "takeover",
		CronExpression: "* * * * *", Timezone: "UTC", Message: "tick", Headers: "{}",
		MisfirePolicy: "fire_once", OverlapPolicy: "skip", NextRunAt: now.Add(-time.Minute),
	}
	require.NoError(t, database.createSchedule(context.Background(), schedule))

	first, claimed, err := database.claimDueSchedule(context.Background(), "owner-a", now, defaultScheduleLease)
	require.NoError(t, err)
	require.True(t, claimed)
	second, claimed, err := database.claimDueSchedule(context.Background(), "owner-b", now.Add(defaultScheduleLease+time.Second), defaultScheduleLease)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Greater(t, second.FencingToken, first.FencingToken)

	_, err = database.persistScheduleOccurrence(context.Background(), first, first.NextRunAt,
		now.Add(time.Minute), false, true, "owner-a")
	require.ErrorIs(t, err, ErrScheduleLeaseLost)
	_, err = database.persistScheduleOccurrence(context.Background(), second, second.NextRunAt,
		now.Add(time.Minute), false, true, "owner-b")
	require.NoError(t, err)
}

func TestSchedulerRestartFiresAtMostOneMissedOccurrence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scheduler-restart.db")
	clock := &testClock{now: time.Date(2026, time.July, 10, 10, 0, 0, 0, time.UTC)}
	driver, err := sqlite.Open(path, sqlite.Config{})
	require.NoError(t, err)
	first := New(driver, Options{Clock: clock})
	require.NoError(t, first.Run(context.Background()))
	topic := NewTopic("restart")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "30s",
	})
	require.NoError(t, first.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	schedule, err := first.CreateSchedule(context.Background(), topic, ScheduleInput{
		Name: "every-minute", CronExpression: "* * * * *", Timezone: "UTC", Message: "tick",
	})
	require.NoError(t, err)
	shutdownContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	require.NoError(t, first.Shutdown(shutdownContext))
	cancel()

	clock.Advance(5 * time.Minute)
	driver, err = sqlite.Open(path, sqlite.Config{})
	require.NoError(t, err)
	second := New(driver, Options{Clock: clock})
	require.NoError(t, second.Run(context.Background()))
	t.Cleanup(func() {
		ctx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_ = second.Shutdown(ctx)
	})
	require.Eventually(t, func() bool {
		var runs int
		return testDB(driver).Get(&runs,
			"SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?", schedule.ID) == nil && runs == 1
	}, time.Second, 10*time.Millisecond)
	var nextRun time.Time
	require.NoError(t, testDB(driver).Get(&nextRun,
		"SELECT next_run_at FROM schedules WHERE id = ?", schedule.ID))
	require.True(t, nextRun.After(clock.Now()))
	time.Sleep(30 * time.Millisecond)
	var runs int
	require.NoError(t, testDB(driver).Get(&runs,
		"SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = ?", schedule.ID))
	require.Equal(t, 1, runs)
}

func TestSubscriberMutationFailureLeavesRuntimeUnchanged(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	_, err := testDB(driver).Exec(`
		CREATE TRIGGER reject_subscriber_delete
		BEFORE UPDATE OF deleted_at ON topic_subscribers
		BEGIN SELECT RAISE(ABORT, 'injected mutation failure'); END`)
	require.NoError(t, err)
	err = queue.DeleteSubscriber(context.Background(), topic, "worker")
	require.Error(t, err)
	runtime, exists := queue.getTopicRuntime(topic)
	require.True(t, exists)
	_, exists = runtime.subscriberByName("worker")
	require.True(t, exists, "failed DB mutation must not change the registry")

	_, err = testDB(driver).Exec("DROP TRIGGER reject_subscriber_delete")
	require.NoError(t, err)
	_, err = queue.Publish(context.Background(), topic, Message{Message: "still-routable"})
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
}

func TestTopicMutationBarrierDoesNotBlockOtherTopicAdmission(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "topic-fence.db"), sqlite.Config{BusyTimeout: 1})
	require.NoError(t, err)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 100, FlushInterval: time.Millisecond,
		RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond,
	}})
	require.NoError(t, queue.Run(context.Background()))
	blockedTopic := NewTopic("blocked-topic")
	blockedSubscriber := NewSubscriber(blockedTopic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), blockedTopic, Subscribers{blockedSubscriber}))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	otherTopic := NewTopic("unrelated-topic")
	otherSubscriber := NewSubscriber(otherTopic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), otherTopic, Subscribers{otherSubscriber}))

	lock, err := testDB(driver).Connx(context.Background())
	require.NoError(t, err)
	requireSQLiteWriteLock(t, lock)
	lockHeld := true
	defer func() {
		if lockHeld {
			_, _ = lock.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = lock.Close()
	}()

	_, err = queue.PublishAsync(context.Background(), blockedTopic, Message{Message: "blocked before barrier"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !queue.writer.Healthy() }, time.Second, 5*time.Millisecond)

	mutationResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mutationResult <- queue.DeleteSubscriber(ctx, blockedTopic, "worker")
	}()
	require.Eventually(t, func() bool { return len(queue.writer.queue) > 0 }, time.Second, time.Millisecond,
		"the destructive mutation must install its writer barrier")

	started := time.Now()
	receipt, err := queue.PublishAsync(context.Background(), otherTopic, Message{Message: "not globally blocked"})
	require.NoError(t, err)
	require.Equal(t, "admitted", receipt.State)
	require.Less(t, time.Since(started), 250*time.Millisecond,
		"a mutation barrier for one topic must not block unrelated admission")

	_, err = lock.ExecContext(context.Background(), "ROLLBACK")
	require.NoError(t, err)
	lockHeld = false
	require.NoError(t, <-mutationResult)
	barrierCtx, cancelBarrier := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelBarrier()
	require.NoError(t, queue.writer.Barrier(barrierCtx))

	var persisted int
	require.NoError(t, testDB(driver).Get(&persisted,
		"SELECT COUNT(*) FROM messages WHERE id = ?", receipt.MessageID))
	require.Equal(t, 1, persisted)
}

func TestInvalidTopicRuntimeNeverCommitsMetadata(t *testing.T) {
	queue, driver, _ := setupQueue(t)
	topic := Topic{ID: uuid.New(), Name: "invalid-runtime"}
	subscriber := Subscriber{
		ID: uuid.New(), TopicID: topic.ID, Name: "worker",
		Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "not-a-duration"},
	}
	err := queue.CreateTopic(context.Background(), topic, Subscribers{subscriber})
	require.Error(t, err)
	var count int
	require.NoError(t, testDB(driver).Get(&count, "SELECT COUNT(*) FROM topics WHERE id = ?", topic.ID))
	require.Zero(t, count)
}

func TestResourceNamesRejectUnaddressablePathSegments(t *testing.T) {
	topic := Topic{ID: uuid.New(), Name: "orders/eu"}
	_, err := buildTopicRuntime(topic, nil)
	require.ErrorIs(t, err, ErrInvalidTopic)

	topic.Name = "orders"
	subscriber := Subscriber{
		ID: uuid.New(), TopicID: topic.ID, Name: "worker/priority",
		Options: SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "30s"},
	}
	_, err = buildTopicRuntime(topic, Subscribers{subscriber})
	require.ErrorIs(t, err, ErrInvalidSubscriber)
}

func TestRuntimeReloadReusesUnchangedDeliveryWakeChannels(t *testing.T) {
	queue, _, topic := setupQueue(t)
	beforeTopic, exists := queue.getTopicRuntime(topic)
	require.True(t, exists)
	beforeSubscriber, exists := beforeTopic.subscriberByName("worker")
	require.True(t, exists)
	wake := beforeSubscriber.deliveryWake

	require.NoError(t, queue.reloadRuntime(context.Background()))
	afterTopic, exists := queue.getTopicRuntime(topic)
	require.True(t, exists)
	afterSubscriber, exists := afterTopic.subscriberByName("worker")
	require.True(t, exists)
	require.Same(t, beforeTopic, afterTopic)
	require.Same(t, beforeSubscriber, afterSubscriber)
	require.Equal(t, wake, afterSubscriber.deliveryWake)
}

func TestDurableWaitCancellationReturnsStableCommitUnknownIDs(t *testing.T) {
	driver, cleanup := setupTestDB(t, "commit_unknown")
	defer cleanup()
	topicID := uuid.New()
	subscriberID := uuid.New()
	_, err := testDB(driver).Exec("INSERT INTO topics (id, name) VALUES (?, ?)", topicID, "commit-unknown")
	require.NoError(t, err)
	_, err = testDB(driver).Exec(
		"INSERT INTO topic_subscribers (id, topic_id, name, option) VALUES (?, ?, ?, ?)",
		subscriberID, topicID, "worker", `{"max_attempts":3,"visibility_duration":"30s"}`)
	require.NoError(t, err)

	lock, err := testDB(driver).Beginx()
	require.NoError(t, err)
	_, err = lock.Exec("UPDATE topics SET name = name WHERE id = ?", topicID)
	require.NoError(t, err)
	buffer := newWriter(context.Background(), newDb(driver), WriterOptions{
		BatchSize: 1, RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond,
	})
	messageID := newMessageID()
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = buffer.EnqueueBatchDurable(ctx, []writeRequest{{
		TopicID: topicID, MessageID: messageID, Message: "ambiguous", Headers: []byte("{}"),
		VisibleAt: now, CreatedAt: now,
	}})
	var unknown *CommitUnknownError
	require.True(t, errors.As(err, &unknown), err)
	require.Equal(t, []string{messageID}, unknown.MessageIDs)
	require.NoError(t, lock.Rollback())

	barrierCtx, barrierCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer barrierCancel()
	require.NoError(t, buffer.Barrier(barrierCtx))
	require.NoError(t, buffer.CloseContext(barrierCtx))
	var count int
	require.NoError(t, testDB(driver).Get(&count, "SELECT COUNT(*) FROM messages WHERE id = ?", messageID))
	require.Equal(t, 1, count)
}
