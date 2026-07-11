package blockqueue

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type contractBackend struct {
	name    string
	open    func(*testing.T) store.Driver
	enabled bool
}

func TestStorageContract(t *testing.T) {
	backends := []contractBackend{{
		name:    "sqlite",
		enabled: true,
		open: func(t *testing.T) store.Driver {
			driver, err := sqlite.Open(filepath.Join(t.TempDir(), "contract.db"), sqlite.Config{BusyTimeout: 5000})
			require.NoError(t, err)
			return driver
		},
	}}
	postgresURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
	backends = append(backends, contractBackend{
		name:    "postgres",
		enabled: postgresURL != "",
		open: func(t *testing.T) store.Driver {
			schema, err := testdb.OpenPostgreSQLSchema(
				context.Background(), postgresURL, "_test", "blockqueue_contract",
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, schema.Close()) })
			return schema.Driver
		},
	})

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			if !backend.enabled {
				t.Skip("set BLOCKQUEUE_TEST_POSTGRES_URL to run the PostgreSQL contract")
			}
			runStorageContract(t, backend.open(t))
		})
	}
}

func runStorageContract(t *testing.T, driver store.Driver) {
	queue := New(driver, Options{
		Writer: WriterOptions{BatchSize: 20, FlushInterval: time.Millisecond},
	})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("contract-topic")
	subscribers := Subscribers{
		NewSubscriber(topic, "one", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "100ms"}),
		NewSubscriber(topic, "two", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "100ms"}),
	}
	require.NoError(t, queue.CreateTopic(context.Background(), topic, subscribers))

	first, err := queue.PublishDurable(context.Background(), topic, Message{Message: "idempotent", IdempotencyKey: "contract-key", Priority: 10})
	require.NoError(t, err)
	duplicate, err := queue.PublishDurable(context.Background(), topic, Message{Message: "idempotent", IdempotencyKey: "contract-key", Priority: 10})
	require.NoError(t, err)
	require.Equal(t, first.MessageID, duplicate.MessageID)
	require.True(t, *duplicate.Duplicate)

	for _, subscriber := range []string{"one", "two"} {
		claimed, err := queue.Claim(context.Background(), topic, subscriber, 1, time.Second)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, first.MessageID, claimed[0].ID)
		require.NoError(t, queue.AckDelivery(context.Background(), topic, subscriber, claimed[0].ID, claimed[0].ReceiptToken))
	}

	require.NoError(t, queue.PauseSubscriber(context.Background(), topic, "one"))
	_, err = queue.PublishDurable(context.Background(), topic, Message{Message: "paused"})
	require.NoError(t, err)
	_, err = queue.Claim(context.Background(), topic, "one", 1, time.Second)
	require.ErrorIs(t, err, ErrResourcePaused)
	require.NoError(t, queue.ResumeSubscriber(context.Background(), topic, "one"))
	claimed, err := queue.Claim(context.Background(), topic, "one", 1, time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, queue.AckDelivery(context.Background(), topic, "one", claimed[0].ID, claimed[0].ReceiptToken))

	// Lease expiry must advance without another consumer claim. This exercises
	// the global reaper and its terminal timestamp update on every backend.
	reaperTopic := NewTopic("contract-global-reaper")
	reaperSubscriber := NewSubscriber(reaperTopic, "worker", SubscriberOptions{
		MaxAttempts: 1, VisibilityDuration: "25ms", DequeueBatchSize: 1,
	})
	require.NoError(t, queue.CreateTopic(context.Background(), reaperTopic, Subscribers{reaperSubscriber}))
	reaperReceipt, err := queue.PublishDurable(context.Background(), reaperTopic, Message{Message: "expire-to-dlq"})
	require.NoError(t, err)
	reaperClaim, err := queue.Claim(context.Background(), reaperTopic, reaperSubscriber.Name, 1, 25*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, reaperClaim, 1)
	require.Equal(t, reaperReceipt.MessageID, reaperClaim[0].ID)
	require.Eventually(t, func() bool {
		page, listErr := queue.ListDeliveries(context.Background(), reaperTopic, reaperSubscriber.Name, true, 10, "")
		return listErr == nil && len(page.Messages) == 1 && page.Messages[0].ID == reaperReceipt.MessageID
	}, 3*time.Second, 20*time.Millisecond, "expired delivery must reach the DLQ without another claim")

	priorityTopic, prioritySubscriber := createContractTopic(t, queue, "contract-priority", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 10,
	})
	low, err := queue.PublishDurable(context.Background(), priorityTopic, Message{Message: "low", Priority: -10})
	require.NoError(t, err)
	high, err := queue.PublishDurable(context.Background(), priorityTopic, Message{Message: "high", Priority: 10})
	require.NoError(t, err)
	ordered, err := queue.Claim(context.Background(), priorityTopic, prioritySubscriber.Name, 2, time.Second)
	require.NoError(t, err)
	require.Len(t, ordered, 2)
	require.Equal(t, []string{high.MessageID, low.MessageID}, []string{ordered[0].ID, ordered[1].ID})
	for _, delivery := range ordered {
		require.NoError(t, queue.AckDelivery(context.Background(), priorityTopic, prioritySubscriber.Name, delivery.ID, delivery.ReceiptToken))
	}

	delayed, err := queue.PublishDurable(context.Background(), priorityTopic, Message{Message: "delayed", Delay: "75ms"})
	require.NoError(t, err)
	notYetVisible, err := queue.Claim(context.Background(), priorityTopic, prioritySubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Empty(t, notYetVisible)
	delayedContext, delayedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	delayedClaim, err := queue.ClaimWait(delayedContext, priorityTopic, prioritySubscriber.Name, 1, time.Second)
	delayedCancel()
	require.NoError(t, err)
	require.Len(t, delayedClaim, 1)
	require.Equal(t, delayed.MessageID, delayedClaim[0].ID)
	require.NoError(t, queue.AckDelivery(context.Background(), priorityTopic, prioritySubscriber.Name,
		delayedClaim[0].ID, delayedClaim[0].ReceiptToken))

	require.NoError(t, queue.PauseTopic(context.Background(), priorityTopic))
	pausedTopicMessage, err := queue.PublishDurable(context.Background(), priorityTopic, Message{Message: "topic-paused"})
	require.NoError(t, err)
	_, err = queue.Claim(context.Background(), priorityTopic, prioritySubscriber.Name, 1, time.Second)
	require.ErrorIs(t, err, ErrResourcePaused)
	require.NoError(t, queue.ResumeTopic(context.Background(), priorityTopic))
	resumedClaim, err := queue.Claim(context.Background(), priorityTopic, prioritySubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, resumedClaim, 1)
	require.Equal(t, pausedTopicMessage.MessageID, resumedClaim[0].ID)
	require.NoError(t, queue.AckDelivery(context.Background(), priorityTopic, prioritySubscriber.Name,
		resumedClaim[0].ID, resumedClaim[0].ReceiptToken))

	retryTopic, retrySubscriber := createContractTopic(t, queue, "contract-retry", SubscriberOptions{
		MaxAttempts: 2, VisibilityDuration: "250ms", DequeueBatchSize: 1,
	})
	retryReceipt, err := queue.PublishDurable(context.Background(), retryTopic, Message{Message: "retry-to-dlq"})
	require.NoError(t, err)
	firstLease, err := queue.Claim(context.Background(), retryTopic, retrySubscriber.Name, 1, 250*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, firstLease, 1)
	extendedUntil, err := queue.ExtendLease(context.Background(), retryTopic, retrySubscriber.Name,
		firstLease[0].ID, firstLease[0].ReceiptToken, 250*time.Millisecond)
	require.NoError(t, err)
	require.True(t, extendedUntil.After(*firstLease[0].LeaseExpiresAt))
	require.NoError(t, queue.NackDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		firstLease[0].ID, firstLease[0].ReceiptToken, 50*time.Millisecond, "contract retry"))
	require.ErrorIs(t, queue.AckDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		firstLease[0].ID, firstLease[0].ReceiptToken), ErrLeaseLost)

	retryContext, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
	secondLease, err := queue.ClaimWait(retryContext, retryTopic, retrySubscriber.Name, 1, time.Second)
	retryCancel()
	require.NoError(t, err)
	require.Len(t, secondLease, 1)
	require.NotEqual(t, firstLease[0].ReceiptToken, secondLease[0].ReceiptToken)
	require.NoError(t, queue.NackDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		secondLease[0].ID, secondLease[0].ReceiptToken, 0, "terminal"))
	dlqPage, err := queue.ListDeliveries(context.Background(), retryTopic, retrySubscriber.Name, true, 10, "")
	require.NoError(t, err)
	require.Len(t, dlqPage.Messages, 1)
	require.Equal(t, retryReceipt.MessageID, dlqPage.Messages[0].ID)
	replay := queue.ReplayDeadLetters(context.Background(), retryTopic, retrySubscriber.Name, []string{retryReceipt.MessageID})
	require.Len(t, replay, 1)
	require.Equal(t, "pending", replay[0].Status)
	replayedClaim, err := queue.Claim(context.Background(), retryTopic, retrySubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, replayedClaim, 1)
	require.NoError(t, queue.AckDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		replayedClaim[0].ID, replayedClaim[0].ReceiptToken))

	concurrentTopic, concurrentSubscriber := createContractTopic(t, queue, "contract-concurrent-claim", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	concurrentReceipt, err := queue.PublishDurable(context.Background(), concurrentTopic, Message{Message: "single-owner"})
	require.NoError(t, err)
	startClaims := make(chan struct{})
	claimResults := make(chan Deliveries, 8)
	claimErrors := make(chan error, 8)
	var claimWorkers sync.WaitGroup
	for index := 0; index < 8; index++ {
		claimWorkers.Add(1)
		go func() {
			defer claimWorkers.Done()
			<-startClaims
			items, claimErr := queue.Claim(context.Background(), concurrentTopic, concurrentSubscriber.Name, 1, time.Second)
			claimResults <- items
			claimErrors <- claimErr
		}()
	}
	close(startClaims)
	claimWorkers.Wait()
	close(claimResults)
	close(claimErrors)
	for claimErr := range claimErrors {
		require.NoError(t, claimErr)
	}
	var owner Delivery
	claimedCount := 0
	for items := range claimResults {
		claimedCount += len(items)
		if len(items) == 1 {
			owner = items[0]
		}
	}
	require.Equal(t, 1, claimedCount)
	require.Equal(t, concurrentReceipt.MessageID, owner.ID)
	require.NoError(t, queue.AckDelivery(context.Background(), concurrentTopic, concurrentSubscriber.Name,
		owner.ID, owner.ReceiptToken))

	batchTopic, batchSubscriber := createContractTopic(t, queue, "contract-batch-result", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	_, err = queue.PublishDurable(context.Background(), batchTopic, Message{Message: "batch-result"})
	require.NoError(t, err)
	batchClaim, err := queue.Claim(context.Background(), batchTopic, batchSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, batchClaim, 1)
	batchResults := queue.BatchAckDeliveries(context.Background(), batchTopic, batchSubscriber.Name, []BatchAckItem{
		{MessageID: batchClaim[0].ID, ReceiptToken: batchClaim[0].ReceiptToken},
		{MessageID: newMessageID(), ReceiptToken: "missing-receipt"},
	})
	require.Len(t, batchResults, 2)
	require.Equal(t, "processed", batchResults[0].Status)
	require.Equal(t, "failed", batchResults[1].Status)

	scheduleTopic, scheduleSubscriber := createContractTopic(t, queue, "contract-schedule", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	schedule, err := queue.CreateSchedule(context.Background(), scheduleTopic, ScheduleInput{
		Name: "daily", CronExpression: "0 0 * * *", Timezone: "UTC", Message: "scheduled-contract",
		Headers: map[string]string{"source": "contract"}, Priority: 7,
	})
	require.NoError(t, err)
	run, err := queue.RunScheduleNow(context.Background(), scheduleTopic, schedule.ID, false)
	require.NoError(t, err)
	require.Equal(t, "running", run.Status)
	scheduledClaim, err := queue.Claim(context.Background(), scheduleTopic, scheduleSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, scheduledClaim, 1)
	require.Equal(t, "scheduled-contract", scheduledClaim[0].Message)
	require.Equal(t, 7, scheduledClaim[0].Priority)
	require.NoError(t, queue.AckDelivery(context.Background(), scheduleTopic, scheduleSubscriber.Name,
		scheduledClaim[0].ID, scheduledClaim[0].ReceiptToken))
	require.Eventually(t, func() bool {
		history, historyErr := queue.ScheduleRunHistory(context.Background(), scheduleTopic, schedule.ID, 10, "")
		return historyErr == nil && len(history.Runs) == 1 && history.Runs[0].Status == "completed"
	}, time.Second, 20*time.Millisecond)
	retentionCutoff := time.Now().UTC().Add(-31 * 24 * time.Hour)
	_, err = queue.db.Conn().ExecContext(context.Background(), queue.db.Conn().Rebind(
		"UPDATE schedule_runs SET created_at = ?, finished_at = ? WHERE id = ?"),
		retentionCutoff, retentionCutoff, run.ID)
	require.NoError(t, err)
	runningRunID := uuid.NewString()
	_, err = queue.db.Conn().ExecContext(context.Background(), queue.db.Conn().Rebind(`
		INSERT INTO schedule_runs (id, schedule_id, scheduled_for, status, created_at)
		VALUES (?, ?, ?, 'running', ?)`),
		runningRunID, schedule.ID, retentionCutoff.Add(-time.Hour), retentionCutoff)
	require.NoError(t, err)
	require.NoError(t, queue.db.pruneScheduleRuns(context.Background(), 30*24*time.Hour))
	var completedRunRows, runningRunRows int
	require.NoError(t, queue.db.Conn().Get(&completedRunRows, queue.db.Conn().Rebind(
		"SELECT COUNT(*) FROM schedule_runs WHERE id = ?"), run.ID))
	require.NoError(t, queue.db.Conn().Get(&runningRunRows, queue.db.Conn().Rebind(
		"SELECT COUNT(*) FROM schedule_runs WHERE id = ?"), runningRunID))
	require.Zero(t, completedRunRows)
	require.Equal(t, 1, runningRunRows, "retention must never delete an active run")

	// Retrying the exact generated message ID models a connection loss after
	// COMMIT. The retry must succeed without another fanout, while a payload
	// mismatch on the same ID remains a hard conflict.
	ambiguousTopic, _ := createContractTopic(t, queue, "contract-ambiguous-commit", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	now := time.Now().UTC().Truncate(time.Millisecond)
	ambiguousRequest := writeRequest{
		TopicID: ambiguousTopic.ID, MessageID: newMessageIDAt(now), Message: "commit-once",
		Headers: []byte("{}"), VisibleAt: now, CreatedAt: now,
	}
	duplicates, err := queue.db.persistWriteRequests(context.Background(), []writeRequest{ambiguousRequest})
	require.NoError(t, err)
	require.Equal(t, []bool{false}, duplicates)
	duplicates, err = queue.db.persistWriteRequests(context.Background(), []writeRequest{ambiguousRequest})
	require.NoError(t, err)
	require.Equal(t, []bool{true}, duplicates)
	var canonicalRows, deliveryRows int
	require.NoError(t, queue.db.Conn().Get(&canonicalRows,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM messages WHERE id = ?"), ambiguousRequest.MessageID))
	require.NoError(t, queue.db.Conn().Get(&deliveryRows,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM message_deliveries WHERE message_id = ?"), ambiguousRequest.MessageID))
	require.Equal(t, 1, canonicalRows)
	require.Equal(t, 1, deliveryRows)
	conflict := ambiguousRequest
	conflict.Message = "different-payload"
	_, err = queue.db.persistWriteRequests(context.Background(), []writeRequest{conflict})
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	defaultTopic, defaultSubscriber := createContractTopic(t, queue, "contract-default-attempts", SubscriberOptions{
		VisibilityDuration: "25ms", DequeueBatchSize: 1,
	})
	require.Equal(t, 3, defaultSubscriber.Options.MaxAttempts)
	defaultReceipt, err := queue.PublishDurable(context.Background(), defaultTopic, Message{Message: "retry-by-default"})
	require.NoError(t, err)
	defaultClaim, err := queue.Claim(context.Background(), defaultTopic, defaultSubscriber.Name, 1, 25*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, defaultClaim, 1)
	require.Eventually(t, func() bool {
		var status string
		statusErr := queue.db.Conn().Get(&status, queue.db.Conn().Rebind(
			"SELECT status FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"),
			defaultReceipt.MessageID, defaultSubscriber.ID)
		return statusErr == nil && status == "pending"
	}, 3*time.Second, 20*time.Millisecond, "one expired lease must be retried by default, not dead-lettered")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, queue.Shutdown(ctx))
}

func createContractTopic(t *testing.T, queue *Queue, name string, options SubscriberOptions) (Topic, Subscriber) {
	t.Helper()
	topic := NewTopic(name)
	subscriber := NewSubscriber(topic, "worker", options)
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	return topic, subscriber
}
