package blockqueue

import (
	"context"
	"database/sql"
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
	runTransactionalPublishContract(t, queue, driver, topic)
	if driver.Dialect() == store.DialectPostgres {
		runPostgresTopologyFenceContract(t, queue, driver)
	}

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

	cancelTopic, cancelSubscriber := createContractTopic(t, queue, "contract-receipt-cancel", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	cancelReceipt, err := queue.PublishDurable(context.Background(), cancelTopic, Message{Message: "poison"})
	require.NoError(t, err)
	cancelClaim, err := queue.Claim(context.Background(), cancelTopic, cancelSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, cancelClaim, 1)
	require.NoError(t, queue.CancelClaimedDelivery(
		context.Background(), cancelTopic, cancelSubscriber.Name,
		cancelClaim[0].ID, cancelClaim[0].ReceiptToken, "unsupported payload",
	))
	require.NoError(t, queue.CancelClaimedDelivery(
		context.Background(), cancelTopic, cancelSubscriber.Name,
		cancelClaim[0].ID, cancelClaim[0].ReceiptToken, "unsupported payload",
	))
	cancelStatus, err := queue.GetMessageStatus(context.Background(), cancelTopic, cancelReceipt.MessageID)
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelStatus.Deliveries[0].Status)
	require.Equal(t, "unsupported payload", cancelStatus.Deliveries[0].CancelReason)

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

	snoozeTopic, snoozeSubscriber := createContractTopic(t, queue, "contract-snooze", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	snoozeReceipt, err := queue.PublishDurable(context.Background(), snoozeTopic, Message{Message: "snooze without failure"})
	require.NoError(t, err)
	snoozeClaim, err := queue.Claim(context.Background(), snoozeTopic, snoozeSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, snoozeClaim, 1)
	_, err = queue.SnoozeDelivery(context.Background(), snoozeTopic, snoozeSubscriber.Name,
		snoozeClaim[0].ID, snoozeClaim[0].ReceiptToken, 25*time.Millisecond)
	require.NoError(t, err)
	snoozeStatus, err := queue.GetMessageStatus(context.Background(), snoozeTopic, snoozeReceipt.MessageID)
	require.NoError(t, err)
	require.Equal(t, 1, snoozeStatus.Deliveries[0].DeliveryCount)
	require.Zero(t, snoozeStatus.Deliveries[0].FailureCount)
	require.Equal(t, "pending", snoozeStatus.Deliveries[0].Status)
	require.ErrorIs(t, queue.AckDelivery(context.Background(), snoozeTopic, snoozeSubscriber.Name,
		snoozeClaim[0].ID, snoozeClaim[0].ReceiptToken), ErrLeaseLost)
	snoozeContext, snoozeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	snoozedAgain, err := queue.ClaimWait(snoozeContext, snoozeTopic, snoozeSubscriber.Name, 1, time.Second)
	snoozeCancel()
	require.NoError(t, err)
	require.Len(t, snoozedAgain, 1)
	require.NoError(t, queue.AckDelivery(context.Background(), snoozeTopic, snoozeSubscriber.Name,
		snoozedAgain[0].ID, snoozedAgain[0].ReceiptToken))

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
	require.NoError(t, queue.NackDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		replayedClaim[0].ID, replayedClaim[0].ReceiptToken, time.Millisecond, "replayed retry"))
	replayedContext, replayedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	replayedFinal, err := queue.ClaimWait(replayedContext, retryTopic, retrySubscriber.Name, 1, time.Second)
	replayedCancel()
	require.NoError(t, err)
	require.Len(t, replayedFinal, 1)
	require.NoError(t, queue.NackDelivery(context.Background(), retryTopic, retrySubscriber.Name,
		replayedFinal[0].ID, replayedFinal[0].ReceiptToken, 0, "replayed terminal"))
	errorHistory, err := queue.DeliveryErrors(context.Background(), retryTopic, retrySubscriber.Name,
		retryReceipt.MessageID, 10, "")
	require.NoError(t, err)
	require.Len(t, errorHistory.Errors, 4, "DLQ replay must start a new failure cycle without losing history")

	policyTopic, policySubscriber := createContractTopic(t, queue, "contract-retry-policy", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
		RetryPolicy: RetryPolicy{InitialDelay: "200ms", MaxDelay: "200ms", Multiplier: 2, Jitter: 0.2},
	})
	policyReceipt, err := queue.PublishDurable(context.Background(), policyTopic, Message{Message: "policy retry"})
	require.NoError(t, err)
	policyClaim, err := queue.Claim(context.Background(), policyTopic, policySubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, policyClaim, 1)
	failedAt := time.Now().UTC()
	require.NoError(t, queue.NackDelivery(context.Background(), policyTopic, policySubscriber.Name,
		policyClaim[0].ID, policyClaim[0].ReceiptToken, 0, "policy delay"))
	policyStatus, err := queue.GetMessageStatus(context.Background(), policyTopic, policyReceipt.MessageID)
	require.NoError(t, err)
	policyDelay := policyStatus.Deliveries[0].VisibleAt.Sub(failedAt)
	require.GreaterOrEqual(t, policyDelay, 100*time.Millisecond)
	require.LessOrEqual(t, policyDelay, 350*time.Millisecond)
	require.Equal(t, 1, policyStatus.Deliveries[0].FailureCount)

	retentionTopic, retentionSubscriber := createContractTopic(t, queue, "contract-error-retention", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	retentionReceipt, err := queue.PublishDurable(context.Background(), retentionTopic, Message{Message: "cascade error history"})
	require.NoError(t, err)
	retentionClaim, err := queue.Claim(context.Background(), retentionTopic, retentionSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, retentionClaim, 1)
	require.NoError(t, queue.NackDelivery(context.Background(), retentionTopic, retentionSubscriber.Name,
		retentionClaim[0].ID, retentionClaim[0].ReceiptToken, time.Hour, "retained until delivery removal"))
	var retainedErrors int
	require.NoError(t, queue.db.Conn().Get(&retainedErrors, queue.db.Conn().Rebind(
		"SELECT COUNT(*) FROM delivery_errors WHERE message_id = ?"), retentionReceipt.MessageID))
	require.Equal(t, 1, retainedErrors)
	require.NoError(t, queue.DeleteSubscriber(context.Background(), retentionTopic, retentionSubscriber.Name))
	require.NoError(t, queue.db.Conn().Get(&retainedErrors, queue.db.Conn().Rebind(
		"SELECT COUNT(*) FROM delivery_errors WHERE message_id = ?"), retentionReceipt.MessageID))
	require.Zero(t, retainedErrors, "failure history retention must follow its delivery row")

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

	terminalTopic, terminalSubscriber := createContractTopic(t, queue, "contract-terminal-race", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1s", DequeueBatchSize: 1,
	})
	terminalReceipt, err := queue.PublishDurable(context.Background(), terminalTopic, Message{Message: "one terminal owner"})
	require.NoError(t, err)
	terminalClaim, err := queue.Claim(context.Background(), terminalTopic, terminalSubscriber.Name, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, terminalClaim, 1)
	type terminalResult struct {
		operation string
		err       error
	}
	terminalResults := make(chan terminalResult, 3)
	startTerminal := make(chan struct{})
	var terminalWorkers sync.WaitGroup
	for operation, execute := range map[string]func() error{
		"ack": func() error {
			return queue.AckDelivery(context.Background(), terminalTopic, terminalSubscriber.Name,
				terminalClaim[0].ID, terminalClaim[0].ReceiptToken)
		},
		"nack": func() error {
			return queue.NackDelivery(context.Background(), terminalTopic, terminalSubscriber.Name,
				terminalClaim[0].ID, terminalClaim[0].ReceiptToken, time.Hour, "terminal race")
		},
		"cancel": func() error {
			return queue.CancelClaimedDelivery(context.Background(), terminalTopic, terminalSubscriber.Name,
				terminalClaim[0].ID, terminalClaim[0].ReceiptToken, "terminal race")
		},
	} {
		operation, execute := operation, execute
		terminalWorkers.Add(1)
		go func() {
			defer terminalWorkers.Done()
			<-startTerminal
			terminalResults <- terminalResult{operation: operation, err: execute()}
		}()
	}
	close(startTerminal)
	terminalWorkers.Wait()
	close(terminalResults)
	winner := ""
	for result := range terminalResults {
		if result.err == nil {
			require.Empty(t, winner, "more than one terminal operation succeeded")
			winner = result.operation
			continue
		}
		require.ErrorIs(t, result.err, ErrLeaseLost)
	}
	require.NotEmpty(t, winner)
	terminalStatus, err := queue.GetMessageStatus(context.Background(), terminalTopic, terminalReceipt.MessageID)
	require.NoError(t, err)
	require.Len(t, terminalStatus.Deliveries, 1)
	expectedTerminalStatus := map[string]string{"ack": "processed", "nack": "pending", "cancel": "cancelled"}[winner]
	require.Equal(t, expectedTerminalStatus, terminalStatus.Deliveries[0].Status)

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

	expiryTopic, expirySubscriber := createContractTopic(t, queue, "contract-schedule-expiry", SubscriberOptions{
		MaxAttempts: 1, VisibilityDuration: "25ms", DequeueBatchSize: 1,
		RetryPolicy: RetryPolicy{InitialDelay: "0s", MaxDelay: "0s"},
	})
	expirySchedule, err := queue.CreateSchedule(context.Background(), expiryTopic, ScheduleInput{
		Name: "expire", CronExpression: "0 0 * * *", Timezone: "UTC", Message: "expire-run",
	})
	require.NoError(t, err)
	_, err = queue.RunScheduleNow(context.Background(), expiryTopic, expirySchedule.ID, false)
	require.NoError(t, err)
	expiryClaim, err := queue.Claim(context.Background(), expiryTopic, expirySubscriber.Name, 1, 25*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, expiryClaim, 1)
	require.Eventually(t, func() bool {
		history, historyErr := queue.ScheduleRunHistory(
			context.Background(), expiryTopic, expirySchedule.ID, 10, "",
		)
		return historyErr == nil && len(history.Runs) == 1 && history.Runs[0].Status == "completed"
	}, 3*time.Second, 20*time.Millisecond, "lease-expiry DLQ must complete only its related schedule run")

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

func runTransactionalPublishContract(t *testing.T, queue *Queue, driver store.Driver, topic Topic) {
	t.Helper()
	ctx := context.Background()
	database := testDB(driver)
	_, err := database.ExecContext(ctx, "CREATE TABLE contract_business_records (id VARCHAR(36) PRIMARY KEY)")
	require.NoError(t, err)
	observer, err := database.Connx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, observer.Close()) }()

	tx, err := driver.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, database.Rebind("INSERT INTO contract_business_records (id) VALUES (?)"), uuid.NewString())
	require.NoError(t, err)
	receipt, err := queue.PublishTx(ctx, tx, topic, Message{Message: "transaction-commit"})
	require.NoError(t, err)
	require.Equal(t, "staged", receipt.State)
	var visibleBeforeCommit int
	require.NoError(t, observer.GetContext(ctx, &visibleBeforeCommit,
		database.Rebind("SELECT COUNT(*) FROM messages WHERE id = ?"), receipt.MessageID))
	require.Zero(t, visibleBeforeCommit, "staged rows must not be visible before caller commit")
	require.NoError(t, tx.Commit())

	tx, err = driver.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, database.Rebind("INSERT INTO contract_business_records (id) VALUES (?)"), uuid.NewString())
	require.NoError(t, err)
	_, err = queue.PublishTx(ctx, tx, topic, Message{Message: "transaction-rollback"})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	var businessRows, committedMessages, rolledBackMessages int
	require.NoError(t, database.Get(&businessRows, "SELECT COUNT(*) FROM contract_business_records"))
	require.NoError(t, database.Get(&committedMessages,
		database.Rebind("SELECT COUNT(*) FROM messages WHERE id = ?"), receipt.MessageID))
	require.NoError(t, database.Get(&rolledBackMessages,
		"SELECT COUNT(*) FROM messages WHERE message = 'transaction-rollback'"))
	require.Equal(t, 1, businessRows)
	require.Equal(t, 1, committedMessages)
	require.Zero(t, rolledBackMessages)

	claimed, err := queue.Claim(ctx, topic, "one", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	tx, err = driver.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, database.Rebind("INSERT INTO contract_business_records (id) VALUES (?)"), uuid.NewString())
	require.NoError(t, err)
	require.NoError(t, queue.AckDeliveryTx(ctx, tx, topic, "one", claimed[0].ID, claimed[0].ReceiptToken))
	require.NoError(t, tx.Rollback())
	require.NoError(t, queue.AckDelivery(ctx, topic, "one", claimed[0].ID, claimed[0].ReceiptToken))

	nackReceipt, err := queue.Publish(ctx, topic, Message{Message: "transactional-nack"})
	require.NoError(t, err)
	nackClaim, err := queue.Claim(ctx, topic, "one", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, nackClaim, 1)
	require.Equal(t, nackReceipt.MessageID, nackClaim[0].ID)
	require.NoError(t, queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		return queue.NackDeliveryTx(ctx, tx, topic, "one", nackClaim[0].ID,
			nackClaim[0].ReceiptToken, time.Hour, "transactional failure")
	}))
	errorPage, err := queue.DeliveryErrors(ctx, topic, "one", nackReceipt.MessageID, 10, "")
	require.NoError(t, err)
	require.Len(t, errorPage.Errors, 1)
	require.Equal(t, 1, errorPage.Errors[0].FailureCount)

	cancelReceipt, err := queue.Publish(ctx, topic, Message{Message: "transactional-cancel"})
	require.NoError(t, err)
	var cancelResults []DeliveryResult
	require.NoError(t, queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		results, cancelErr := queue.CancelMessageTx(ctx, tx, topic, cancelReceipt.MessageID, "business rollback")
		cancelResults = results
		return cancelErr
	}))
	require.Len(t, cancelResults, 2)
	status, err := queue.GetMessageStatus(ctx, topic, cancelReceipt.MessageID)
	require.NoError(t, err)
	require.Len(t, status.Deliveries, 2)
	for _, delivery := range status.Deliveries {
		require.Equal(t, "cancelled", delivery.Status)
		require.Equal(t, "business rollback", delivery.CancelReason)
	}
}

func runPostgresTopologyFenceContract(t *testing.T, queue *Queue, driver store.Driver) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	topic := NewTopic("transaction-fence")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{MaxAttempts: 3, VisibilityDuration: "1m"})
	require.NoError(t, queue.CreateTopic(ctx, topic, Subscribers{subscriber}))

	tx, err := driver.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = queue.PublishTx(ctx, tx, topic, Message{Message: "held-shared-fence"})
	require.NoError(t, err)

	publishResult := make(chan error, 1)
	go func() {
		_, publishErr := queue.Publish(ctx, topic, Message{Message: "concurrent-publisher"})
		publishResult <- publishErr
	}()
	select {
	case err := <-publishResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "shared topology fence blocked another publisher")
	}

	mutationResult := make(chan error, 1)
	go func() { mutationResult <- queue.DeleteSubscriber(ctx, topic, subscriber.Name) }()
	select {
	case err := <-mutationResult:
		require.Failf(t, "mutation bypassed topology fence", "error=%v", err)
	case <-time.After(100 * time.Millisecond):
	}
	secondReceipt, err := queue.PublishTx(ctx, tx, topic, Message{Message: "same caller transaction"})
	require.NoError(t, err,
		"caller-owned transactions rely on the database row fence and must not deadlock on the process-local writer fence")
	require.Equal(t, "staged", secondReceipt.State)
	require.NoError(t, tx.Commit())
	require.NoError(t, <-mutationResult)
}

func createContractTopic(t *testing.T, queue *Queue, name string, options SubscriberOptions) (Topic, Subscriber) {
	t.Helper()
	topic := NewTopic(name)
	subscriber := NewSubscriber(topic, "worker", options)
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	return topic, subscriber
}
