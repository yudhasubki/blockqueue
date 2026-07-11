package blockqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNackTracksFailureHistoryAndPolicyDelay(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx := context.Background()
	receipt, err := queue.Publish(ctx, topic, Message{Message: "retry-policy"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, queue.NackDelivery(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, 25*time.Millisecond, "temporary",
	))

	status, err := queue.GetMessageStatus(ctx, topic, receipt.MessageID)
	require.NoError(t, err)
	require.Len(t, status.Deliveries, 1)
	require.Equal(t, 1, status.Deliveries[0].DeliveryCount)
	require.Equal(t, 1, status.Deliveries[0].FailureCount)
	require.Equal(t, "pending", status.Deliveries[0].Status)

	errorsPage, err := queue.DeliveryErrors(ctx, topic, "worker", receipt.MessageID, 100, "")
	require.NoError(t, err)
	require.Len(t, errorsPage.Errors, 1)
	require.Equal(t, "temporary", errorsPage.Errors[0].Error)
	require.Equal(t, 1, errorsPage.Errors[0].FailureCount)

	var rows int
	require.NoError(t, testDB(driver).Get(&rows, "SELECT COUNT(*) FROM delivery_errors"))
	require.Equal(t, 1, rows)
}

func TestNackWithoutExplicitDelayUsesSubscriberRetryPolicy(t *testing.T) {
	queue, _, _ := setupQueue(t)
	ctx := context.Background()
	topic := NewTopic("retry-policy-topic")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
		RetryPolicy: RetryPolicy{InitialDelay: "40ms", MaxDelay: "40ms", Multiplier: 2, Jitter: 0.2},
	})
	require.NoError(t, queue.CreateTopic(ctx, topic, Subscribers{subscriber}))
	receipt, err := queue.Publish(ctx, topic, Message{Message: "retry-default"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, queue.NackDelivery(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, 0, "retry",
	))
	status, err := queue.GetMessageStatus(ctx, topic, receipt.MessageID)
	require.NoError(t, err)
	require.Greater(t, time.Until(status.Deliveries[0].VisibleAt), 20*time.Millisecond)
	immediate, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Empty(t, immediate)
	require.Eventually(t, func() bool {
		redelivered, claimErr := queue.Claim(ctx, topic, "worker", 1, time.Minute)
		return claimErr == nil && len(redelivered) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestSnoozeDoesNotConsumeFailureAttempt(t *testing.T) {
	queue, _, topic := setupQueue(t)
	ctx := context.Background()
	receipt, err := queue.Publish(ctx, topic, Message{Message: "snooze"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	visibleAt, err := queue.SnoozeDelivery(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, 25*time.Millisecond,
	)
	require.NoError(t, err)
	require.True(t, visibleAt.After(time.Now().Add(-time.Second)))

	status, err := queue.GetMessageStatus(ctx, topic, receipt.MessageID)
	require.NoError(t, err)
	require.Equal(t, 1, status.Deliveries[0].DeliveryCount)
	require.Zero(t, status.Deliveries[0].FailureCount)
	require.Equal(t, "pending", status.Deliveries[0].Status)
	require.ErrorIs(t, queue.AckDelivery(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken,
	), ErrLeaseLost)
}

func TestCancelMessageIsIdempotentAndFencesReceipt(t *testing.T) {
	queue, _, topic := setupQueue(t)
	ctx := context.Background()
	receipt, err := queue.Publish(ctx, topic, Message{Message: "cancel"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	results, err := queue.CancelMessage(ctx, topic, receipt.MessageID, "operator request")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "cancelled", results[0].Status)
	results, err = queue.CancelMessage(ctx, topic, receipt.MessageID, "operator request")
	require.NoError(t, err)
	require.Equal(t, "cancelled", results[0].Status)
	require.ErrorIs(t, queue.AckDelivery(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken,
	), ErrLeaseLost)

	status, err := queue.GetMessageStatus(ctx, topic, receipt.MessageID)
	require.NoError(t, err)
	require.Equal(t, "cancelled", status.Deliveries[0].Status)
	require.NotNil(t, status.Deliveries[0].CancelledAt)
	require.Equal(t, "operator request", status.Deliveries[0].CancelReason)
}

func TestDeliveryErrorHistorySurvivesDLQReplay(t *testing.T) {
	queue, _, _ := setupQueue(t)
	ctx := context.Background()
	topic := NewTopic("replay-error-history")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 1, VisibilityDuration: "1m",
		RetryPolicy: RetryPolicy{InitialDelay: "0s", MaxDelay: "0s"},
	})
	require.NoError(t, queue.CreateTopic(ctx, topic, Subscribers{subscriber}))
	receipt, err := queue.Publish(ctx, topic, Message{Message: "two failure cycles"})
	require.NoError(t, err)

	for cycle := 0; cycle < 2; cycle++ {
		claimed, claimErr := queue.Claim(ctx, topic, subscriber.Name, 1, time.Minute)
		require.NoError(t, claimErr)
		require.Len(t, claimed, 1)
		require.NoError(t, queue.NackDelivery(
			ctx, topic, subscriber.Name, claimed[0].ID, claimed[0].ReceiptToken, 0, "cycle failure",
		))
		if cycle == 0 {
			replay := queue.ReplayDeadLetters(ctx, topic, subscriber.Name, []string{receipt.MessageID})
			require.Len(t, replay, 1)
			require.Equal(t, "pending", replay[0].Status)
		}
	}

	first, err := queue.DeliveryErrors(ctx, topic, subscriber.Name, receipt.MessageID, 1, "")
	require.NoError(t, err)
	require.Len(t, first.Errors, 1)
	require.NotEmpty(t, first.NextCursor)
	second, err := queue.DeliveryErrors(ctx, topic, subscriber.Name, receipt.MessageID, 1, first.NextCursor)
	require.NoError(t, err)
	require.Len(t, second.Errors, 1)
	require.Empty(t, second.NextCursor)
	require.NotEqual(t, first.Errors[0].ID, second.Errors[0].ID)
	require.Equal(t, 1, first.Errors[0].FailureCount)
	require.Equal(t, 1, second.Errors[0].FailureCount)
}

func TestLeaseExtensionIsCappedAtTwelveHourHorizon(t *testing.T) {
	queue, _, topic := setupQueue(t)
	ctx := context.Background()
	_, err := queue.Publish(ctx, topic, Message{Message: "bounded lease"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, 11*time.Hour)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	before := time.Now().UTC()
	expires, err := queue.ExtendLease(
		ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, 12*time.Hour,
	)
	require.NoError(t, err)
	require.False(t, expires.After(before.Add(12*time.Hour+time.Second)))
	require.True(t, expires.After(before.Add(11*time.Hour)))
}

func TestSubscriberVisibilityCannotExceedLeaseLimit(t *testing.T) {
	queue, _, _ := setupQueue(t)
	topic := NewTopic("invalid-visibility")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{VisibilityDuration: "13h"})
	err := queue.CreateTopic(context.Background(), topic, Subscribers{subscriber})
	require.ErrorIs(t, err, ErrInvalidSubscriber)
}
