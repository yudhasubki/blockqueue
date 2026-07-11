package blockqueue

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestPersistenceWorksWithSingleSQLiteConnection(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "single-connection.db"), sqlite.Config{
		MaxOpenConns: 1, MaxIdleConns: 1,
	})
	require.NoError(t, err)
	queue := New(driver, Options{})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("single-connection")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "30s",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = queue.Publish(ctx, topic, Message{Message: "prepared"})
	require.NoError(t, err)
	require.NoError(t, queue.Shutdown(ctx))
}

func TestDeliveryHotPathStatementsReuseSingleConnectionSafely(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "delivery-statements.db"), sqlite.Config{
		MaxOpenConns: 1, MaxIdleConns: 1,
	})
	require.NoError(t, err)
	queue := New(driver, Options{})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("delivery-statements")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "30s",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	run := func(payload string) {
		_, publishErr := queue.Publish(ctx, topic, Message{Message: payload})
		require.NoError(t, publishErr)
		claimed, claimErr := queue.Claim(ctx, topic, subscriber.Name, 1, time.Minute)
		require.NoError(t, claimErr)
		require.Len(t, claimed, 1)
		require.NoError(t, queue.AckDelivery(
			ctx, topic, subscriber.Name, claimed[0].ID, claimed[0].ReceiptToken,
		))
		_, _, wakeErr := queue.db.nextDeliveryWake(ctx, subscriber.ID)
		require.NoError(t, wakeErr)
	}

	run("first")
	firstSize := queue.db.statementCacheLen()
	run("second")
	require.Equal(t, firstSize, queue.db.statementCacheLen(),
		"repeated fixed-shape delivery operations must reuse prepared statements")
	require.NoError(t, queue.Shutdown(ctx))
}
