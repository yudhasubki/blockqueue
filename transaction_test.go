package blockqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublishTxCommitAndRollback(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx := context.Background()
	require.NoError(t, func() error {
		_, err := driver.DB().ExecContext(ctx, "CREATE TABLE business_records (id TEXT PRIMARY KEY)")
		return err
	}())

	var committed PublishReceipt
	err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO business_records (id) VALUES (?)", "committed"); err != nil {
			return err
		}
		var err error
		committed, err = queue.PublishTx(ctx, tx, topic, Message{Message: "committed"})
		return err
	})
	require.NoError(t, err)
	require.Equal(t, "staged", committed.State)
	require.NotNil(t, committed.Duplicate)
	require.False(t, *committed.Duplicate)

	rollbackErr := errors.New("rollback business operation")
	err = queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO business_records (id) VALUES (?)", "rolled-back"); err != nil {
			return err
		}
		if _, err := queue.PublishTx(ctx, tx, topic, Message{Message: "rolled-back"}); err != nil {
			return err
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)

	var business, messages, deliveries int
	require.NoError(t, testDB(driver).Get(&business, "SELECT COUNT(*) FROM business_records"))
	require.NoError(t, testDB(driver).Get(&messages, "SELECT COUNT(*) FROM messages"))
	require.NoError(t, testDB(driver).Get(&deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.Equal(t, 1, business)
	require.Equal(t, 1, messages)
	require.Equal(t, 1, deliveries)
}

func TestAckDeliveryTxRollsBackWithBusinessEffect(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx := context.Background()
	_, err := driver.DB().ExecContext(ctx, "CREATE TABLE effects (id TEXT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = queue.Publish(ctx, topic, Message{Message: "effect"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	rollbackErr := errors.New("rollback effect")
	err = queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO effects (id) VALUES (?)", "rolled-back"); err != nil {
			return err
		}
		if err := queue.AckDeliveryTx(ctx, tx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken); err != nil {
			return err
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)
	require.NoError(t, queue.AckDelivery(ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken))

	var effects int
	require.NoError(t, testDB(driver).Get(&effects, "SELECT COUNT(*) FROM effects"))
	require.Zero(t, effects)
}

func TestCancelClaimedDeliveryTxRollsBackWithBusinessEffect(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx := context.Background()
	_, err := driver.DB().ExecContext(ctx, "CREATE TABLE cancellation_effects (id TEXT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = queue.Publish(ctx, topic, Message{Message: "cancel atomically"})
	require.NoError(t, err)
	claimed, err := queue.Claim(ctx, topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	rollbackErr := errors.New("rollback cancellation")
	err = queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO cancellation_effects (id) VALUES (?)", "rolled-back"); err != nil {
			return err
		}
		if err := queue.CancelClaimedDeliveryTx(
			ctx, tx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken, "permanent",
		); err != nil {
			return err
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)
	require.NoError(t, queue.AckDelivery(ctx, topic, "worker", claimed[0].ID, claimed[0].ReceiptToken))

	var effects int
	require.NoError(t, testDB(driver).Get(&effects, "SELECT COUNT(*) FROM cancellation_effects"))
	require.Zero(t, effects)
}

func TestSQLiteCallerTransactionSerializesWriterWithoutLoss(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := driver.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = queue.PublishTx(ctx, tx, topic, Message{Message: "held"})
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, publishErr := queue.Publish(ctx, topic, Message{Message: "queued-behind-transaction"})
		result <- publishErr
	}()
	select {
	case err := <-result:
		require.Failf(t, "writer returned before transaction completed", "error=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, <-result)

	var messages, deliveries int
	require.NoError(t, testDB(driver).Get(&messages, "SELECT COUNT(*) FROM messages"))
	require.NoError(t, testDB(driver).Get(&deliveries, "SELECT COUNT(*) FROM message_deliveries"))
	require.Equal(t, 2, messages)
	require.Equal(t, 2, deliveries)
}

func TestShutdownDrainsRegisteredTransaction(t *testing.T) {
	queue, _, topic := setupQueue(t)
	started := make(chan struct{})
	release := make(chan struct{})
	transactionResult := make(chan error, 1)
	go func() {
		transactionResult <- queue.WithTx(context.Background(), nil, func(tx *sql.Tx) error {
			close(started)
			<-release
			_, err := queue.PublishTx(context.Background(), tx, topic, Message{Message: "drained transaction"})
			return err
		})
	}()
	<-started

	shutdownResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownResult <- queue.Shutdown(ctx)
	}()
	require.Eventually(t, func() bool { return queue.State() == LifecycleStopping }, time.Second, time.Millisecond)
	select {
	case err := <-shutdownResult:
		require.Failf(t, "shutdown returned before transaction drained", "error=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-transactionResult)
	require.NoError(t, <-shutdownResult)
	require.Equal(t, LifecycleStopped, queue.State())
}
