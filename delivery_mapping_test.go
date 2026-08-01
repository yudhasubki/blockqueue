package blockqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeliveryFromRowDecodesHeaders(t *testing.T) {
	now := time.Now().UTC()
	delivery, err := deliveryFromRow(deliveryRow{
		MessageID:      "message-id",
		Message:        "payload",
		Headers:        `{"source":"test"}`,
		CorrelationID:  sql.NullString{String: "correlation", Valid: true},
		Status:         DeliveryStatusPending,
		ReceiptToken:   sql.NullString{String: "receipt", Valid: true},
		LeaseExpiresAt: sql.NullTime{Time: now.Add(time.Minute), Valid: true},
		VisibleAt:      now,
		CreatedAt:      now,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"source": "test"}, delivery.Headers)
	require.Equal(t, "correlation", delivery.CorrelationID)
	require.Equal(t, "receipt", delivery.ReceiptToken)
	require.NotNil(t, delivery.LeaseExpiresAt)
}

func TestPersistedHeaderDecodeRejectsCorruption(t *testing.T) {
	_, err := deliveryFromRow(deliveryRow{MessageID: "corrupt", Headers: "{"})
	require.ErrorContains(t, err, "decode persisted headers")

	schedule := Schedule{Headers: "{"}
	_, err = json.Marshal(schedule)
	require.ErrorContains(t, err, "decode persisted headers")
	require.Empty(t, schedule.PublicHeaders(), "compatibility helper must remain safe for existing callers")
}

func TestClaimRejectsCorruptHeadersBeforeLeasing(t *testing.T) {
	queue, driver, topic := setupQueue(t)
	receipt, err := queue.PublishDurable(context.Background(), topic, Message{Message: "payload"})
	require.NoError(t, err)
	_, err = testDB(driver).Exec("UPDATE messages SET headers = ? WHERE id = ?", "{", receipt.MessageID)
	require.NoError(t, err)

	_, err = queue.Claim(context.Background(), topic, "worker", 1, time.Minute)
	require.ErrorContains(t, err, "decode persisted headers")
	var state struct {
		Status         string         `db:"status"`
		DeliveryCount  int            `db:"delivery_count"`
		ReceiptToken   sql.NullString `db:"receipt_token"`
		LeaseExpiresAt sql.NullTime   `db:"lease_expires_at"`
	}
	require.NoError(t, testDB(driver).Get(&state, `
		SELECT status, delivery_count, receipt_token, lease_expires_at
		FROM message_deliveries WHERE message_id = ?`, receipt.MessageID))
	require.Equal(t, DeliveryStatusPending, state.Status)
	require.Zero(t, state.DeliveryCount)
	require.False(t, state.ReceiptToken.Valid)
	require.False(t, state.LeaseExpiresAt.Valid)

	_, err = testDB(driver).Exec("UPDATE messages SET headers = '{}' WHERE id = ?", receipt.MessageID)
	require.NoError(t, err)
	claimed, err := queue.Claim(context.Background(), topic, "worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
}
