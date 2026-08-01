package blockqueue

import (
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
