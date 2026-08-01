package blockqueue

import (
	"encoding/json"
	"fmt"
)

func decodePersistedHeaders(raw string) (map[string]string, error) {
	headers := make(map[string]string)
	if raw == "" {
		return headers, nil
	}
	if err := json.Unmarshal([]byte(raw), &headers); err != nil {
		return nil, fmt.Errorf("decode persisted headers: %w", err)
	}
	if headers == nil {
		headers = make(map[string]string)
	}
	return headers, nil
}

func deliveryFromRow(row deliveryRow) (Delivery, error) {
	headers, err := decodePersistedHeaders(row.Headers)
	if err != nil {
		return Delivery{}, fmt.Errorf("message %s: %w", row.MessageID, err)
	}
	return Delivery{
		ID:             row.MessageID,
		Message:        row.Message,
		Headers:        headers,
		CorrelationID:  row.CorrelationID.String,
		Status:         row.Status,
		DeliveryCount:  row.DeliveryCount,
		FailureCount:   row.FailureCount,
		Priority:       row.Priority,
		ReceiptToken:   row.ReceiptToken.String,
		LeaseExpiresAt: optionalTime(row.LeaseExpiresAt),
		VisibleAt:      row.VisibleAt,
		CreatedAt:      row.CreatedAt,
	}, nil
}
