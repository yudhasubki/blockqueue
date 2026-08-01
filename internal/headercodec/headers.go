// Package headercodec owns the persisted JSON representation of message headers.
package headercodec

import (
	"encoding/json"
	"errors"
)

var errObjectRequired = errors.New("headers must be a JSON object with string values")

// Decode converts persisted headers into their public representation.
func Decode(raw string) (map[string]string, error) {
	headers := make(map[string]string)
	if raw == "" {
		return headers, nil
	}
	if err := json.Unmarshal([]byte(raw), &headers); err != nil {
		return nil, err
	}
	if headers == nil {
		return nil, errObjectRequired
	}
	return headers, nil
}
