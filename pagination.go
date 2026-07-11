package blockqueue

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const maximumResourcePageSize = 1000

type resourceCursor struct {
	Sort string `json:"sort"`
	ID   string `json:"id"`
}

func encodeResourceCursor(sortValue, id string) string {
	encoded, _ := json.Marshal(resourceCursor{Sort: sortValue, ID: id})
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeResourceCursor(cursor string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", err
	}
	var decoded resourceCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", "", err
	}
	if decoded.Sort == "" || decoded.ID == "" {
		return "", "", fmt.Errorf("invalid resource cursor")
	}
	return decoded.Sort, decoded.ID, nil
}
