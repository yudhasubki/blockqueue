package bucket

import "time"

type Message struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

type Messages []Message

type MessageVisibility struct {
	Message     Message                      `json:"message"`
	RetryPolicy MessageVisibilityRetryPolicy `json:"retry_policy"`
}

type MessageVisibilities []MessageVisibility

type MessageVisibilityRetryPolicy struct {
	MaxAttempts int       `json:"max_attempts"`
	NextIter    time.Time `json:"next_iteration"`
}
