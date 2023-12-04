package core

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type MessageStatus string

const (
	MessageStatusWaiting   MessageStatus = "waiting"
	MessageStatusDelivered MessageStatus = "delivered"
)

type Message struct {
	Id        uuid.UUID     `db:"id"`
	TopicId   uuid.UUID     `db:"topic_id"`
	Message   string        `db:"message"`
	Status    MessageStatus `db:"status"`
	CreatedAt string        `db:"created_at"`
}

type Messages []Message

func (messages Messages) Ids() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.Id)
	}

	return ids
}

type FilterMessage struct {
	TopicId       []uuid.UUID
	Status        []MessageStatus
	SortBy        string
	SortDirection string
	Offset        int
	Limit         int
}

func (f FilterMessage) Filter(operator string) (string, map[string]interface{}) {
	var (
		clause []string
		args   = make(map[string]interface{})
	)

	if len(f.TopicId) > 0 {
		args["topic_id"] = f.TopicId
		clause = append(clause, "topic_id IN(:topic_id)")
	}

	if len(f.Status) > 0 {
		args["status"] = f.Status
		clause = append(clause, "status IN(:status)")
	}

	return strings.Join(clause, " "+operator+" "), args
}

func (f FilterMessage) Sort() string {
	sortBy := f.SortBy
	if f.SortBy == "" {
		sortBy = "created_at"
	}

	sortDirection := f.SortDirection
	if f.SortDirection == "" {
		sortDirection = "asc"
	}

	return fmt.Sprintf("ORDER BY %s %s", sortBy, sortDirection)
}

func (f FilterMessage) Page() string {
	if f.Offset == 0 && f.Limit == 0 {
		return ""
	}

	offset := (f.Offset - 1) * f.Limit

	return fmt.Sprintf(" LIMIT %d OFFSET %d", f.Limit, offset)
}
