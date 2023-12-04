package core

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/guregu/null.v4"
)

type Subscriber struct {
	Id        uuid.UUID   `db:"id"`
	TopicId   uuid.UUID   `db:"topic_id"`
	TopicName string      `db:"topic_name"`
	Name      string      `db:"name"`
	CreatedAt string      `db:"created_at"`
	DeletedAt null.String `db:"deleted_at"`
}

func (s Subscriber) Bucket() string {
	return fmt.Sprintf("%s:%s", s.TopicId, s.Name)
}

type Subscribers []Subscriber

func (subscribers Subscribers) MapByTopic() map[uuid.UUID]Subscribers {
	mmap := make(map[uuid.UUID]Subscribers)

	for _, subscriber := range subscribers {
		if _, exist := mmap[subscriber.TopicId]; !exist {
			mmap[subscriber.TopicId] = make(Subscribers, 0)
		}
		mmap[subscriber.TopicId] = append(mmap[subscriber.TopicId], subscriber)
	}

	return mmap
}

type FilterSubscriber struct {
	TopicId     []uuid.UUID
	Name        []string
	WithDeleted bool
}

func (f FilterSubscriber) Filter(operator string) (string, map[string]interface{}) {
	var (
		clause []string
		args   = make(map[string]interface{})
	)

	if len(f.TopicId) > 0 {
		args["topic_id"] = f.TopicId
		clause = append(clause, "topic_subscribers.topic_id IN(:topic_id)")
	}

	if len(f.Name) > 0 {
		args["name"] = f.Name
		clause = append(clause, "topic_subscribers.name IN(:name)")
	}

	if f.WithDeleted {
		clause = append(clause, "topic_subscribers.deleted_at IS NOT NULL")
	} else {
		clause = append(clause, "topic_subscribers.deleted_at IS NULL")
	}

	return strings.Join(clause, " "+operator+" "), args
}
