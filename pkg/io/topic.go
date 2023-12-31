package io

import (
	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/pkg/core"
)

type Topic struct {
	Name        string      `json:"name"`
	Subscribers Subscribers `json:"subscribers"`
}

func (t Topic) Topic() core.Topic {
	return core.Topic{
		Id:   uuid.New(),
		Name: t.Name,
	}
}

func (t Topic) Subscriber(topicId uuid.UUID) core.Subscribers {
	subscribers := make(core.Subscribers, 0, len(t.Subscribers))
	for _, subscriber := range t.Subscribers {
		subscribers = append(subscribers, core.Subscriber{
			Id:      uuid.New(),
			TopicId: topicId,
			Name:    subscriber.Name,
			Option:  subscriber.Option.Marshal(),
		})
	}

	return subscribers
}
