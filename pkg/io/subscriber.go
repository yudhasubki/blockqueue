package io

import (
	"github.com/google/uuid"
	"github.com/yudhasubki/queuestream/pkg/core"
)

type RequestCreateSubscriberPartition struct {
	Subscriber  string
	PartitionId string
}

type Subscriber struct {
	Subscribers []string `json:"subscribers"`
}

func (subscriber Subscriber) Subscriber(topicId uuid.UUID) core.Subscribers {
	subscribers := make(core.Subscribers, 0)
	for _, sub := range subscriber.Subscribers {
		subscribers = append(subscribers, core.Subscriber{
			Id:      uuid.New(),
			TopicId: topicId,
			Name:    sub,
		})
	}

	return subscribers
}
