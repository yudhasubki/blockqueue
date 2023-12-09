package io

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/pkg/core"
)

type Subscriber struct {
	Name   string        `json:"name"`
	Option SubscriberOpt `json:"option"`
}

type Subscribers []Subscriber

func (subscriber Subscribers) Subscriber(topicId uuid.UUID) core.Subscribers {
	subscribers := make(core.Subscribers, 0)
	for _, sub := range subscriber {
		subscribers = append(subscribers, core.Subscriber{
			Id:      uuid.New(),
			TopicId: topicId,
			Name:    sub.Name,
			Option:  sub.Option.Marshal(),
		})
	}

	return subscribers
}

type SubscriberOpt struct {
	MaxAttempts        int    `json:"max_attempts"`
	VisibilityDuration string `json:"visibility_duration"`
}

func (s *SubscriberOpt) Marshal() []byte {
	if s.MaxAttempts <= 0 {
		s.MaxAttempts = 1
	}

	parse, err := time.ParseDuration(s.VisibilityDuration)
	if err != nil {
		s.VisibilityDuration = "5m"
	} else {
		if parse.Minutes() < 5 {
			s.VisibilityDuration = "1m"
		}
	}

	b, _ := json.Marshal(s)

	return b
}
