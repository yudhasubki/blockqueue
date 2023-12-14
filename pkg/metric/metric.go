package metric

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagePublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "message_store_to_the_topic_watcher",
		Help: "The total number succesfully published to the topic watcher",
	})

	MessagePublishedTopic = func(topicName string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("message_published_%s", topicName),
			Help: fmt.Sprintf("The total number succesfully published to the topic %s", topicName),
		})
	}
)
