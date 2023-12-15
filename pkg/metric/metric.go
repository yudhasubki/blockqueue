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

	TotalFlightRequestQueueSubscriber = func(topicName, subscriberName string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("total_flight_request_queue_topic_%s_subscriber_%s", topicName, subscriberName),
			Help: fmt.Sprintf("The current total flight request queue on topic %s subscriber %s", topicName, subscriberName),
		})
	}

	TotalConsumedMessage = func(topicName, subscriberName string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("total_consumed_message_topic_%s_subscriber_%s", topicName, subscriberName),
			Help: fmt.Sprintf("The current total consumed message on topic %s subscriber %s", topicName, subscriberName),
		})
	}
)
