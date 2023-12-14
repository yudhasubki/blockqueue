package metric

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagePublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: fmt.Sprintf("message_store_to_the_topic_watcher"),
		Help: "The total number succesfully published to the topic",
	})
)
