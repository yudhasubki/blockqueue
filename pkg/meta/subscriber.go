package meta

type Subscriber struct {
	SubscriberName string `json:"subscriber_name"`
	TotalMessage   int    `json:"total_message"`
}

type Subscribers []Subscriber
