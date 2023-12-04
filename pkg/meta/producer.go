package meta

type Producer struct {
	Topic        string `json:"topic"`
	Subscriber   string `json:"subscriber"`
	ConsumerId   string `json:"consumer_id"`
	TotalMessage int    `json:"total_message"`
}
