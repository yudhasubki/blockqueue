package io

import "fmt"

type ResponseMessage struct {
	Id         string `json:"id"`
	Message    string `json:"message"`
	Status     string `json:"status,omitempty"`
	RetryCount int    `json:"retry_count,omitempty"`
	VisibleAt  string `json:"visible_at,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

type ResponseMessages []ResponseMessage

type RequestDeletePartitionMessage struct {
	Subscriber  string
	PartitionId string
	MessageId   string
}

type MessageClaim struct {
	SourceId      string `json:"source_id"`
	DestinationId string `json:"destination_id"`
}

func (m *MessageClaim) SourceConsumerBucket(bucket string) string {
	return fmt.Sprintf("%s:%s", bucket, m.SourceId)
}

func (m MessageClaim) DestinationConsumerBucket(bucket string) string {
	return fmt.Sprintf("%s:%s", bucket, m.DestinationId)
}
