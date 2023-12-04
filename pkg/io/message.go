package io

import "fmt"

type ResponseMessage struct {
	Id      string `json:"id"`
	Message string `json:"message"`
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
