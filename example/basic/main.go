// Package main demonstrates using BlockQueue as an imported Go library.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func main() {
	driver, err := sqlite.Open("example.db", sqlite.Config{})
	if err != nil {
		log.Fatal(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{})
	ctx := context.Background()
	if err := queue.Run(ctx); err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	topic := blockqueue.NewTopic("notifications")
	subscriber := blockqueue.NewSubscriber(topic,
		"email-sender",
		blockqueue.SubscriberOptions{
			MaxAttempts:        3,
			VisibilityDuration: "1m",
		},
	)
	if err := queue.CreateTopic(ctx, topic, blockqueue.Subscribers{subscriber}); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_, err := queue.Publish(ctx, topic,
			blockqueue.Message{
				Message:        fmt.Sprintf("notification-%d", i),
				IdempotencyKey: fmt.Sprintf("example-%d", i),
			},
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	messages, err := queue.ClaimWait(claimCtx, topic, subscriber.Name, 10, time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	for _, message := range messages {
		log.Printf("received %q", message.Message)
		if err := queue.AckDelivery(ctx, topic, subscriber.Name, message.ID, message.ReceiptToken); err != nil {
			log.Fatal(err)
		}
	}
}
