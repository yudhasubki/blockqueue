// Package main demonstrates basic BlockQueue library usage
// This example shows how to create a topic, publish messages, and consume them
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

func main() {
	// 1. Initialize database driver
	db, err := sqlite.New("example.db", sqlite.Config{
		BusyTimeout: 5000,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// 2. Create BlockQueue instance
	stream := blockqueue.New(db, blockqueue.BlockQueueOption{
		WriteBufferConfig: blockqueue.WriteBufferConfig{
			BatchSize:     100,
			FlushInterval: 100 * time.Millisecond,
			BufferSize:    1000,
		},
	})

	if err := stream.Run(ctx); err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	// 3. Define topic and subscriber
	request := io.Topic{
		Name: "notifications",
		Subscribers: io.Subscribers{
			{
				Name: "email_sender",
				Option: io.SubscriberOpt{
					MaxAttempts:        3,
					VisibilityDuration: "1m",
				},
			},
		},
	}

	topic := request.Topic()
	if err := stream.AddJob(ctx, topic, request.Subscriber(topic.Id)); err != nil {
		log.Fatal(err)
	}
	log.Printf("Created topic: %s with subscriber: email_sender", topic.Name)

	// 4. Start consumer in background
	done := make(chan bool)
	go func() {
		count := 0
		for count < 5 {
			messages, err := stream.Read(ctx, topic, "email_sender")
			if err != nil {
				log.Printf("Read error: %v", err)
				continue
			}

			for _, msg := range messages {
				log.Printf("Received: %s", msg.Message)

				// Simulate processing
				time.Sleep(50 * time.Millisecond)

				if err := stream.Ack(ctx, topic, "email_sender", msg.Id); err != nil {
					log.Printf("Ack error: %v", err)
					continue
				}
				log.Printf("Processed and acknowledged message")
				count++
			}
		}
		done <- true
	}()

	// 5. Publish messages
	for i := 1; i <= 5; i++ {
		msg := fmt.Sprintf("Notification #%d: Hello World", i)
		stream.Publish(ctx, topic, io.Publish{Message: msg})
		log.Printf("Published: %s", msg)
	}

	// 6. Wait for consumer to finish
	select {
	case <-done:
		log.Println("All messages processed")
	case <-time.After(10 * time.Second):
		log.Println("Timeout waiting for messages")
	}
}
