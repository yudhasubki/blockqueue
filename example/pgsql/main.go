// Package main demonstrates BlockQueue library usage with PostgreSQL
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/postgre"
)

func main() {
	host := flag.String("host", "localhost", "PostgreSQL host")
	port := flag.Int("port", 5432, "PostgreSQL port")
	user := flag.String("user", "postgres", "PostgreSQL user")
	pass := flag.String("password", "", "PostgreSQL password")
	dbname := flag.String("db", "blockqueue", "PostgreSQL database name")
	flag.Parse()

	// Initialize PostgreSQL driver
	db, err := postgre.New(postgre.Config{
		Host:         *host,
		Port:         *port,
		Username:     *user,
		Password:     *pass,
		Name:         *dbname,
		Timezone:     "UTC",
		MaxOpenConns: 25,
		MaxIdleConns: 10,
	})
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create BlockQueue instance
	// Note: Use smaller batch size for PostgreSQL due to parameter limits
	stream := blockqueue.New(db, blockqueue.BlockQueueOption{
		WriteBufferConfig: blockqueue.WriteBufferConfig{
			BatchSize:     500, // Smaller for PostgreSQL
			FlushInterval: 50 * time.Millisecond,
			BufferSize:    10000,
		},
	})

	if err := stream.Run(ctx); err != nil {
		log.Fatalf("failed to run stream: %v", err)
	}

	// Create topic with subscriber
	topicReq := io.Topic{
		Name: "events",
		Subscribers: io.Subscribers{
			{
				Name: "event_handler",
				Option: io.SubscriberOpt{
					MaxAttempts:        3,
					VisibilityDuration: "2m",
				},
			},
		},
	}

	topic := topicReq.Topic()
	if err := stream.AddJob(ctx, topic, topicReq.Subscriber(topic.Id)); err != nil {
		log.Fatalf("failed to add job: %v", err)
	}
	log.Printf("Created topic: %s", topic.Name)

	// Start consumer
	go consume(ctx, stream, topic)

	// Publish messages
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf(`{"event_type": "user_action", "user_id": %d}`, i)
		stream.Publish(ctx, topic, io.Publish{Message: msg})
		log.Printf("Published: %s", msg)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Shutting down...")
	case <-time.After(10 * time.Second):
		log.Println("Timeout, shutting down...")
	}

	cancel()
	stream.Close()
}

func consume(ctx context.Context, stream *blockqueue.BlockQueue[chan io.ResponseMessages], topic core.Topic) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			messages, err := stream.Read(ctx, topic, "event_handler")
			if err != nil {
				log.Printf("read error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, msg := range messages {
				log.Printf("Received: %s", msg.Message)

				// Process event here...

				if err := stream.Ack(ctx, topic, "event_handler", msg.Id); err != nil {
					log.Printf("ack error: %v", err)
					continue
				}
				log.Printf("Acknowledged: %s", msg.Id)
			}
		}
	}
}
