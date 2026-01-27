// Package main demonstrates BlockQueue library usage with SQLite
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

func main() {
	dbPath := flag.String("db", "blockqueue.db", "SQLite database path")
	flag.Parse()

	// Initialize SQLite driver
	db, err := sqlite.New(*dbPath, sqlite.Config{
		BusyTimeout: 5000,
		CacheSize:   -4000, // 4MB cache
		MmapSize:    0,     // Minimal memory
	})
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create BlockQueue instance
	stream := blockqueue.New(db, blockqueue.BlockQueueOption{
		WriteBufferConfig: blockqueue.WriteBufferConfig{
			BatchSize:     100,
			FlushInterval: 100 * time.Millisecond,
			BufferSize:    10000,
		},
	})

	if err := stream.Run(ctx); err != nil {
		log.Fatalf("failed to run stream: %v", err)
	}

	// Create topic with subscriber
	topicReq := io.Topic{
		Name: "orders",
		Subscribers: io.Subscribers{
			{
				Name: "order_processor",
				Option: io.SubscriberOpt{
					MaxAttempts:        5,
					VisibilityDuration: "5m",
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

	// Start HTTP Server for UI
	go func() {
		// Use default UI path ("./ui") if running from root, or relative if running from example dir
		// For simplicity, we assume running from root or we can try to guess.
		// Let's assume default "./ui" is fine if running from root.
		httpStream := &blockqueue.Http{
			Stream: stream,
			UIPath: "./ui",
		}
		log.Println("Starting UI at http://localhost:8080")
		if err := http.ListenAndServe(":8080", httpStream.Router()); err != nil {
			log.Printf("HTTP Server failed: %v", err)
		}
	}()

	// Publish messages
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf(`{"order_id": %d, "product": "item-%d"}`, i, i)
		stream.Publish(ctx, topic, io.Publish{Message: msg})
		log.Printf("Published: %s", msg)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Shutting down...")
		// Remove timeout to keep UI running
		// case <-time.After(10 * time.Second):
		// 	log.Println("Timeout, shutting down...")
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
			messages, err := stream.Read(ctx, topic, "order_processor")
			if err != nil {
				// Reduce log spam for read error (timeout)
				// log.Printf("read error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, msg := range messages {
				log.Printf("Received: %s", msg.Message)

				// Process message here...

				if err := stream.Ack(ctx, topic, "order_processor", msg.Id); err != nil {
					log.Printf("ack error: %v", err)
					continue
				}
				log.Printf("Acknowledged: %s", msg.Id)
			}
		}
	}
}
