// Package main demonstrates the typed BlockQueue worker runtime.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/sqlite"
	blockworker "github.com/yudhasubki/blockqueue/worker"
)

type fulfillOrder struct {
	OrderID string `json:"order_id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	driver, err := sqlite.Open("worker-example.db", sqlite.Config{})
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{})
	if err := queue.Run(context.Background()); err != nil {
		return fmt.Errorf("run queue: %w", err)
	}
	defer queue.Close()

	if _, err := driver.DB().Exec(`
		CREATE TABLE IF NOT EXISTS fulfilled_orders (
			order_id TEXT PRIMARY KEY,
			fulfilled_at DATETIME NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create business table: %w", err)
	}

	topic := blockqueue.NewTopic("orders")
	subscriber := blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{
		MaxAttempts:        5,
		VisibilityDuration: "1m",
		DequeueBatchSize:   10,
	})
	if err := queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{subscriber}); err != nil {
		if !errors.Is(err, blockqueue.ErrResourceConflict) {
			return fmt.Errorf("create topic: %w", err)
		}
		existing, ok := queue.GetTopic(topic.Name)
		if !ok {
			return errors.New("worker-example.db reported a topic conflict but the topic is unavailable")
		}
		topic = existing
		subscriber.TopicID = existing.ID
		log.Print("reusing existing orders topic")
		statuses, statusErr := queue.GetSubscribersStatus(context.Background(), topic)
		if statusErr != nil {
			return fmt.Errorf("read subscriber status: %w", statusErr)
		}
		found := false
		for _, status := range statuses {
			if status.Name == subscriber.Name {
				found = true
				break
			}
		}
		if !found {
			if err := queue.CreateSubscribers(
				context.Background(), topic, blockqueue.Subscribers{subscriber},
			); err != nil {
				return fmt.Errorf("create subscriber: %w", err)
			}
		}
	}

	runner, err := blockworker.NewJSON(
		queue,
		topic,
		subscriber.Name,
		blockworker.TypedHandlerFunc[fulfillOrder](func(
			ctx context.Context,
			job *blockworker.TypedJob[fulfillOrder],
		) error {
			return job.CompleteTx(ctx, nil, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO fulfilled_orders (order_id, fulfilled_at)
					VALUES (?, ?)
					ON CONFLICT (order_id) DO NOTHING
				`, job.Args.OrderID, time.Now().UTC())
				return err
			})
		}),
		blockworker.Options{Concurrency: 4},
	)
	if err != nil {
		return fmt.Errorf("create worker: %w", err)
	}

	if _, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{
		Message:        `{"order_id":"order-1022"}`,
		IdempotencyKey: "fulfill-order-1022-" + time.Now().UTC().Format("20060102T150405.000000000"),
	}); err != nil {
		return fmt.Errorf("publish order: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Print("worker running; press Ctrl-C to drain and stop")
	workerErr := runner.Run(ctx)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	shutdownErr := queue.Shutdown(shutdownCtx)
	return errors.Join(workerErr, shutdownErr)
}
