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
	subscribers := blockqueue.Subscribers{
		blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{
			MaxAttempts:        5,
			VisibilityDuration: "1m",
			DequeueBatchSize:   10,
		}),
		blockqueue.NewSubscriber(topic, "notifications", blockqueue.SubscriberOptions{
			MaxAttempts:        5,
			VisibilityDuration: "1m",
			DequeueBatchSize:   10,
		}),
	}
	if err := queue.CreateTopic(context.Background(), topic, subscribers); err != nil {
		if !errors.Is(err, blockqueue.ErrResourceConflict) {
			return fmt.Errorf("create topic: %w", err)
		}
		existing, ok := queue.GetTopic(topic.Name)
		if !ok {
			return errors.New("worker-example.db reported a topic conflict but the topic is unavailable")
		}
		topic = existing
		log.Print("reusing existing orders topic")
		statuses, statusErr := queue.GetSubscribersStatus(context.Background(), topic)
		if statusErr != nil {
			return fmt.Errorf("read subscriber status: %w", statusErr)
		}
		existingSubscribers := make(map[string]struct{}, len(statuses))
		for _, status := range statuses {
			existingSubscribers[status.Name] = struct{}{}
		}
		missing := make(blockqueue.Subscribers, 0, len(subscribers))
		for index := range subscribers {
			subscribers[index].TopicID = existing.ID
			if _, found := existingSubscribers[subscribers[index].Name]; !found {
				missing = append(missing, subscribers[index])
			}
		}
		if len(missing) > 0 {
			if err := queue.CreateSubscribers(context.Background(), topic, missing); err != nil {
				return fmt.Errorf("create subscriber: %w", err)
			}
		}
	}

	fulfillmentWorker, err := blockworker.NewJSON(
		queue,
		topic,
		subscribers[0].Name,
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
		return fmt.Errorf("create fulfillment worker: %w", err)
	}

	notificationWorker, err := blockworker.NewJSON(
		queue,
		topic,
		subscribers[1].Name,
		blockworker.TypedHandlerFunc[fulfillOrder](func(
			_ context.Context,
			job *blockworker.TypedJob[fulfillOrder],
		) error {
			log.Printf("notification requested for %s", job.Args.OrderID)
			return nil
		}),
		blockworker.Options{Concurrency: 2},
	)
	if err != nil {
		return fmt.Errorf("create notification worker: %w", err)
	}

	group, err := blockworker.NewGroup(fulfillmentWorker, notificationWorker)
	if err != nil {
		return fmt.Errorf("create worker group: %w", err)
	}

	if _, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{
		Message:        `{"order_id":"order-1022"}`,
		IdempotencyKey: "fulfill-order-1022-" + time.Now().UTC().Format("20060102T150405.000000000"),
	}); err != nil {
		return fmt.Errorf("publish order: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Print("worker group running; press Ctrl-C to drain and stop")
	workerErr := group.Run(ctx)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	shutdownErr := queue.Shutdown(shutdownCtx)
	return errors.Join(workerErr, shutdownErr)
}
