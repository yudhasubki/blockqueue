package blockqueue_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func ExampleQueue_Publish() {
	queue, topic := newExampleQueue()
	defer queue.Close()

	receipt, err := queue.Publish(context.Background(), topic, blockqueue.Message{
		Message:        `{"order_id":"order-1022"}`,
		IdempotencyKey: "order-1022",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(receipt.State, *receipt.Duplicate)
	// Output: persisted false
}

func ExampleQueue_WithTx() {
	queue, topic := newExampleQueue()
	defer queue.Close()

	ctx := context.Background()
	var publishState string
	err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			CREATE TABLE orders (id TEXT PRIMARY KEY, status TEXT NOT NULL)
		`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO orders (id, status) VALUES (?, ?)", "order-1022", "pending"); err != nil {
			return err
		}
		receipt, err := queue.PublishTx(ctx, tx, topic, blockqueue.Message{
			Message:        `{"order_id":"order-1022"}`,
			IdempotencyKey: "fulfill-order-1022",
		})
		publishState = receipt.State
		return err
	})

	fmt.Println(publishState, err == nil)
	// Output: staged true
}

func newExampleQueue() (*blockqueue.Queue, blockqueue.Topic) {
	driver, err := sqlite.Open(":memory:", sqlite.Config{})
	if err != nil {
		panic(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
	if err := queue.Run(context.Background()); err != nil {
		panic(err)
	}
	topic := blockqueue.NewTopic("orders")
	subscriber := blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{})
	if err := queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{subscriber}); err != nil {
		panic(err)
	}
	return queue, topic
}
