// Package main demonstrates transactional enqueueing and transactional
// delivery completion with BlockQueue and an application table in one SQLite
// database.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type fulfillOrder struct {
	OrderID string `json:"order_id"`
}

func main() {
	ctx := context.Background()
	driver, err := sqlite.Open(":memory:", sqlite.Config{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		log.Fatal(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
	if err := queue.Run(ctx); err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	if _, err := driver.DB().ExecContext(ctx, `
		CREATE TABLE orders (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			completed_by_message_id TEXT
		)`); err != nil {
		log.Fatal(err)
	}

	topic := blockqueue.NewTopic("orders")
	worker := blockqueue.NewSubscriber(topic, "fulfillment", blockqueue.SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "30s",
	})
	if err := queue.CreateTopic(ctx, topic, blockqueue.Subscribers{worker}); err != nil {
		log.Fatal(err)
	}

	orderID := "order-1022"
	payload, err := json.Marshal(fulfillOrder{OrderID: orderID})
	if err != nil {
		log.Fatal(err)
	}

	// Producer transaction: the order row and its delivery either both commit
	// or both roll back. The worker cannot see the staged delivery before this
	// callback returns and WithTx commits.
	var receipt blockqueue.PublishReceipt
	if err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO orders (id, status) VALUES (?, ?)", orderID, "pending"); err != nil {
			return err
		}
		var publishErr error
		receipt, publishErr = queue.PublishTx(ctx, tx, topic, blockqueue.Message{
			Message:        string(payload),
			IdempotencyKey: "fulfill-" + orderID,
		})
		return publishErr
	}); err != nil {
		log.Fatal(err)
	}
	log.Printf("producer committed order and message %s", receipt.MessageID)

	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	deliveries, err := queue.ClaimWait(claimCtx, topic, worker.Name, 1, 30*time.Second)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	if len(deliveries) != 1 {
		log.Fatal("expected one delivery")
	}
	delivery := deliveries[0]
	var command fulfillOrder
	if err := json.Unmarshal([]byte(delivery.Message), &command); err != nil {
		log.Fatal(err)
	}

	// Consumer transaction: the application result and ACK commit together.
	// Returning an error from either operation rolls both changes back, leaving
	// the delivery eligible for retry after its lease expires.
	if err := queue.WithTx(ctx, nil, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE orders
			SET status = ?, completed_by_message_id = ?
			WHERE id = ? AND completed_by_message_id IS NULL`,
			"fulfilled", delivery.ID, command.OrderID)
		if err != nil {
			return err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if updated != 1 {
			return fmt.Errorf("order %s was already completed", command.OrderID)
		}
		return queue.AckDeliveryTx(
			ctx, tx, topic, worker.Name, delivery.ID, delivery.ReceiptToken,
		)
	}); err != nil {
		log.Fatal(err)
	}

	var status, completedBy string
	if err := driver.DB().QueryRowContext(ctx,
		"SELECT status, completed_by_message_id FROM orders WHERE id = ?", orderID,
	).Scan(&status, &completedBy); err != nil {
		log.Fatal(err)
	}
	log.Printf("consumer committed order=%s status=%s message=%s", orderID, status, completedBy)
}
