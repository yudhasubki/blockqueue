package turso_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	blockqueue "github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/store/turso"
)

func TestOpenRejectsEmptyURL(t *testing.T) {
	_, err := turso.Open("  ")
	if !errors.Is(err, turso.ErrEmptyURL) {
		t.Fatalf("Open() error=%v, want ErrEmptyURL", err)
	}
}

func TestExperimentalLocalSmoke(t *testing.T) {
	driver, err := turso.Open("file://" + filepath.Join(t.TempDir(), "turso-smoke.db"))
	if err != nil {
		t.Fatal(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
	if err := queue.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	topic := blockqueue.NewTopic("turso-smoke")
	subscriber := blockqueue.NewSubscriber(topic, "worker", blockqueue.SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "30s",
	})
	if err := queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{subscriber}); err != nil {
		t.Fatal(err)
	}
	receipt, err := queue.PublishDurable(context.Background(), topic, blockqueue.Message{Message: "smoke"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := queue.Claim(context.Background(), topic, subscriber.Name, 1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].ID != receipt.MessageID {
		t.Fatalf("claim count=%d err=%v", len(claimed), err)
	}
	if err := queue.AckDelivery(context.Background(), topic, subscriber.Name, claimed[0].ID, claimed[0].ReceiptToken); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := queue.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}
