package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yudhasubki/blockqueue"
)

func TestDashboardParsesAndRendersPageEnvelopes(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required for the dependency-free dashboard contract test")
	}

	queue, handler := setupHTTPQueue(t)
	orders, exists := queue.GetTopic("orders")
	if !exists {
		t.Fatal("orders topic was not created")
	}
	extraSubscriber := blockqueue.NewSubscriber(orders, "worker<primary>", blockqueue.SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "1m",
	})
	if err := queue.CreateSubscribers(context.Background(), orders, blockqueue.Subscribers{extraSubscriber}); err != nil {
		t.Fatal(err)
	}
	dangerousTopic := blockqueue.NewTopic("returns<script>")
	dangerousSubscriber := blockqueue.NewSubscriber(dangerousTopic, "worker", blockqueue.SubscriberOptions{
		MaxAttempts:        3,
		VisibilityDuration: "1m",
	})
	if err := queue.CreateTopic(
		context.Background(), dangerousTopic, blockqueue.Subscribers{dangerousSubscriber},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.PublishDurable(context.Background(), orders, blockqueue.Message{Message: "dashboard"}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(context.Background(), orders, "worker", 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	topicsResponse := httptest.NewRecorder()
	handler.ServeHTTP(topicsResponse, httptest.NewRequest(http.MethodGet, "/v1/topics?limit=100", nil))
	if topicsResponse.Code != http.StatusOK {
		t.Fatalf("topics status=%d body=%s", topicsResponse.Code, topicsResponse.Body.String())
	}
	subscribersResponse := httptest.NewRecorder()
	handler.ServeHTTP(subscribersResponse, httptest.NewRequest(
		http.MethodGet, "/v1/topics/orders/subscribers?limit=100", nil,
	))
	if subscribersResponse.Code != http.StatusOK {
		t.Fatalf("subscribers status=%d body=%s", subscribersResponse.Code, subscribersResponse.Body.String())
	}

	fixture := struct {
		Topics      json.RawMessage `json:"topics"`
		Subscribers json.RawMessage `json:"subscribers"`
	}{
		Topics:      append(json.RawMessage(nil), topicsResponse.Body.Bytes()...),
		Subscribers: append(json.RawMessage(nil), subscribersResponse.Body.Bytes()...),
	}
	payload, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(t.TempDir(), "page-envelopes.json")
	if err := os.WriteFile(fixturePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(node, filepath.Join("testdata", "dashboard_contract.js"), fixturePath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dashboard contract failed: %v\n%s", err, output)
	}
}
