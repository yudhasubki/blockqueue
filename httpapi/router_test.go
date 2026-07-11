package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/httpapi"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestRouterPublishClaimAckAndStrictJSON(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "http.db"), sqlite.Config{})
	if err != nil {
		t.Fatal(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{})
	if err := queue.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	topic := blockqueue.NewTopic("orders")
	subscriber := blockqueue.NewSubscriber(topic, "worker", blockqueue.SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	if err := queue.CreateTopic(context.Background(), topic, blockqueue.Subscribers{subscriber}); err != nil {
		t.Fatal(err)
	}
	handler := httpapi.Router(queue, httpapi.Options{DisableUI: true})

	removedVersion := httptest.NewRequest(http.MethodGet, "/v2/topics", nil)
	removedVersionRecorder := httptest.NewRecorder()
	handler.ServeHTTP(removedVersionRecorder, removedVersion)
	if removedVersionRecorder.Code != http.StatusNotFound {
		t.Fatalf("removed /v2 route status=%d", removedVersionRecorder.Code)
	}

	invalidTimeout := httptest.NewRequest(http.MethodPost,
		"/v1/topics/orders/subscribers/worker/claim?timeout=not-a-duration", nil)
	invalidTimeoutRecorder := httptest.NewRecorder()
	handler.ServeHTTP(invalidTimeoutRecorder, invalidTimeout)
	if invalidTimeoutRecorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid timeout status=%d body=%s", invalidTimeoutRecorder.Code, invalidTimeoutRecorder.Body.String())
	}
	oversizedLease := httptest.NewRequest(http.MethodPost,
		"/v1/topics/orders/subscribers/worker/claim?timeout=1s&lease=13h", nil)
	oversizedLeaseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(oversizedLeaseRecorder, oversizedLease)
	if oversizedLeaseRecorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized lease status=%d body=%s", oversizedLeaseRecorder.Code, oversizedLeaseRecorder.Body.String())
	}

	unknown := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/messages", bytes.NewBufferString(`{"message":"x","unknown":true}`))
	unknownRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unknownRecorder, unknown)
	if unknownRecorder.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", unknownRecorder.Code, unknownRecorder.Body.String())
	}

	publish := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/messages?wait_for=commit", bytes.NewBufferString(`{"message":"http","idempotency_key":"http-1"}`))
	publishRecorder := httptest.NewRecorder()
	handler.ServeHTTP(publishRecorder, publish)
	if publishRecorder.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", publishRecorder.Code, publishRecorder.Body.String())
	}

	claim := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/subscribers/worker/claim?timeout=1s&limit=1", nil)
	claimRecorder := httptest.NewRecorder()
	handler.ServeHTTP(claimRecorder, claim)
	if claimRecorder.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", claimRecorder.Code, claimRecorder.Body.String())
	}
	var claimed struct {
		Data blockqueue.Deliveries `json:"data"`
	}
	if err := json.Unmarshal(claimRecorder.Body.Bytes(), &claimed); err != nil || len(claimed.Data) != 1 {
		t.Fatalf("decode claim: %v body=%s", err, claimRecorder.Body.String())
	}
	body, _ := json.Marshal(map[string]string{"receipt_token": claimed.Data[0].ReceiptToken})
	ack := httptest.NewRequest(http.MethodPost,
		"/v1/topics/orders/subscribers/worker/messages/"+claimed.Data[0].ID+"/ack", bytes.NewReader(body))
	ackRecorder := httptest.NewRecorder()
	handler.ServeHTTP(ackRecorder, ack)
	if ackRecorder.Code != http.StatusOK {
		t.Fatalf("ack status=%d body=%s", ackRecorder.Code, ackRecorder.Body.String())
	}
}

func TestRouterServesEmbeddedDashboard(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "dashboard.db"), sqlite.Config{})
	if err != nil {
		t.Fatal(err)
	}
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
	if err := queue.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = queue.Shutdown(ctx)
	})
	handler := httpapi.Router(queue, httpapi.Options{})
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte("BlockQueue")) {
		t.Fatalf("dashboard status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
