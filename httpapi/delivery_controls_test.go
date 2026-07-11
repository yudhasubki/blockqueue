package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/httpapi"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestAsyncPublishReturnsResolvableLocation(t *testing.T) {
	_, handler := setupHTTPQueue(t)
	publish := httptest.NewRecorder()
	handler.ServeHTTP(publish, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/messages", bytes.NewBufferString(`{"message":"async location"}`),
	))
	if publish.Code != http.StatusAccepted {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	var receipt struct {
		Data blockqueue.PublishReceipt `json:"data"`
	}
	if err := json.Unmarshal(publish.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	expected := "/v1/topics/orders/messages/" + receipt.Data.MessageID
	if location := publish.Header().Get("Location"); location != expected {
		t.Fatalf("Location=%q expected=%q", location, expected)
	}

	deadline := time.Now().Add(time.Second)
	for {
		status := httptest.NewRecorder()
		handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, expected, nil))
		if status.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Location did not become resolvable: status=%d body=%s", status.Code, status.Body.String())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHTTPDeliveryControlsAndProblemDetails(t *testing.T) {
	queue, handler := setupHTTPQueue(t)
	_ = queue

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/messages", bytes.NewBufferString(`{"unknown":true}`),
	))
	if invalid.Code != http.StatusBadRequest || invalid.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("problem status=%d content-type=%q body=%s", invalid.Code, invalid.Header().Get("Content-Type"), invalid.Body.String())
	}
	var invalidProblem struct {
		Status int    `json:"status"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal(invalid.Body.Bytes(), &invalidProblem); err != nil || invalidProblem.Code != "validation_error" {
		t.Fatalf("decode problem: %v body=%s", err, invalid.Body.String())
	}

	publish := httptest.NewRecorder()
	handler.ServeHTTP(publish, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/messages?wait_for=commit", bytes.NewBufferString(`{"message":"controls"}`),
	))
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	var published struct {
		Data blockqueue.PublishReceipt `json:"data"`
	}
	if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}

	claim := httptest.NewRecorder()
	handler.ServeHTTP(claim, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/subscribers/worker/claim?timeout=1s&limit=1", nil,
	))
	var claimed struct {
		Data blockqueue.Deliveries `json:"data"`
	}
	if err := json.Unmarshal(claim.Body.Bytes(), &claimed); err != nil || len(claimed.Data) != 1 {
		t.Fatalf("claim status=%d err=%v body=%s", claim.Code, err, claim.Body.String())
	}

	snoozeBody, _ := json.Marshal(map[string]string{
		"receipt_token": claimed.Data[0].ReceiptToken, "delay": "1ms",
	})
	snooze := httptest.NewRecorder()
	handler.ServeHTTP(snooze, httptest.NewRequest(
		http.MethodPost,
		"/v1/topics/orders/subscribers/worker/messages/"+published.Data.MessageID+"/snooze",
		bytes.NewReader(snoozeBody),
	))
	if snooze.Code != http.StatusOK {
		t.Fatalf("snooze status=%d body=%s", snooze.Code, snooze.Body.String())
	}
	time.Sleep(5 * time.Millisecond)

	claim = httptest.NewRecorder()
	handler.ServeHTTP(claim, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/subscribers/worker/claim?timeout=1s&limit=1", nil,
	))
	if err := json.Unmarshal(claim.Body.Bytes(), &claimed); err != nil || len(claimed.Data) != 1 {
		t.Fatalf("reclaim status=%d err=%v body=%s", claim.Code, err, claim.Body.String())
	}

	nackBody, _ := json.Marshal(map[string]string{
		"receipt_token": claimed.Data[0].ReceiptToken, "retry_delay": "1ms", "error": "temporary",
	})
	nack := httptest.NewRecorder()
	handler.ServeHTTP(nack, httptest.NewRequest(
		http.MethodPost,
		"/v1/topics/orders/subscribers/worker/messages/"+published.Data.MessageID+"/nack",
		bytes.NewReader(nackBody),
	))
	if nack.Code != http.StatusOK {
		t.Fatalf("nack status=%d body=%s", nack.Code, nack.Body.String())
	}

	history := httptest.NewRecorder()
	handler.ServeHTTP(history, httptest.NewRequest(
		http.MethodGet,
		"/v1/topics/orders/subscribers/worker/messages/"+published.Data.MessageID+"/errors",
		nil,
	))
	if history.Code != http.StatusOK || !bytes.Contains(history.Body.Bytes(), []byte("temporary")) {
		t.Fatalf("history status=%d body=%s", history.Code, history.Body.String())
	}

	cancel := httptest.NewRecorder()
	handler.ServeHTTP(cancel, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/messages/"+published.Data.MessageID+"/cancel",
		bytes.NewBufferString(`{"reason":"operator"}`),
	))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", cancel.Code, cancel.Body.String())
	}

	status := httptest.NewRecorder()
	handler.ServeHTTP(status, httptest.NewRequest(
		http.MethodGet, "/v1/topics/orders/messages/"+published.Data.MessageID, nil,
	))
	if status.Code != http.StatusOK || !bytes.Contains(status.Body.Bytes(), []byte("cancelled")) {
		t.Fatalf("message status=%d body=%s", status.Code, status.Body.String())
	}
}

func TestOpenAPIAndPluggableAuthentication(t *testing.T) {
	queue, _ := setupHTTPQueue(t)
	authCalls := 0
	resolverCalls := 0
	handler := httpapi.Router(queue, httpapi.Options{
		DisableUI: true,
		PrincipalResolver: func(request *http.Request) (string, error) {
			resolverCalls++
			if request.Header.Get("Authorization") == "" {
				return "", errors.New("missing credentials")
			}
			return "user-42", nil
		},
		AuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				authCalls++
				principal, ok := httpapi.PrincipalFromContext(r.Context())
				if !ok || principal != "user-42" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				next.ServeHTTP(w, r)
			})
		},
	})

	openapi := httptest.NewRecorder()
	handler.ServeHTTP(openapi, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if openapi.Code != http.StatusOK || !json.Valid(openapi.Body.Bytes()) {
		t.Fatalf("openapi status=%d body=%s", openapi.Code, openapi.Body.String())
	}
	var document struct {
		Paths map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(openapi.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/topics", "/topics/{topicName}/messages", "/topics/{topicName}/messages/batch",
		"/topics/{topicName}/messages/{messageID}", "/topics/{topicName}/messages/{messageID}/cancel",
		"/topics/{topicName}/subscribers", "/topics/{topicName}/subscribers/{subscriberName}/claim",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/ack",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/nack",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/lease",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/snooze",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/cancel",
		"/topics/{topicName}/subscribers/{subscriberName}/messages/{messageID}/errors",
		"/topics/{topicName}/subscribers/{subscriberName}/dlq",
		"/topics/{topicName}/schedules", "/topics/{topicName}/schedules/{scheduleId}/runs",
	} {
		if _, exists := document.Paths[path]; !exists {
			t.Errorf("OpenAPI path %s is missing", path)
		}
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/topics", nil))
	if unauthorized.Code != http.StatusUnauthorized || authCalls != 0 || resolverCalls != 1 {
		t.Fatalf("auth status=%d auth_calls=%d resolver_calls=%d", unauthorized.Code, authCalls, resolverCalls)
	}
	authorizedRequest := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	authorizedRequest.Header.Set("Authorization", "Bearer test")
	authorized := httptest.NewRecorder()
	handler.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusOK || authCalls != 1 || resolverCalls != 2 {
		t.Fatalf("authorized status=%d auth_calls=%d resolver_calls=%d body=%s",
			authorized.Code, authCalls, resolverCalls, authorized.Body.String())
	}
}

func TestOpenAPIRouteMethodCoverageIsBidirectional(t *testing.T) {
	queue, handler := setupHTTPQueue(t)
	_ = queue
	routes, ok := handler.(chi.Routes)
	if !ok {
		t.Fatal("HTTP router does not expose chi route metadata")
	}
	actual := make(map[string]struct{})
	if err := chi.Walk(routes, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route == "/v1" || strings.HasPrefix(route, "/v1/") {
			path := strings.TrimPrefix(route, "/v1")
			actual[strings.ToLower(method)+" "+path] = struct{}{}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	openapi := httptest.NewRecorder()
	handler.ServeHTTP(openapi, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	var document struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(openapi.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	expected := make(map[string]struct{})
	methods := map[string]struct{}{
		"get": {}, "post": {}, "put": {}, "patch": {}, "delete": {},
	}
	for path, item := range document.Paths {
		for method := range item {
			if _, isMethod := methods[method]; isMethod {
				expected[method+" "+path] = struct{}{}
			}
		}
	}

	missing := setDifference(expected, actual)
	extra := setDifference(actual, expected)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("route/OpenAPI mismatch\nmissing routes: %v\nundocumented routes: %v", missing, extra)
	}
}

func setDifference(left, right map[string]struct{}) []string {
	result := make([]string, 0)
	for item := range left {
		if _, exists := right[item]; !exists {
			result = append(result, item)
		}
	}
	slices.Sort(result)
	return result
}

func TestInvalidSubscriberOptionsUseProblemValidationResponse(t *testing.T) {
	_, handler := setupHTTPQueue(t)
	request := httptest.NewRecorder()
	handler.ServeHTTP(request, httptest.NewRequest(
		http.MethodPost, "/v1/topics",
		bytes.NewBufferString(`{"name":"invalid-options","subscribers":[{"name":"worker","option":{"visibility_duration":"13h"}}]}`),
	))
	if request.Code != http.StatusBadRequest || request.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("invalid subscriber status=%d content-type=%q body=%s",
			request.Code, request.Header().Get("Content-Type"), request.Body.String())
	}
}

func TestPostgreSQLHTTPDeliveryControlsE2E(t *testing.T) {
	postgresURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
	if postgresURL == "" {
		t.Skip("set BLOCKQUEUE_TEST_POSTGRES_URL to run the PostgreSQL HTTP contract")
	}
	schema, err := testdb.OpenPostgreSQLSchema(
		context.Background(), postgresURL, "_test", "blockqueue_http_controls",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := schema.Close(); err != nil {
			t.Error(err)
		}
	})
	_, handler := setupHTTPQueueWithDriver(t, schema.Driver)

	publish := httptest.NewRecorder()
	handler.ServeHTTP(publish, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/messages?wait_for=commit",
		bytes.NewBufferString(`{"message":"postgres-http"}`),
	))
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", publish.Code, publish.Body.String())
	}
	var published struct {
		Data blockqueue.PublishReceipt `json:"data"`
	}
	if err := json.Unmarshal(publish.Body.Bytes(), &published); err != nil {
		t.Fatal(err)
	}

	claim := httptest.NewRecorder()
	handler.ServeHTTP(claim, httptest.NewRequest(
		http.MethodPost, "/v1/topics/orders/subscribers/worker/claim?timeout=1s&limit=1", nil,
	))
	var claimed struct {
		Data blockqueue.Deliveries `json:"data"`
	}
	if err := json.Unmarshal(claim.Body.Bytes(), &claimed); err != nil || len(claimed.Data) != 1 {
		t.Fatalf("claim status=%d err=%v body=%s", claim.Code, err, claim.Body.String())
	}
	nackBody, _ := json.Marshal(map[string]string{
		"receipt_token": claimed.Data[0].ReceiptToken,
		"retry_delay":   "1ms",
		"error":         "postgres adapter failure",
	})
	nack := httptest.NewRecorder()
	handler.ServeHTTP(nack, httptest.NewRequest(
		http.MethodPost,
		"/v1/topics/orders/subscribers/worker/messages/"+published.Data.MessageID+"/nack",
		bytes.NewReader(nackBody),
	))
	if nack.Code != http.StatusOK {
		t.Fatalf("nack status=%d body=%s", nack.Code, nack.Body.String())
	}

	history := httptest.NewRecorder()
	handler.ServeHTTP(history, httptest.NewRequest(
		http.MethodGet,
		"/v1/topics/orders/subscribers/worker/messages/"+published.Data.MessageID+"/errors", nil,
	))
	if history.Code != http.StatusOK || !bytes.Contains(history.Body.Bytes(), []byte("postgres adapter failure")) {
		t.Fatalf("history status=%d body=%s", history.Code, history.Body.String())
	}
}

func setupHTTPQueue(t *testing.T) (*blockqueue.Queue, http.Handler) {
	t.Helper()
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "http-controls.db"), sqlite.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return setupHTTPQueueWithDriver(t, driver)
}

func setupHTTPQueueWithDriver(t *testing.T, driver store.Driver) (*blockqueue.Queue, http.Handler) {
	t.Helper()
	queue := blockqueue.New(driver, blockqueue.Options{DisableMetrics: true})
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
	return queue, httpapi.Router(queue, httpapi.Options{DisableUI: true})
}
