package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/httpapi"
	"github.com/yudhasubki/blockqueue/internal/testdb"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type e2eBackend struct {
	name    string
	enabled bool
	open    func(*testing.T) store.Driver
}

func TestHTTPRealTCPEndToEnd(t *testing.T) {
	postgresURL := os.Getenv("BLOCKQUEUE_TEST_POSTGRES_URL")
	backends := []e2eBackend{
		{
			name:    "sqlite",
			enabled: true,
			open: func(t *testing.T) store.Driver {
				driver, err := sqlite.Open(filepath.Join(t.TempDir(), "http-e2e.db"), sqlite.Config{})
				require.NoError(t, err)
				return driver
			},
		},
		{
			name:    "postgres",
			enabled: postgresURL != "",
			open: func(t *testing.T) store.Driver {
				schema, err := testdb.OpenPostgreSQLSchema(
					context.Background(), postgresURL, "_test", "blockqueue_http_e2e",
				)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, schema.Close()) })
				return schema.Driver
			},
		},
	}

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			if !backend.enabled {
				t.Skip("set BLOCKQUEUE_TEST_POSTGRES_URL to run PostgreSQL HTTP E2E")
			}
			runHTTPRealTCPEndToEnd(t, backend.open(t))
		})
	}
}

func runHTTPRealTCPEndToEnd(t *testing.T, driver store.Driver) {
	t.Helper()
	queue := blockqueue.New(driver, blockqueue.Options{})
	require.NoError(t, queue.Run(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, queue.Shutdown(ctx))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{
		Handler:           httpapi.Router(queue, httpapi.Options{DisableUI: true}),
		ReadHeaderTimeout: 2 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
		require.ErrorIs(t, <-serveResult, http.ErrServerClosed)
	})

	client := &http.Client{Timeout: 3 * time.Second}
	baseURL := "http://" + listener.Addr().String()
	requireHTTPStatus(t, client, http.MethodGet, baseURL+"/readyz", nil, http.StatusOK, nil)
	requireHTTPStatus(t, client, http.MethodPost, baseURL+"/v1/topics", []byte(`{
		"name":"orders",
		"subscribers":[{"name":"worker","option":{"max_attempts":3,"visibility_duration":"1s","dequeue_batch_size":10}}]
	}`), http.StatusCreated, nil)

	var published struct {
		Data blockqueue.PublishReceipt `json:"data"`
	}
	requireHTTPStatus(t, client, http.MethodPost,
		baseURL+"/v1/topics/orders/messages?wait_for=commit",
		[]byte(`{"message":"e2e","headers":{"source":"http"},"idempotency_key":"http-e2e"}`),
		http.StatusOK, &published)
	require.Equal(t, "persisted", published.Data.State)
	require.NotEmpty(t, published.Data.MessageID)

	var claimed struct {
		Data blockqueue.Deliveries `json:"data"`
	}
	requireHTTPStatus(t, client, http.MethodPost,
		baseURL+"/v1/topics/orders/subscribers/worker/claim?timeout=1s&limit=1&lease=1s",
		nil, http.StatusOK, &claimed)
	require.Len(t, claimed.Data, 1)
	require.Equal(t, published.Data.MessageID, claimed.Data[0].ID)
	require.NotEmpty(t, claimed.Data[0].ReceiptToken)

	leaseBody, err := json.Marshal(map[string]string{
		"receipt_token": claimed.Data[0].ReceiptToken,
		"extension":     "1s",
	})
	require.NoError(t, err)
	requireHTTPStatus(t, client, http.MethodPost,
		baseURL+"/v1/topics/orders/subscribers/worker/messages/"+claimed.Data[0].ID+"/lease",
		leaseBody, http.StatusOK, nil)

	ackBody, err := json.Marshal(map[string]string{"receipt_token": claimed.Data[0].ReceiptToken})
	require.NoError(t, err)
	requireHTTPStatus(t, client, http.MethodPost,
		baseURL+"/v1/topics/orders/subscribers/worker/messages/"+claimed.Data[0].ID+"/ack",
		ackBody, http.StatusOK, nil)
}

func requireHTTPStatus(
	t *testing.T,
	client *http.Client,
	method, target string,
	body []byte,
	wantStatus int,
	decode any,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, target, bytes.NewReader(body))
	require.NoError(t, err)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, wantStatus, response.StatusCode, string(responseBody))
	if decode != nil {
		require.NoError(t, json.Unmarshal(responseBody, decode), string(responseBody))
	}
}
