package blockqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	httpresponse "github.com/yudhasubki/blockqueue/pkg/http"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

func setupE2E(t *testing.T) (*httptest.Server, *BlockQueue[chan bqio.ResponseMessages]) {
	dbName := fmt.Sprintf("e2e_%d.db", time.Now().UnixNano())
	db, err := sqlite.New(dbName, sqlite.Config{BusyTimeout: 5000})
	require.NoError(t, err)

	// Run migrations
	runTestMigrate(t, db)

	bq := New(db, BlockQueueOption{
		WriteBufferConfig: WriteBufferConfig{
			BatchSize:     10,
			FlushInterval: 10 * time.Millisecond,
			BufferSize:    100,
		},
	})

	err = bq.Run(context.Background())
	require.NoError(t, err)

	handler := &Http{
		Stream: bq,
	}

	server := httptest.NewServer(handler.Router())

	t.Cleanup(func() {
		server.Close()
		bq.Close()
		db.Database.Close()
		os.Remove(dbName)
		os.Remove(dbName + "-shm")
		os.Remove(dbName + "-wal")
	})

	return server, bq
}

func runTestMigrate(t *testing.T, db *sqlite.SQLite) {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS topics (
			id VARCHAR(36) PRIMARY KEY,
			name VARCHAR(255) NOT NULL UNIQUE,
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS topic_subscribers (
			id VARCHAR(36) PRIMARY KEY,
			topic_id VARCHAR(36) NOT NULL,
			name VARCHAR(255) NOT NULL,
			option TEXT DEFAULT '{}',
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS topic_messages (
			id VARCHAR(36) PRIMARY KEY,
			topic_id VARCHAR(36) NOT NULL,
			message TEXT NOT NULL,
			status VARCHAR(15) DEFAULT 'pending',
			deleted_at DATETIME DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS subscriber_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			subscriber_id VARCHAR(36) NOT NULL,
			topic_id VARCHAR(36) NOT NULL,
			message_id VARCHAR(50) NOT NULL,
			message TEXT NOT NULL,
			status VARCHAR(15) DEFAULT 'pending',
			retry_count INTEGER DEFAULT 0,
			visible_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (subscriber_id) REFERENCES topic_subscribers(id) ON DELETE CASCADE,
			FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_poll ON subscriber_messages(subscriber_id, status, visible_at)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_ack ON subscriber_messages(message_id, subscriber_id)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_topic ON subscriber_messages(topic_id)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriber_messages_pending ON subscriber_messages(subscriber_id, visible_at) WHERE status = 'pending'`,
	}

	for _, migration := range migrations {
		_, err := db.Database.Exec(migration)
		require.NoError(t, err)
	}
}

func TestE2E_StandardLifecycle(t *testing.T) {
	server, _ := setupE2E(t)
	client := server.Client()

	topicName := "orders"
	subName := "processor"

	// 1. Create Topic
	t.Log("Creating Topic...")
	createTopicBody := bqio.Topic{
		Name: topicName,
		Subscribers: bqio.Subscribers{
			{
				Name: subName,
				Option: bqio.SubscriberOpt{
					MaxAttempts:        3,
					VisibilityDuration: "5m", // Long enough to not reappear
				},
			},
		},
	}
	sendRequest(t, client, "POST", server.URL+"/topics", createTopicBody, http.StatusOK)

	// 2. Publish Message
	t.Log("Publishing Message...")
	msgContent := "order-123"
	publishBody := bqio.Publish{Message: msgContent}
	sendRequest(t, client, "POST", server.URL+"/topics/"+topicName+"/messages", publishBody, http.StatusOK)

	// Wait for WriteBuffer flush
	time.Sleep(50 * time.Millisecond)

	// 3. Consume Message
	t.Log("Consuming Message...")
	resp := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)

	var readResp httpresponse.Response
	json.NewDecoder(resp.Body).Decode(&readResp)

	dataBytes, _ := json.Marshal(readResp.Data)
	var messages []bqio.ResponseMessage
	json.Unmarshal(dataBytes, &messages)

	require.Len(t, messages, 1)
	require.Equal(t, msgContent, messages[0].Message)
	msgId := messages[0].Id
	require.NotEmpty(t, msgId)

	// 4. Ack Message
	t.Log("Acking Message...")
	req, _ := http.NewRequest("DELETE", server.URL+"/topics/"+topicName+"/subscribers/"+subName+"/messages/"+msgId, nil)
	doRequest(t, client, req, http.StatusOK)

	// 5. Verify Empty
	t.Log("Verifying Empty...")
	respEmpty := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)
	var readRespEmpty httpresponse.Response
	json.NewDecoder(respEmpty.Body).Decode(&readRespEmpty)
	dataBytesEmpty, _ := json.Marshal(readRespEmpty.Data)
	var messagesEmpty []bqio.ResponseMessage
	json.Unmarshal(dataBytesEmpty, &messagesEmpty)
	require.Empty(t, messagesEmpty)
}

func TestE2E_DLQ(t *testing.T) {
	server, _ := setupE2E(t)
	client := server.Client()

	topicName := "fail-topic"
	subName := "fail-sub"

	// 1. Create Topic with short visibility and 1 attempt
	createTopicBody := bqio.Topic{
		Name: topicName,
		Subscribers: bqio.Subscribers{
			{
				Name: subName,
				Option: bqio.SubscriberOpt{
					MaxAttempts:        1,
					VisibilityDuration: "1s",
				},
			},
		},
	}
	sendRequest(t, client, "POST", server.URL+"/topics", createTopicBody, http.StatusOK)

	// 2. Publish Message
	publishBody := bqio.Publish{Message: "fail-msg"}
	sendRequest(t, client, "POST", server.URL+"/topics/"+topicName+"/messages", publishBody, http.StatusOK)
	time.Sleep(500 * time.Millisecond)

	// 3. Consume (Attempt 1)
	resp := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)
	var readResp httpresponse.Response
	json.NewDecoder(resp.Body).Decode(&readResp)
	dataBytes, _ := json.Marshal(readResp.Data)
	var messages []bqio.ResponseMessage
	json.Unmarshal(dataBytes, &messages)
	require.Len(t, messages, 1)
	msgId := messages[0].Id

	// 4. Do NOT Ack. Wait for visibility (1s) + buffer
	time.Sleep(2 * time.Second)

	// 5. Check DLQ
	respDLQ := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"/dlq?limit=10&offset=0", http.StatusOK)
	var dlqResp httpresponse.Response
	json.NewDecoder(respDLQ.Body).Decode(&dlqResp)
	dlqBytes, _ := json.Marshal(dlqResp.Data)
	var dlqMessages []bqio.ResponseMessage
	json.Unmarshal(dlqBytes, &dlqMessages)

	require.Len(t, dlqMessages, 1)
	require.Equal(t, "fail-msg", dlqMessages[0].Message)

	// 6. Replay
	reqReplay, _ := http.NewRequest("POST", server.URL+"/topics/"+topicName+"/subscribers/"+subName+"/dlq/"+msgId+"/replay", nil)
	doRequest(t, client, reqReplay, http.StatusOK)
	time.Sleep(1 * time.Second)

	// 7. Consume again
	respReplay := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)
	json.NewDecoder(respReplay.Body).Decode(&readResp)
	dataBytesReplay, _ := json.Marshal(readResp.Data)
	json.Unmarshal(dataBytesReplay, &messages)
	require.Len(t, messages, 1)
	require.Equal(t, "fail-msg", messages[0].Message)
}

func TestE2E_Batch(t *testing.T) {
	server, _ := setupE2E(t)
	client := server.Client()
	topicName := "batch-topic"
	subName := "batch-sub"

	createTopicBody := bqio.Topic{
		Name:        topicName,
		Subscribers: bqio.Subscribers{{Name: subName, Option: bqio.SubscriberOpt{MaxAttempts: 3, VisibilityDuration: "5m"}}},
	}
	sendRequest(t, client, "POST", server.URL+"/topics", createTopicBody, http.StatusOK)

	// Batch Publish
	batchReq := []bqio.Publish{
		{Message: "msg-1"},
		{Message: "msg-2"},
		{Message: "msg-3"},
	}
	sendRequest(t, client, "POST", server.URL+"/topics/"+topicName+"/messages/batch", batchReq, http.StatusOK)
	time.Sleep(500 * time.Millisecond)

	// Consume (loop until getting all)
	var allMessages []bqio.ResponseMessage
	for len(allMessages) < 3 {
		resp := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)
		var readResp httpresponse.Response
		json.NewDecoder(resp.Body).Decode(&readResp)
		dataBytes, _ := json.Marshal(readResp.Data)
		var msgs []bqio.ResponseMessage
		json.Unmarshal(dataBytes, &msgs)
		allMessages = append(allMessages, msgs...)
		if len(msgs) == 0 {
			break
		}
	}
	require.Len(t, allMessages, 3)

	// Batch Ack
	ids := make([]string, len(allMessages))
	for i, m := range allMessages {
		ids[i] = m.Id
	}
	sendRequest(t, client, "DELETE", server.URL+"/topics/"+topicName+"/subscribers/"+subName+"/messages/batch", ids, http.StatusOK)

	// Verify empty
	respEmpty := getRequest(t, client, server.URL+"/topics/"+topicName+"/subscribers/"+subName+"?timeout=1s", http.StatusOK)
	var readRespEmpty httpresponse.Response
	json.NewDecoder(respEmpty.Body).Decode(&readRespEmpty)
	dataBytesEmpty, _ := json.Marshal(readRespEmpty.Data)
	var messagesEmpty []bqio.ResponseMessage
	json.Unmarshal(dataBytesEmpty, &messagesEmpty)
	require.Empty(t, messagesEmpty)
}

func sendRequest(t *testing.T, client *http.Client, method, url string, body interface{}, expectCode int) *http.Response {
	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return doRequest(t, client, req, expectCode)
}

func getRequest(t *testing.T, client *http.Client, url string, expectCode int) *http.Response {
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)
	return doRequest(t, client, req, expectCode)
}

func doRequest(t *testing.T, client *http.Client, req *http.Request, expectCode int) *http.Response {
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, expectCode, resp.StatusCode)
	return resp
}
