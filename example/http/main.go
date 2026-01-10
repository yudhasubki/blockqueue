package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const baseURL = "http://localhost:8080"

func main() {
	// 1. Create topic with subscriber
	createTopic()

	// 2. Publish messages
	for i := 0; i < 5; i++ {
		publishMessage(fmt.Sprintf("Message %d", i))
	}

	// 3. Read and acknowledge messages
	for i := 0; i < 5; i++ {
		readAndAckMessage()
	}
}

func createTopic() {
	payload := map[string]interface{}{
		"name": "orders",
		"subscribers": []map[string]interface{}{
			{
				"name": "order_processor",
				"option": map[string]interface{}{
					"max_attempts":        5,
					"visibility_duration": "5m",
				},
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		log.Println("Topic created successfully")
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("Create topic response: %s", respBody)
	}
}

func publishMessage(message string) {
	payload := map[string]string{
		"message": message,
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/topics/orders/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	log.Printf("Published: %s (status: %d)", message, resp.StatusCode)
}

func readAndAckMessage() {
	// Read message with 5s timeout (long-polling)
	resp, err := http.Get(baseURL + "/topics/orders/subscribers/order_processor?timeout=5s")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("No message available (status: %d)", resp.StatusCode)
		return
	}

	var messages []struct {
		Id      string `json:"id"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		log.Printf("Error decoding: %v", err)
		return
	}

	for _, msg := range messages {
		log.Printf("Received: %s", msg.Message)

		// Acknowledge message
		req, _ := http.NewRequest(
			http.MethodDelete,
			fmt.Sprintf("%s/topics/orders/subscribers/order_processor/messages/%s", baseURL, msg.Id),
			nil,
		)
		ackResp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Error acking: %v", err)
			continue
		}
		ackResp.Body.Close()

		log.Printf("Acknowledged: %s", msg.Id)
	}

	time.Sleep(100 * time.Millisecond)
}
