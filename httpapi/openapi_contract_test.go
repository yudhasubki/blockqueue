package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAPIRequestAndResponseSchemas(t *testing.T) {
	_, handler := setupHTTPQueue(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("openapi status=%d body=%s", response.Code, response.Body.String())
	}
	if err := rejectDuplicateJSONKeys(response.Body.Bytes()); err != nil {
		t.Fatal(err)
	}

	type operation struct {
		OperationID string                     `json:"operationId"`
		RequestBody json.RawMessage            `json:"requestBody"`
		Responses   map[string]json.RawMessage `json:"responses"`
	}
	type media struct {
		Schema json.RawMessage `json:"schema"`
	}
	type responseObject struct {
		Reference string           `json:"$ref"`
		Content   map[string]media `json:"content"`
	}
	var document struct {
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Responses map[string]responseObject `json:"responses"`
		} `json:"components"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}

	expectsBody := map[string]struct{}{
		"createTopic": {}, "publishMessage": {}, "publishMessageBatch": {},
		"cancelMessage": {}, "createSubscribers": {}, "ackDelivery": {},
		"nackDelivery": {}, "extendLease": {}, "snoozeDelivery": {},
		"cancelDelivery": {}, "batchAckDeliveries": {}, "batchNackDeliveries": {},
		"replayDeadLetters": {}, "createSchedule": {}, "updateSchedule": {},
	}
	seenBodies := make(map[string]struct{}, len(expectsBody))
	methods := map[string]struct{}{
		"get": {}, "post": {}, "put": {}, "patch": {}, "delete": {},
	}
	for path, item := range document.Paths {
		for method, raw := range item {
			if _, ok := methods[method]; !ok {
				continue
			}
			var operation operation
			if err := json.Unmarshal(raw, &operation); err != nil {
				t.Fatalf("decode %s %s: %v", method, path, err)
			}
			_, shouldHaveBody := expectsBody[operation.OperationID]
			hasBody := len(operation.RequestBody) > 0 && string(operation.RequestBody) != "null"
			if hasBody != shouldHaveBody {
				t.Errorf("%s %s requestBody=%v, want %v", method, path, hasBody, shouldHaveBody)
			}
			if hasBody {
				seenBodies[operation.OperationID] = struct{}{}
			}
			for status, rawResponse := range operation.Responses {
				if !strings.HasPrefix(status, "2") || status == "204" {
					continue
				}
				var declared responseObject
				if err := json.Unmarshal(rawResponse, &declared); err != nil {
					t.Fatalf("decode %s %s response %s: %v", method, path, status, err)
				}
				if declared.Reference != "" {
					name := strings.TrimPrefix(declared.Reference, "#/components/responses/")
					var exists bool
					declared, exists = document.Components.Responses[name]
					if !exists {
						t.Errorf("%s %s response %s references missing %s", method, path, status, name)
						continue
					}
				}
				jsonMedia, exists := declared.Content["application/json"]
				if !exists || len(jsonMedia.Schema) == 0 {
					t.Errorf("%s %s response %s has no application/json schema", method, path, status)
				}
			}
		}
	}
	if len(seenBodies) != len(expectsBody) {
		t.Errorf("documented request bodies=%d, want %d", len(seenBodies), len(expectsBody))
	}

	var rawDocument any
	if err := json.Unmarshal(response.Body.Bytes(), &rawDocument); err != nil {
		t.Fatal(err)
	}
	if err := validateLocalReferences(rawDocument, rawDocument); err != nil {
		t.Fatal(err)
	}
}

func rejectDuplicateJSONKeys(document []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := consumeJSONValue(decoder, "$"); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("openapi has trailing JSON: %v", err)
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode object key at %s: %w", path, err)
			}
			key := keyToken.(string)
			if _, exists := seen[key]; exists {
				return fmt.Errorf("openapi has duplicate key %s.%s", path, key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, path+"."+key); err != nil {
				return err
			}
		}
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := consumeJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q at %s", delimiter, path)
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

func validateLocalReferences(root, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		if reference, ok := typed["$ref"].(string); ok && strings.HasPrefix(reference, "#/") {
			current := root
			for _, segment := range strings.Split(strings.TrimPrefix(reference, "#/"), "/") {
				segment = strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")
				object, ok := current.(map[string]any)
				if !ok {
					return fmt.Errorf("openapi reference %s traverses a non-object", reference)
				}
				current, ok = object[segment]
				if !ok {
					return fmt.Errorf("openapi reference %s does not resolve", reference)
				}
			}
		}
		for _, child := range typed {
			if err := validateLocalReferences(root, child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := validateLocalReferences(root, child); err != nil {
				return err
			}
		}
	}
	return nil
}
