package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteEncodesBeforeCommittingStatus(t *testing.T) {
	recorder := httptest.NewRecorder()
	write(recorder, http.StatusOK, make(chan struct{}))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != mediaTypeProblemJSON {
		t.Fatalf("content-type=%q", contentType)
	}
	var response problem
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if response.Code != problemCodeInternal {
		t.Fatalf("code=%q", response.Code)
	}
}
