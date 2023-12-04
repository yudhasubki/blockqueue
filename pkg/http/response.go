package http

import (
	"encoding/json"
	"net/http"
)

const (
	MessageFailure    string = "failure"
	MessageNotFound   string = "not found"
	MessageNoJobFound string = "no job found"
	MessageSuccess    string = "success"
)

type Response struct {
	Message  string      `json:"message"`
	Data     interface{} `json:"data,omitempty"`
	Error    string      `json:"error,omitempty"`
	Metadata interface{} `json:"metadata,omitempty"`
}

func Write(w http.ResponseWriter, httpcode int, r *Response) {
	js, _ := json.Marshal(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpcode)
	w.Write(js)
}
