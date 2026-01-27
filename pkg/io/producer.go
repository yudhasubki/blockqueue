package io

type Publish struct {
	Message string `json:"message"`
	Delay   string `json:"delay,omitempty"` // e.g. "1m", "5s"
}
