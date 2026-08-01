package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func (h *Handler) decode(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		h.decodeError(w, r, err)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body must contain one JSON value")
		}
		h.decodeError(w, r, err)
		return false
	}
	return true
}

func (h *Handler) decodeOptional(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); errors.Is(err, io.EOF) {
		return true
	} else if err != nil {
		h.decodeError(w, r, err)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body must contain one JSON value")
		}
		h.decodeError(w, r, err)
		return false
	}
	return true
}

func (h *Handler) decodeError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeProblem(w, r, http.StatusRequestEntityTooLarge, problemCodePayloadTooLarge,
			fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit))
		return
	}
	writeProblem(w, r, http.StatusBadRequest, problemCodeValidation, err.Error())
}

func (h *Handler) validation(w http.ResponseWriter, err error) {
	writeProblem(w, nil, http.StatusBadRequest, problemCodeValidation, err.Error())
}

func (h *Handler) respond(w http.ResponseWriter, status int, data any, err error) {
	if err != nil {
		h.writeError(w, err)
		return
	}
	write(w, status, data)
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	status, code, message := h.mapError(err)
	writeProblem(w, nil, status, code, message)
}

func write(w http.ResponseWriter, status int, data any) {
	payload, err := json.Marshal(envelope{Data: data})
	if err != nil {
		slog.Error("encode HTTP response", "error", err)
		writeProblem(w, nil, http.StatusInternalServerError, problemCodeInternal, "internal server error")
		return
	}
	writeJSON(w, status, mediaTypeJSON, payload)
}

func writeProblem(w http.ResponseWriter, request *http.Request, status int, code, message string) {
	instance := ""
	if request != nil {
		instance = request.URL.Path
	}
	payload, err := json.Marshal(problem{
		Type:     "https://blockqueue.dev/problems/" + code,
		Title:    http.StatusText(status),
		Status:   status,
		Detail:   message,
		Code:     code,
		Instance: instance,
	})
	if err != nil {
		slog.Error("encode HTTP problem response", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writeJSON(w, status, mediaTypeProblemJSON, payload)
}

func writeJSON(w http.ResponseWriter, status int, contentType string, payload []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(append(payload, '\n'))
}

func queryLimit(r *http.Request, fallback int) int {
	value, err := strconv.Atoi(r.URL.Query().Get(queryParamLimit))
	if err != nil || value <= 0 {
		return fallback
	}
	if value > maxBatch {
		return maxBatch
	}
	return value
}

func optionalDuration(raw string, positive bool) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || (positive && parsed <= 0) || (!positive && parsed < 0) {
		return 0, errors.New("invalid duration")
	}
	return parsed, nil
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
