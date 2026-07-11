package httpapi

import (
	"errors"
	"net/http"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/yudhasubki/blockqueue"
)

type Options struct {
	// Prefix defaults to /v1. Handler.Attach can be used directly when the
	// embedding application wants to own all routing decisions.
	Prefix    string
	UIPath    string
	DisableUI bool
}

func Router(queue *blockqueue.Queue, options Options) http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)
	router.Get("/livez", func(w http.ResponseWriter, _ *http.Request) {
		if !queue.Live() {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	router.Get("/readyz", func(w http.ResponseWriter, request *http.Request) {
		if !queue.Ready(request.Context()) {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	prefix := options.Prefix
	if prefix == "" {
		prefix = "/v1"
	}
	handler := New(queue, queueErrorMapper(queue))
	router.Route(prefix, handler.Attach)
	if !options.DisableUI {
		var fileServer http.Handler
		if options.UIPath == "" {
			fileServer = http.FileServer(http.FS(blockqueue.DashboardFS()))
		} else {
			fileServer = http.FileServer(http.Dir(filepath.Clean(options.UIPath)))
		}
		router.Handle("/*", fileServer)
	}
	return router
}

func queueErrorMapper(queue *blockqueue.Queue) ErrorMapper {
	return func(err error) (int, string) {
		status := http.StatusInternalServerError
		message := "internal server error"
		switch {
		case errors.Is(err, blockqueue.ErrInvalidPublish), errors.Is(err, blockqueue.ErrInvalidReceipt),
			errors.Is(err, blockqueue.ErrInvalidTopic), errors.Is(err, blockqueue.ErrInvalidSubscriber):
			status, message = http.StatusBadRequest, err.Error()
		case errors.Is(err, blockqueue.ErrTopicNotFound), errors.Is(err, blockqueue.ErrSubscriberNotFound),
			errors.Is(err, blockqueue.ErrSubscriberDeleted), errors.Is(err, blockqueue.ErrDeliveryNotFound),
			errors.Is(err, blockqueue.ErrScheduleNotFound):
			status, message = http.StatusNotFound, err.Error()
		case errors.Is(err, blockqueue.ErrLeaseLost), errors.Is(err, blockqueue.ErrIdempotencyConflict),
			errors.Is(err, blockqueue.ErrNoActiveSubscriber), errors.Is(err, blockqueue.ErrScheduleVersion),
			errors.Is(err, blockqueue.ErrScheduleOverlap), errors.Is(err, blockqueue.ErrResourcePaused),
			errors.Is(err, blockqueue.ErrScheduleLeaseLost), errors.Is(err, blockqueue.ErrResourceConflict):
			status, message = http.StatusConflict, err.Error()
		case errors.Is(err, blockqueue.ErrPendingBudgetExceeded):
			status, message = http.StatusTooManyRequests, err.Error()
			if !queue.WriterHealthy() {
				status = http.StatusServiceUnavailable
			}
		case errors.Is(err, blockqueue.ErrQueueNotRunning), errors.Is(err, blockqueue.ErrQueueStopping),
			errors.Is(err, blockqueue.ErrWriterClosed), errors.Is(err, blockqueue.ErrWriterDrainTimeout),
			errors.Is(err, blockqueue.ErrCommitUnknown):
			status, message = http.StatusServiceUnavailable, err.Error()
		}
		return status, message
	}
}
