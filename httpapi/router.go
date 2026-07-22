package httpapi

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/yudhasubki/blockqueue"
)

type principalContextKey struct{}

// PrincipalResolver authenticates a request and returns the stable subject
// exposed to downstream authorization middleware and handlers. A nil resolver
// keeps the HTTP package authentication-neutral.
type PrincipalResolver func(*http.Request) (string, error)

// PrincipalFromContext returns the subject installed by PrincipalResolver.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(string)
	return principal, ok && principal != ""
}

// Options configures API routing, dashboard serving, and authentication hooks.
type Options struct {
	// Prefix defaults to /v1. Handler.Attach can be used directly when the
	// embedding application wants to own all routing decisions.
	Prefix    string
	UIPath    string
	DisableUI bool
	// AuthMiddleware is applied only to the versioned API. Health checks,
	// OpenAPI, and static UI remain available for the embedding application to
	// protect at a wider router boundary when desired.
	AuthMiddleware func(http.Handler) http.Handler
	// PrincipalResolver is optional. When set, resolution failure or an empty
	// subject returns an RFC 9457 401 response before AuthMiddleware runs.
	PrincipalResolver PrincipalResolver
}

// Router builds the health, OpenAPI, dashboard, and versioned API routes.
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
	router.Group(func(api chi.Router) {
		if options.PrincipalResolver != nil {
			api.Use(principalMiddleware(options.PrincipalResolver))
		}
		if options.AuthMiddleware != nil {
			api.Use(options.AuthMiddleware)
		}
		api.Route(prefix, handler.Attach)
	})
	router.Get("/openapi.json", serveOpenAPI)
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

func principalMiddleware(resolve PrincipalResolver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			principal, err := resolve(request)
			principal = strings.TrimSpace(principal)
			if err != nil || principal == "" {
				writeProblem(w, request, http.StatusUnauthorized, problemCodeUnauthorized, "authentication failed")
				return
			}
			ctx := context.WithValue(request.Context(), principalContextKey{}, principal)
			next.ServeHTTP(w, request.WithContext(ctx))
		})
	}
}

func queueErrorMapper(queue *blockqueue.Queue) ErrorMapper {
	return func(err error) (int, string, string) {
		status := http.StatusInternalServerError
		code := problemCodeInternal
		message := "internal server error"
		switch {
		case errors.Is(err, blockqueue.ErrInvalidPublish), errors.Is(err, blockqueue.ErrInvalidCursor),
			errors.Is(err, blockqueue.ErrInvalidReceipt),
			errors.Is(err, blockqueue.ErrInvalidTopic), errors.Is(err, blockqueue.ErrInvalidSubscriber):
			status, code, message = http.StatusBadRequest, problemCodeValidation, err.Error()
		case errors.Is(err, blockqueue.ErrTopicNotFound), errors.Is(err, blockqueue.ErrSubscriberNotFound),
			errors.Is(err, blockqueue.ErrSubscriberDeleted), errors.Is(err, blockqueue.ErrDeliveryNotFound),
			errors.Is(err, blockqueue.ErrScheduleNotFound):
			status, code, message = http.StatusNotFound, problemCodeResourceNotFound, err.Error()
		case errors.Is(err, blockqueue.ErrLeaseLost), errors.Is(err, blockqueue.ErrIdempotencyConflict),
			errors.Is(err, blockqueue.ErrNoActiveSubscriber), errors.Is(err, blockqueue.ErrScheduleVersion),
			errors.Is(err, blockqueue.ErrScheduleOverlap), errors.Is(err, blockqueue.ErrResourcePaused),
			errors.Is(err, blockqueue.ErrScheduleLeaseLost), errors.Is(err, blockqueue.ErrResourceConflict),
			errors.Is(err, blockqueue.ErrDeliveryTerminal):
			status, code, message = http.StatusConflict, problemCodeConflict, err.Error()
		case errors.Is(err, blockqueue.ErrPendingBudgetExceeded):
			status, code, message = http.StatusTooManyRequests, problemCodeBufferPressure, err.Error()
			if !queue.WriterHealthy() {
				status = http.StatusServiceUnavailable
				code = problemCodeWriterUnhealthy
			}
		case errors.Is(err, blockqueue.ErrQueueNotRunning), errors.Is(err, blockqueue.ErrQueueStopping),
			errors.Is(err, blockqueue.ErrWriterClosed), errors.Is(err, blockqueue.ErrWriterDrainTimeout),
			errors.Is(err, blockqueue.ErrCommitUnknown):
			status, code, message = http.StatusServiceUnavailable, problemCodeServiceUnavailable, err.Error()
		}
		return status, code, message
	}
}
