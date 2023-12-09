package blockqueue

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/queuestream/pkg/core"
	httpresponse "github.com/yudhasubki/queuestream/pkg/http"
	"github.com/yudhasubki/queuestream/pkg/io"
)

type Http struct {
	Stream *BlockQueue[chan io.ResponseMessages]
}

type ctxKeyTopicName string

const (
	topicIdKey ctxKeyTopicName = "topic"
)

func (h *Http) Router() http.Handler {
	r := chi.NewRouter()

	r.Route("/topics", func(r chi.Router) {
		r.Post("/", h.CreateTopic)

		r.Group(func(r chi.Router) {
			r.Use(h.topicExist)
			r.Delete("/{topicName}", h.DeleteTopic)
			r.Post("/{topicName}/messages", h.Publish)

			r.Get("/{topicName}/subscribers", h.GetSubscribers)
			r.Post("/{topicName}/subscribers", h.CreateSubscriber)
			r.Delete("/{topicName}/subscribers/{subscriberName}", h.DeleteSubscriber)
			r.Get("/{topicName}/subscribers/{subscriberName}", h.ReadSubscriber)
			r.Delete("/{topicName}/subscribers/{subscriberName}/messages/{messageId}", h.AckMessage)
		})
	})

	return r
}

func (h *Http) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var request io.Topic

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		slog.Error("[CreateTopic] error decode message", "error", err)
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	topics, err := GetTopics(r.Context(), core.FilterTopic{
		Name: []string{request.Name},
	})
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	if len(topics) > 0 {
		httpresponse.Write(w, http.StatusConflict, &httpresponse.Response{
			Error:   "topic was exist",
			Message: http.StatusText(http.StatusConflict),
		})
		return
	}

	var (
		topic       = request.Topic()
		subscribers = request.Subscriber(topic.Id)
	)

	err = h.Stream.AddJob(r.Context(), topic, subscribers)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := h.getTopic(r.Context())

	err := h.Stream.DeleteJob(topic)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) DeleteSubscriber(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
	)

	err := h.Stream.DeleteSubscriber(r.Context(), topic, subscriber)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}
	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) Publish(w http.ResponseWriter, r *http.Request) {
	var (
		topic   = h.getTopic(r.Context())
		request io.Publish
	)

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		slog.Error("[Publish][json.NewDecoder] error decode message", "error", err)
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	err = h.Stream.Publish(r.Context(), topic, request)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

// GetSubscribers is endpoint to get metadata of subscribers before it claimed to consumer bucket
func (h *Http) GetSubscribers(w http.ResponseWriter, r *http.Request) {

}

func (h *Http) CreateSubscriber(w http.ResponseWriter, r *http.Request) {
	var (
		request io.Subscribers
		topic   = h.getTopic(r.Context())
	)

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	subscribers := request.Subscriber(topic.Id)
	err = Tx(r.Context(), func(ctx context.Context, tx *sqlx.Tx) error {
		return CreateTxSubscribers(ctx, tx, subscribers)
	})
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	err = h.Stream.AddSubscribers(r.Context(), topic)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) ReadSubscriber(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
		timeout    = r.URL.Query().Get("timeout")
	)

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
	}

	ctx, cancel := context.WithTimeout(r.Context(), duration)
	defer cancel()

	messages, err := h.Stream.ReadSubscriber(ctx, topic, subscriber)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Data:    messages,
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) AckMessage(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
		messageId  = chi.URLParam(r, "messageId")
	)

	err := h.Stream.AckMessage(r.Context(), topic, subscriber, messageId)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) topicExist(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topicName := chi.URLParam(r, "topicName")

		topics, err := GetTopics(r.Context(), core.FilterTopic{
			Name: []string{topicName},
		})
		if err != nil {
			httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
				Error:   err.Error(),
				Message: httpresponse.MessageFailure,
			})
			return
		}

		if len(topics) == 0 {
			httpresponse.Write(w, http.StatusNotFound, &httpresponse.Response{
				Error:   "topic not found",
				Message: httpresponse.MessageNotFound,
			})
			return
		}

		ctx := context.WithValue(r.Context(), topicIdKey, topics[0])

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Http) getTopic(ctx context.Context) core.Topic {
	if ctx == nil {
		return core.Topic{}
	}

	if topic, ok := ctx.Value(topicIdKey).(core.Topic); ok {
		return topic
	}

	return core.Topic{}
}
