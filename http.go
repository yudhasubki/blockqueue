package blockqueue

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yudhasubki/blockqueue/pkg/core"
	httpresponse "github.com/yudhasubki/blockqueue/pkg/http"
	"github.com/yudhasubki/blockqueue/pkg/io"
)

type Http struct {
	Stream *BlockQueue[chan io.ResponseMessages]
	UIPath string
}

type ctxKeyTopicName string

const (
	topicIdKey ctxKeyTopicName = "topic"
)

func (h *Http) Router() http.Handler {
	r := chi.NewRouter()

	r.Route("/topics", func(r chi.Router) {
		r.Get("/", h.getTopics)
		r.Post("/", h.createTopic)

		r.Group(func(r chi.Router) {
			r.Use(h.topicExist)
			r.Delete("/{topicName}", h.deleteTopic)
			r.Post("/{topicName}/messages", h.publish)
			r.Post("/{topicName}/messages/batch", h.batchPublish)

			r.Get("/{topicName}/subscribers", h.getSubscribers)
			r.Post("/{topicName}/subscribers", h.createSubscriber)
			r.Delete("/{topicName}/subscribers/{subscriberName}", h.deleteSubscriber)
			r.Get("/{topicName}/subscribers/{subscriberName}", h.readSubscriber)
			r.Delete("/{topicName}/subscribers/{subscriberName}/messages/{messageId}", h.ackMessage)
			r.Delete("/{topicName}/subscribers/{subscriberName}/messages/batch", h.batchAckMessage)
			r.Get("/{topicName}/subscribers/{subscriberName}/dlq", h.getDeadLetterMessages)
			r.Get("/{topicName}/subscribers/{subscriberName}/messages", h.getUnackedMessages)
			r.Post("/{topicName}/subscribers/{subscriberName}/dlq/{messageId}/replay", h.replayDeadLetterMessage)
		})
	})

	// Serve UI
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		uiPath := h.UIPath
		if uiPath == "" {
			uiPath = "./ui"
		}
		fs := http.FileServer(http.Dir(uiPath))
		// Serve index.html for root, otherwise serve file
		if r.URL.Path == "/" {
			http.ServeFile(w, r, filepath.Join(uiPath, "index.html"))
			return
		}
		fs.ServeHTTP(w, r)
	})

	return r
}

func (h *Http) createTopic(w http.ResponseWriter, r *http.Request) {
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

	topics, err := h.Stream.getTopics(r.Context(), core.FilterTopic{
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

	err = h.Stream.addJob(r.Context(), topic, subscribers)
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

func (h *Http) deleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := h.getTopic(r.Context())

	err := h.Stream.deleteJob(topic)
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

func (h *Http) deleteSubscriber(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
	)

	err := h.Stream.deleteSubscriber(r.Context(), topic, subscriber)
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

func (h *Http) publish(w http.ResponseWriter, r *http.Request) {
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

	err = h.Stream.publish(r.Context(), topic, request)
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

func (h *Http) batchPublish(w http.ResponseWriter, r *http.Request) {
	var (
		topic   = h.getTopic(r.Context())
		request []io.Publish
	)

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		slog.Error("[BatchPublish][json.NewDecoder] error decode message", "error", err)
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	err = h.Stream.BatchPublish(r.Context(), topic, request)
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
func (h *Http) getSubscribers(w http.ResponseWriter, r *http.Request) {
	var (
		topic = h.getTopic(r.Context())
	)

	subscriberStatus, err := h.Stream.getSubscribersStatus(r.Context(), topic)
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Data:    subscriberStatus,
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) createSubscriber(w http.ResponseWriter, r *http.Request) {
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

	err = h.Stream.addSubscriber(r.Context(), topic, subscribers)
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

func (h *Http) readSubscriber(w http.ResponseWriter, r *http.Request) {
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

	messages, err := h.Stream.readSubscriberMessage(ctx, topic, subscriber)
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

func (h *Http) ackMessage(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
		messageId  = chi.URLParam(r, "messageId")
	)

	err := h.Stream.ackMessage(r.Context(), topic, subscriber, messageId)
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

func (h *Http) batchAckMessage(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
		request    []string
	)

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		httpresponse.Write(w, http.StatusBadRequest, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	err = h.Stream.BatchAck(r.Context(), topic, subscriber, request)
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

		topic, exist := h.Stream.GetTopic(topicName)
		if !exist {
			httpresponse.Write(w, http.StatusNotFound, &httpresponse.Response{
				Error:   "topic not found",
				Message: httpresponse.MessageNotFound,
			})
			return
		}

		ctx := context.WithValue(r.Context(), topicIdKey, topic)

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

func (h *Http) getDeadLetterMessages(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
	)

	messages, err := h.Stream.GetDeadLetterMessages(r.Context(), topic, subscriber, 100, 0)
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

func (h *Http) replayDeadLetterMessage(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
		messageId  = chi.URLParam(r, "messageId")
	)

	err := h.Stream.RestoreDeadLetterMessage(r.Context(), topic, subscriber, messageId)
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

func (h *Http) getTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := h.Stream.getTopics(r.Context(), core.FilterTopic{})
	if err != nil {
		httpresponse.Write(w, http.StatusInternalServerError, &httpresponse.Response{
			Error:   err.Error(),
			Message: httpresponse.MessageFailure,
		})
		return
	}

	httpresponse.Write(w, http.StatusOK, &httpresponse.Response{
		Data:    topics,
		Message: httpresponse.MessageSuccess,
	})
}

func (h *Http) getUnackedMessages(w http.ResponseWriter, r *http.Request) {
	var (
		topic      = h.getTopic(r.Context())
		subscriber = chi.URLParam(r, "subscriberName")
	)

	messages, err := h.Stream.GetUnackedMessages(r.Context(), topic, subscriber, 100, 0)
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
