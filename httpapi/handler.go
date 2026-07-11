package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yudhasubki/blockqueue"
)

const (
	maxRequestBody = 16 << 20
	maxBatch       = 1000
	defaultPoll    = 30 * time.Second
	maximumPoll    = 60 * time.Second
	maximumLease   = 12 * time.Hour
)

type Handler struct {
	service  Service
	mapError ErrorMapper
}

type envelope struct {
	Data any `json:"data,omitempty"`
}

type problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Code     string `json:"code"`
	Instance string `json:"instance,omitempty"`
}

type topicContextKey struct{}

func New(service Service, mapper ErrorMapper) *Handler {
	if mapper == nil {
		mapper = func(error) (int, string, string) {
			return http.StatusInternalServerError, problemCodeInternal, "internal server error"
		}
	}
	return &Handler{service: service, mapError: mapper}
}

func (h *Handler) Attach(root chi.Router) {
	root.Get("/topics", h.getTopics)
	root.Post("/topics", h.createTopic)
	root.Route("/topics/{topicName}", func(r chi.Router) {
		r.Use(h.topicExists)
		r.Post("/messages", h.publish)
		r.Post("/messages/batch", h.batchPublish)
		r.Get("/messages/{messageID}", h.getMessageStatus)
		r.Post("/messages/{messageID}/cancel", h.cancelMessage)
		r.Delete("/", h.deleteTopic)
		r.Post("/pause", h.pauseTopic)
		r.Post("/resume", h.resumeTopic)
		r.Get("/subscribers", h.getSubscribers)
		r.Post("/subscribers", h.createSubscribers)
		r.Delete("/subscribers/{subscriberName}", h.deleteSubscriber)
		r.Post("/subscribers/{subscriberName}/claim", h.claim)
		r.Get("/subscribers/{subscriberName}/messages", h.activeMessages)
		r.Post("/subscribers/{subscriberName}/messages/{messageID}/ack", h.ack)
		r.Post("/subscribers/{subscriberName}/messages/{messageID}/nack", h.nack)
		r.Post("/subscribers/{subscriberName}/messages/{messageID}/lease", h.lease)
		r.Post("/subscribers/{subscriberName}/messages/{messageID}/snooze", h.snooze)
		r.Post("/subscribers/{subscriberName}/messages/{messageID}/cancel", h.cancelDelivery)
		r.Get("/subscribers/{subscriberName}/messages/{messageID}/errors", h.deliveryErrors)
		r.Post("/subscribers/{subscriberName}/messages/batch/ack", h.batchAck)
		r.Post("/subscribers/{subscriberName}/messages/batch/nack", h.batchNack)
		r.Post("/subscribers/{subscriberName}/pause", h.pauseSubscriber)
		r.Post("/subscribers/{subscriberName}/resume", h.resumeSubscriber)
		r.Get("/subscribers/{subscriberName}/dlq", h.deadLetters)
		r.Post("/subscribers/{subscriberName}/dlq/replay", h.replayDeadLetters)
		r.Get("/schedules", h.listSchedules)
		r.Post("/schedules", h.createSchedule)
		r.Get("/schedules/{scheduleId}", h.getSchedule)
		r.Put("/schedules/{scheduleId}", h.updateSchedule)
		r.Delete("/schedules/{scheduleId}", h.deleteSchedule)
		r.Post("/schedules/{scheduleId}/pause", h.pauseSchedule)
		r.Post("/schedules/{scheduleId}/resume", h.resumeSchedule)
		r.Post("/schedules/{scheduleId}/run-now", h.runScheduleNow)
		r.Get("/schedules/{scheduleId}/runs", h.scheduleRuns)
	})
}

func (h *Handler) getTopics(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.GetTopics(r.Context(), blockqueue.TopicFilter{})
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) createTopic(w http.ResponseWriter, r *http.Request) {
	var request TopicRequest
	if !h.decode(w, r, &request) {
		return
	}
	if request.Name == "" || len(request.Name) > 150 || len(request.Subscribers) == 0 || len(request.Subscribers) > maxBatch {
		h.validation(w, errors.New("topic name and 1-1000 subscribers are required"))
		return
	}
	topic, subscribers := request.domain()
	if err := h.service.CreateTopic(r.Context(), topic, subscribers); err != nil {
		h.writeError(w, err)
		return
	}
	write(w, http.StatusCreated, topic)
}

func (h *Handler) deleteTopic(w http.ResponseWriter, r *http.Request) {
	if err := h.service.DeleteTopic(r.Context(), topicFrom(r.Context())); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) getSubscribers(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.GetSubscribersStatus(r.Context(), topicFrom(r.Context()))
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) createSubscribers(w http.ResponseWriter, r *http.Request) {
	var request []SubscriberRequest
	if !h.decode(w, r, &request) {
		return
	}
	if len(request) == 0 || len(request) > maxBatch {
		h.validation(w, errors.New("subscriber batch must contain 1-1000 items"))
		return
	}
	for _, subscriber := range request {
		if subscriber.Name == "" || len(subscriber.Name) > 150 {
			h.validation(w, errors.New("invalid subscriber name"))
			return
		}
	}
	topic := topicFrom(r.Context())
	if err := h.service.CreateSubscribers(r.Context(), topic, subscriberCommands(request, topic.ID)); err != nil {
		h.writeError(w, err)
		return
	}
	write(w, http.StatusCreated, request)
}

func (h *Handler) deleteSubscriber(w http.ResponseWriter, r *http.Request) {
	if err := h.service.DeleteSubscriber(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName)); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	var request PublishRequest
	if !h.decode(w, r, &request) {
		return
	}
	waitFor := r.URL.Query().Get(queryParamWaitFor)
	if waitFor != "" && waitFor != waitForCommit {
		h.validation(w, errors.New("wait_for must be commit"))
		return
	}
	var receipt blockqueue.PublishReceipt
	var err error
	status := http.StatusAccepted
	if waitFor == waitForCommit {
		receipt, err = h.service.PublishDurable(r.Context(), topicFrom(r.Context()), request.command())
		status = http.StatusOK
	} else {
		receipt, err = h.service.PublishAsync(r.Context(), topicFrom(r.Context()), request.command())
	}
	if err == nil && status == http.StatusAccepted {
		location := strings.TrimSuffix(r.URL.EscapedPath(), "/") + "/" + url.PathEscape(receipt.MessageID)
		w.Header().Set("Location", location)
	}
	h.respond(w, status, receipt, err)
}

func (h *Handler) batchPublish(w http.ResponseWriter, r *http.Request) {
	var request []PublishRequest
	if !h.decode(w, r, &request) {
		return
	}
	if len(request) == 0 || len(request) > maxBatch {
		h.validation(w, errors.New("batch must contain 1-1000 messages"))
		return
	}
	commands := make([]blockqueue.Message, len(request))
	for i, item := range request {
		commands[i] = item.command()
	}
	waitFor := r.URL.Query().Get(queryParamWaitFor)
	var receipts blockqueue.PublishReceipts
	var err error
	status := http.StatusAccepted
	switch waitFor {
	case waitForCommit:
		receipts, err = h.service.BatchPublishDurable(r.Context(), topicFrom(r.Context()), commands)
		status = http.StatusOK
	case "":
		receipts, err = h.service.BatchPublishAsync(r.Context(), topicFrom(r.Context()), commands)
	default:
		h.validation(w, errors.New("wait_for must be commit"))
		return
	}
	h.respond(w, status, receipts, err)
}

func (h *Handler) getMessageStatus(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.GetMessageStatus(
		r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamMessageID),
	)
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) cancelMessage(w http.ResponseWriter, r *http.Request) {
	var request CancelRequest
	if !h.decodeOptional(w, r, &request) {
		return
	}
	result, err := h.service.CancelMessage(
		r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamMessageID), request.Reason,
	)
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) claim(w http.ResponseWriter, r *http.Request) {
	timeout := defaultPoll
	if raw := r.URL.Query().Get(queryParamTimeout); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 || parsed > maximumPoll {
			h.validation(w, errors.New("timeout must be >0 and <=60s"))
			return
		}
		timeout = parsed
	}
	lease := time.Duration(0)
	if raw := r.URL.Query().Get(queryParamLease); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 || parsed > maximumLease {
			h.validation(w, errors.New("lease must be >0 and <=12h"))
			return
		}
		lease = parsed
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	result, err := h.service.ClaimWait(ctx, topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), queryLimit(r, 10), lease)
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) ack(w http.ResponseWriter, r *http.Request) {
	var request ReceiptRequest
	if !h.decode(w, r, &request) {
		return
	}
	err := h.service.AckDelivery(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), chi.URLParam(r, urlParamMessageID), request.ReceiptToken)
	h.respond(w, http.StatusOK, map[string]string{
		responseFieldStatus: blockqueue.DeliveryStatusProcessed,
	}, err)
}

func (h *Handler) nack(w http.ResponseWriter, r *http.Request) {
	var request NackRequest
	if !h.decode(w, r, &request) {
		return
	}
	delay, err := optionalDuration(request.RetryDelay, false)
	if err != nil {
		h.validation(w, err)
		return
	}
	err = h.service.NackDelivery(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), chi.URLParam(r, urlParamMessageID), request.ReceiptToken, delay, request.Error)
	h.respond(w, http.StatusOK, map[string]string{
		responseFieldStatus: blockqueue.DeliveryStatusPending,
	}, err)
}

func (h *Handler) snooze(w http.ResponseWriter, r *http.Request) {
	var request SnoozeRequest
	if !h.decode(w, r, &request) {
		return
	}
	delay, err := optionalDuration(request.Delay, false)
	if err != nil {
		h.validation(w, err)
		return
	}
	visibleAt, err := h.service.SnoozeDelivery(
		r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName),
		chi.URLParam(r, urlParamMessageID), request.ReceiptToken, delay,
	)
	h.respond(w, http.StatusOK, map[string]string{
		responseFieldStatus:    blockqueue.DeliveryStatusPending,
		responseFieldVisibleAt: visibleAt.Format(time.RFC3339Nano),
	}, err)
}

func (h *Handler) cancelDelivery(w http.ResponseWriter, r *http.Request) {
	var request CancelRequest
	if !h.decodeOptional(w, r, &request) {
		return
	}
	err := h.service.CancelDelivery(
		r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName),
		chi.URLParam(r, urlParamMessageID), request.Reason,
	)
	h.respond(w, http.StatusOK, map[string]string{
		responseFieldStatus: blockqueue.DeliveryStatusCancelled,
	}, err)
}

func (h *Handler) deliveryErrors(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.DeliveryErrors(
		r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName),
		chi.URLParam(r, urlParamMessageID), queryLimit(r, 100), r.URL.Query().Get(queryParamCursor),
	)
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) lease(w http.ResponseWriter, r *http.Request) {
	var request LeaseRequest
	if !h.decode(w, r, &request) {
		return
	}
	extension, err := optionalDuration(request.Extension, true)
	if err != nil {
		h.validation(w, err)
		return
	}
	if extension > maximumLease {
		h.validation(w, errors.New("lease extension must be <=12h"))
		return
	}
	expires, err := h.service.ExtendLease(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), chi.URLParam(r, urlParamMessageID), request.ReceiptToken, extension)
	h.respond(w, http.StatusOK, map[string]string{
		responseFieldLeaseExpiresAt: expires.Format(time.RFC3339Nano),
	}, err)
}

func (h *Handler) batchAck(w http.ResponseWriter, r *http.Request) {
	var request []BatchReceiptRequest
	if !h.decode(w, r, &request) {
		return
	}
	if len(request) == 0 || len(request) > maxBatch {
		h.validation(w, errors.New("invalid batch ack"))
		return
	}
	items := make([]blockqueue.BatchAckItem, len(request))
	for i, item := range request {
		items[i] = blockqueue.BatchAckItem{MessageID: item.MessageID, ReceiptToken: item.ReceiptToken}
	}
	write(w, http.StatusOK, h.service.BatchAckDeliveries(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), items))
}

func (h *Handler) batchNack(w http.ResponseWriter, r *http.Request) {
	var request []BatchNackRequest
	if !h.decode(w, r, &request) {
		return
	}
	if len(request) == 0 || len(request) > maxBatch {
		h.validation(w, errors.New("invalid batch nack"))
		return
	}
	items := make([]blockqueue.BatchNackItem, len(request))
	for i, item := range request {
		delay, err := optionalDuration(item.RetryDelay, false)
		if err != nil {
			h.validation(w, errors.New("invalid retry_delay"))
			return
		}
		items[i] = blockqueue.BatchNackItem{MessageID: item.MessageID, ReceiptToken: item.ReceiptToken, RetryDelay: delay, Error: item.Error}
	}
	write(w, http.StatusOK, h.service.BatchNackDeliveries(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), items))
}

func (h *Handler) activeMessages(w http.ResponseWriter, r *http.Request) { h.deliveryPage(w, r, false) }
func (h *Handler) deadLetters(w http.ResponseWriter, r *http.Request)    { h.deliveryPage(w, r, true) }
func (h *Handler) deliveryPage(w http.ResponseWriter, r *http.Request, deadLetter bool) {
	result, err := h.service.ListDeliveries(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), deadLetter, queryLimit(r, 100), r.URL.Query().Get(queryParamCursor))
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) replayDeadLetters(w http.ResponseWriter, r *http.Request) {
	var request ReplayRequest
	if !h.decode(w, r, &request) {
		return
	}
	if len(request.MessageIDs) == 0 || len(request.MessageIDs) > maxBatch {
		h.validation(w, errors.New("invalid replay batch"))
		return
	}
	write(w, http.StatusOK, h.service.ReplayDeadLetters(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName), request.MessageIDs))
}

func (h *Handler) pauseTopic(w http.ResponseWriter, r *http.Request)  { h.setTopicPause(w, r, true) }
func (h *Handler) resumeTopic(w http.ResponseWriter, r *http.Request) { h.setTopicPause(w, r, false) }
func (h *Handler) setTopicPause(w http.ResponseWriter, r *http.Request, paused bool) {
	var err error
	if paused {
		err = h.service.PauseTopic(r.Context(), topicFrom(r.Context()))
	} else {
		err = h.service.ResumeTopic(r.Context(), topicFrom(r.Context()))
	}
	h.respond(w, http.StatusOK, map[string]bool{responseFieldPaused: paused}, err)
}

func (h *Handler) pauseSubscriber(w http.ResponseWriter, r *http.Request) {
	h.setSubscriberPause(w, r, true)
}
func (h *Handler) resumeSubscriber(w http.ResponseWriter, r *http.Request) {
	h.setSubscriberPause(w, r, false)
}
func (h *Handler) setSubscriberPause(w http.ResponseWriter, r *http.Request, paused bool) {
	var err error
	if paused {
		err = h.service.PauseSubscriber(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName))
	} else {
		err = h.service.ResumeSubscriber(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamSubscriberName))
	}
	h.respond(w, http.StatusOK, map[string]bool{responseFieldPaused: paused}, err)
}

func (h *Handler) listSchedules(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.ListSchedules(r.Context(), topicFrom(r.Context()))
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) createSchedule(w http.ResponseWriter, r *http.Request) {
	var request ScheduleRequest
	if !h.decode(w, r, &request) {
		return
	}
	result, err := h.service.CreateSchedule(r.Context(), topicFrom(r.Context()), request.command())
	h.respond(w, http.StatusCreated, result, err)
}

func (h *Handler) getSchedule(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.GetSchedule(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID))
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) updateSchedule(w http.ResponseWriter, r *http.Request) {
	var request ScheduleUpdateRequest
	if !h.decode(w, r, &request) {
		return
	}
	if request.Version <= 0 {
		h.validation(w, errors.New("version is required"))
		return
	}
	result, err := h.service.UpdateSchedule(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID), request.Version, request.command())
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	if err := h.service.DeleteSchedule(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID)); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) pauseSchedule(w http.ResponseWriter, r *http.Request) {
	h.setSchedulePause(w, r, true)
}
func (h *Handler) resumeSchedule(w http.ResponseWriter, r *http.Request) {
	h.setSchedulePause(w, r, false)
}
func (h *Handler) setSchedulePause(w http.ResponseWriter, r *http.Request, paused bool) {
	err := h.service.PauseSchedule(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID), paused)
	h.respond(w, http.StatusOK, map[string]bool{responseFieldPaused: paused}, err)
}

func (h *Handler) runScheduleNow(w http.ResponseWriter, r *http.Request) {
	force, err := strconv.ParseBool(defaultString(r.URL.Query().Get(queryParamForce), defaultFalse))
	if err != nil {
		h.validation(w, errors.New("force must be boolean"))
		return
	}
	result, err := h.service.RunScheduleNow(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID), force)
	h.respond(w, http.StatusAccepted, result, err)
}

func (h *Handler) scheduleRuns(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.ScheduleRunHistory(r.Context(), topicFrom(r.Context()), chi.URLParam(r, urlParamScheduleID), queryLimit(r, 100), r.URL.Query().Get(queryParamCursor))
	h.respond(w, http.StatusOK, result, err)
}

func (h *Handler) topicExists(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic, exists := h.service.GetTopic(chi.URLParam(r, urlParamTopicName))
		if !exists {
			writeProblem(w, r, http.StatusNotFound, problemCodeTopicNotFound, "topic not found")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), topicContextKey{}, topic)))
	})
}

func topicFrom(ctx context.Context) blockqueue.Topic {
	if topic, ok := ctx.Value(topicContextKey{}).(blockqueue.Topic); ok {
		return topic
	}
	return blockqueue.Topic{}
}

func (h *Handler) decode(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		h.validation(w, err)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body must contain one JSON value")
		}
		h.validation(w, err)
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
		h.validation(w, err)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body must contain one JSON value")
		}
		h.validation(w, err)
		return false
	}
	return true
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
	w.Header().Set("Content-Type", mediaTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope{Data: data})
}

func writeProblem(w http.ResponseWriter, request *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", mediaTypeProblemJSON)
	w.WriteHeader(status)
	instance := ""
	if request != nil {
		instance = request.URL.Path
	}
	_ = json.NewEncoder(w).Encode(problem{
		Type: "https://blockqueue.dev/problems/" + code, Title: http.StatusText(status),
		Status: status, Detail: message, Code: code, Instance: instance,
	})
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
