package httpapi

const (
	queryParamWaitFor = "wait_for"
	queryParamTimeout = "timeout"
	queryParamLease   = "lease"
	queryParamCursor  = "cursor"
	queryParamForce   = "force"
	queryParamLimit   = "limit"
	waitForCommit     = "commit"
	defaultFalse      = "false"
)

const (
	urlParamTopicName      = "topicName"
	urlParamSubscriberName = "subscriberName"
	urlParamMessageID      = "messageID"
	urlParamScheduleID     = "scheduleId"
)

const (
	problemCodeInternal           = "internal_error"
	problemCodeValidation         = "validation_error"
	problemCodeTopicNotFound      = "topic_not_found"
	problemCodeResourceNotFound   = "resource_not_found"
	problemCodeConflict           = "conflict"
	problemCodeBufferPressure     = "buffer_pressure"
	problemCodeWriterUnhealthy    = "writer_unhealthy"
	problemCodeServiceUnavailable = "service_unavailable"
	problemCodeUnauthorized       = "unauthorized"
)

const (
	responseFieldStatus         = "status"
	responseFieldVisibleAt      = "visible_at"
	responseFieldLeaseExpiresAt = "lease_expires_at"
	responseFieldPaused         = "paused"
)

const (
	mediaTypeJSON        = "application/json"
	mediaTypeProblemJSON = "application/problem+json"
)
