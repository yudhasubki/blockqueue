package metric

// Common outcomes are part of BlockQueue's stable Prometheus label schema.
const (
	OutcomeSuccess    = "success"
	OutcomeFailed     = "failed"
	OutcomeLeaseLost  = "lease_lost"
	OutcomeDeadLetter = "dead_letter"
	OutcomeBusy       = "busy"
	OutcomeRetry      = "retry"
)

const (
	PublishResultAdmitted  = "admitted"
	PublishResultPersisted = "persisted"
	PublishResultDuplicate = "duplicate"
)

const (
	DeliveryOperationClaim     = "claim"
	DeliveryOperationAck       = "ack"
	DeliveryOperationNack      = "nack"
	DeliveryOperationLease     = "lease"
	DeliveryOperationBatchAck  = "batch_ack"
	DeliveryOperationBatchNack = "batch_nack"
)

const (
	SchedulerOperationClaimed   = "claimed"
	SchedulerOperationPublished = "published"
)

const (
	WorkerOutcomeProcessed        = "processed"
	WorkerOutcomeNacked           = "nacked"
	WorkerOutcomeCancelled        = "cancelled"
	WorkerOutcomeLeaseLost        = OutcomeLeaseLost
	WorkerOutcomeCompletionFailed = "completion_failed"
)

const (
	WorkerHandlerResultOK              = "ok"
	WorkerHandlerResultError           = "error"
	WorkerHandlerResultCancelRequested = "cancel_requested"
	WorkerHandlerResultPanic           = "panic"
)
