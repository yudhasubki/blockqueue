package blockqueue

import "github.com/yudhasubki/blockqueue/internal/persistence"

const (
	lifecycleStateNameNew      = "new"
	lifecycleStateNameRunning  = "running"
	lifecycleStateNameStopping = "stopping"
	lifecycleStateNameStopped  = "stopped"
	lifecycleStateNameUnknown  = "unknown"
)

const (
	scheduleModeImmediate = "immediate"
	scheduleModeDelay     = "delay"
	scheduleModeAbsolute  = "absolute"
)

// Publish receipt states are stable public API values.
const (
	PublishStateAdmitted  = "admitted"
	PublishStatePersisted = "persisted"
	PublishStateStaged    = "staged"
)

// Delivery states are shared by the database state machine and public API.
const (
	DeliveryStatusPending    = persistence.DeliveryStatusPending
	DeliveryStatusDelivered  = persistence.DeliveryStatusDelivered
	DeliveryStatusProcessed  = persistence.DeliveryStatusProcessed
	DeliveryStatusDeadLetter = persistence.DeliveryStatusDeadLetter
	DeliveryStatusCancelled  = persistence.DeliveryStatusCancelled
)

// DeliveryResultStatusFailed is an operation result, not a persisted delivery
// state. The typed error returned by single-item methods remains authoritative.
const DeliveryResultStatusFailed = "failed"

// Schedule run states are stable public API and persisted values.
const (
	ScheduleRunStatusRunning   = persistence.ScheduleRunStatusRunning
	ScheduleRunStatusCompleted = persistence.ScheduleRunStatusCompleted
	ScheduleRunStatusSkipped   = persistence.ScheduleRunStatusSkipped
	ScheduleRunStatusFailed    = persistence.ScheduleRunStatusFailed
)

// Schedule policies currently expose the supported v0.2 behavior.
const (
	ScheduleMisfirePolicyFireOnce = persistence.ScheduleMisfirePolicyFireOnce
	ScheduleOverlapPolicySkip     = persistence.ScheduleOverlapPolicySkip
)
