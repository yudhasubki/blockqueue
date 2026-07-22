package persistence

const (
	defaultDeliveryReaperBatchSize = 1000
	migrationBackendSQLite         = "sqlite"
	migrationBackendPostgres       = "pgsql"
	postgresUniqueViolationCode    = "23505"
	deliveryFailureUpdateChunk     = 100 // 601 binds, below SQLite's conservative 999 limit.
	deliveryErrorInsertChunk       = 150 // 900 binds.
	deliveryAckUpdateChunk         = 300 // 603 binds.
	deliveryNackUpdateChunk        = 100 // 702 binds.
	deliveryIdentityLookupChunk    = 500
	deliveryFanoutChunkSize        = 200 // 801 binds including the topic ID.
)

const (
	scheduleModeImmediate = "immediate"
	scheduleModeDelay     = "delay"
	scheduleModeAbsolute  = "absolute"
)

// Delivery states are persisted values protected by schema constraints. Keep
// them centralized for Go-side comparisons and assignments. SQL predicates
// intentionally retain literals so both backends can match partial indexes.
const (
	DeliveryStatusPending    = "pending"
	DeliveryStatusDelivered  = "delivered"
	DeliveryStatusProcessed  = "processed"
	DeliveryStatusDeadLetter = "dead_letter"
	DeliveryStatusCancelled  = "cancelled"
)

const (
	ScheduleRunStatusRunning   = "running"
	ScheduleRunStatusCompleted = "completed"
	ScheduleRunStatusSkipped   = "skipped"
	ScheduleRunStatusFailed    = "failed"
)

const (
	ScheduleMisfirePolicyFireOnce = "fire_once"
	ScheduleOverlapPolicySkip     = "skip"
)

// PostgreSQL notifications are wake-up hints. These values form the internal
// event protocol shared by persistence and the queue listener.
const (
	EventChannel        = "blockqueue_events"
	EventTopology       = "topology"
	EventScheduler      = "scheduler"
	EventDeliveryPrefix = "delivery:"
)

func deliveryEvent(topicID string) string { return EventDeliveryPrefix + topicID }
