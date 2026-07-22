package blockqueue

import (
	"time"

	"github.com/yudhasubki/blockqueue/internal/subscriberconfig"
)

const (
	// MaximumMessageBytes is the largest accepted UTF-8 message payload.
	MaximumMessageBytes = 1 << 20
	// MaximumHeadersBytes is the largest encoded headers object.
	MaximumHeadersBytes = 16 << 10
	// MaximumIdempotencyKeyBytes bounds a per-topic idempotency key.
	MaximumIdempotencyKeyBytes = 128
	// MaximumCorrelationIDBytes bounds an optional correlation identifier.
	MaximumCorrelationIDBytes = 255
	// MinimumPriority is the lowest accepted delivery priority.
	MinimumPriority = -1000
	// MaximumPriority is the highest accepted delivery priority.
	MaximumPriority = 1000
	// MaximumDeliveryLease is the longest claim or heartbeat lease.
	MaximumDeliveryLease = subscriberconfig.MaximumDeliveryLease
	// MinimumCheckpointInterval bounds automatic SQLite WAL checkpoints.
	MinimumCheckpointInterval = 30 * time.Second
)
