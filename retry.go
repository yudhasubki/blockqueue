package blockqueue

import (
	"hash/fnv"
	"math"
	"strconv"
	"time"
)

const leaseExpiredError = "delivery lease expired"

// retryDelayFor applies deterministic jitter. The same failed delivery always
// receives the same delay, while different message IDs avoid retry stampedes.
func retryDelayFor(options subscriberOptions, failureCount int, messageID string) time.Duration {
	if failureCount < 1 {
		failureCount = 1
	}
	base := float64(options.RetryInitialDelay)
	base *= math.Pow(options.RetryMultiplier, float64(failureCount-1))
	if base > float64(options.RetryMaxDelay) {
		base = float64(options.RetryMaxDelay)
	}
	if base <= 0 || options.RetryJitter <= 0 {
		return time.Duration(base)
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(messageID))
	_, _ = hash.Write([]byte{':'})
	_, _ = hash.Write([]byte(strconv.Itoa(failureCount)))
	unit := float64(hash.Sum64()%1_000_001) / 1_000_000 // [0, 1]
	factor := 1 + ((unit*2)-1)*options.RetryJitter
	delay := time.Duration(base * factor)
	if delay < 0 {
		return 0
	}
	if delay > options.RetryMaxDelay {
		return options.RetryMaxDelay
	}
	return delay
}
