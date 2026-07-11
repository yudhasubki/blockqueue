package worker

import (
	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/internal/textlimit"
)

func boundedDeliveryText(value string) string {
	return textlimit.UTF8(value, blockqueue.MaxDeliveryTextBytes)
}
