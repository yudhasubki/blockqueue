// Package subscriberconfig owns subscriber defaults and validation shared by
// the runtime registry and persistence layer.
package subscriberconfig

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	MaximumDeliveryLease = 12 * time.Hour
	maximumBatchSize     = 1000
)

type RetryPolicy struct {
	InitialDelay  string
	MaxDelay      string
	Multiplier    float64
	Jitter        float64
	DisableJitter bool
}

type Options struct {
	MaxAttempts        int
	VisibilityDuration string
	DequeueBatchSize   int
	RetryPolicy        RetryPolicy
}

type Parsed struct {
	MaxAttempts        int
	VisibilityDuration time.Duration
	DequeueBatchSize   int
	RetryInitialDelay  time.Duration
	RetryMaxDelay      time.Duration
	RetryMultiplier    float64
	RetryJitter        float64
}

func Normalize(options Options) Options {
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.VisibilityDuration == "" {
		options.VisibilityDuration = "5m"
	}
	if options.DequeueBatchSize <= 0 {
		options.DequeueBatchSize = 10
	}
	if options.RetryPolicy.InitialDelay == "" {
		options.RetryPolicy.InitialDelay = "1s"
	}
	if options.RetryPolicy.MaxDelay == "" {
		options.RetryPolicy.MaxDelay = "1h"
	}
	if options.RetryPolicy.Multiplier == 0 {
		options.RetryPolicy.Multiplier = 2
	}
	if options.RetryPolicy.DisableJitter {
		options.RetryPolicy.Jitter = 0
	} else if options.RetryPolicy.Jitter == 0 {
		options.RetryPolicy.Jitter = 0.2
	}
	return options
}

func Parse(options Options) (Parsed, error) {
	options = Normalize(options)
	visibilityDuration, err := time.ParseDuration(options.VisibilityDuration)
	if err != nil || visibilityDuration <= 0 {
		return Parsed{}, errors.New("subscriber visibility_duration must be positive")
	}
	if visibilityDuration > MaximumDeliveryLease {
		return Parsed{}, fmt.Errorf("subscriber visibility_duration cannot exceed %s", MaximumDeliveryLease)
	}
	if visibilityDuration < time.Millisecond {
		return Parsed{}, errors.New("subscriber visibility_duration must be at least 1ms")
	}
	if options.DequeueBatchSize > maximumBatchSize {
		return Parsed{}, fmt.Errorf("subscriber dequeue_batch_size exceeds %d", maximumBatchSize)
	}
	retryInitialDelay, err := time.ParseDuration(options.RetryPolicy.InitialDelay)
	if err != nil || retryInitialDelay < 0 {
		return Parsed{}, errors.New("subscriber retry initial_delay must be non-negative")
	}
	if retryInitialDelay > 0 && retryInitialDelay < time.Millisecond {
		return Parsed{}, errors.New("subscriber retry initial_delay must be zero or at least 1ms")
	}
	retryMaxDelay, err := time.ParseDuration(options.RetryPolicy.MaxDelay)
	if err != nil || retryMaxDelay < retryInitialDelay {
		return Parsed{}, errors.New("subscriber retry max_delay must be at least initial_delay")
	}
	if retryMaxDelay > 0 && retryMaxDelay < time.Millisecond {
		return Parsed{}, errors.New("subscriber retry max_delay must be zero or at least 1ms")
	}
	if math.IsNaN(options.RetryPolicy.Multiplier) || math.IsInf(options.RetryPolicy.Multiplier, 0) ||
		options.RetryPolicy.Multiplier < 1 {
		return Parsed{}, errors.New("subscriber retry multiplier must be at least 1")
	}
	if math.IsNaN(options.RetryPolicy.Jitter) || math.IsInf(options.RetryPolicy.Jitter, 0) ||
		options.RetryPolicy.Jitter < 0 || options.RetryPolicy.Jitter > 1 {
		return Parsed{}, errors.New("subscriber retry jitter must be between 0 and 1")
	}
	return Parsed{
		MaxAttempts:        options.MaxAttempts,
		VisibilityDuration: visibilityDuration,
		DequeueBatchSize:   options.DequeueBatchSize,
		RetryInitialDelay:  retryInitialDelay,
		RetryMaxDelay:      retryMaxDelay,
		RetryMultiplier:    options.RetryPolicy.Multiplier,
		RetryJitter:        options.RetryPolicy.Jitter,
	}, nil
}
