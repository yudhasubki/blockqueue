package subscriberconfig

import (
	"math"
	"testing"
	"time"
)

func TestNormalizeAndParseDefaults(t *testing.T) {
	normalized := Normalize(Options{})
	if normalized.MaxAttempts != 3 || normalized.VisibilityDuration != "5m" ||
		normalized.DequeueBatchSize != 10 || normalized.RetryPolicy.InitialDelay != "1s" ||
		normalized.RetryPolicy.MaxDelay != "1h" || normalized.RetryPolicy.Multiplier != 2 ||
		normalized.RetryPolicy.Jitter != 0.2 {
		t.Fatalf("unexpected normalized defaults: %+v", normalized)
	}

	parsed, err := Parse(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.MaxAttempts != 3 || parsed.VisibilityDuration != 5*time.Minute ||
		parsed.DequeueBatchSize != 10 || parsed.RetryInitialDelay != time.Second ||
		parsed.RetryMaxDelay != time.Hour || parsed.RetryMultiplier != 2 || parsed.RetryJitter != 0.2 {
		t.Fatalf("unexpected parsed defaults: %+v", parsed)
	}
}

func TestNormalizeCanDisableJitter(t *testing.T) {
	normalized := Normalize(Options{RetryPolicy: RetryPolicy{Jitter: 0.8, DisableJitter: true}})
	if normalized.RetryPolicy.Jitter != 0 {
		t.Fatalf("jitter=%v, want 0", normalized.RetryPolicy.Jitter)
	}
}

func TestParseRejectsInvalidOptions(t *testing.T) {
	valid := func() Options {
		return Options{
			MaxAttempts: 3, VisibilityDuration: "1m", DequeueBatchSize: 10,
			RetryPolicy: RetryPolicy{InitialDelay: "1s", MaxDelay: "1m", Multiplier: 2, Jitter: 0.2},
		}
	}
	tests := []struct {
		name   string
		change func(*Options)
	}{
		{name: "non-positive visibility", change: func(options *Options) { options.VisibilityDuration = "0s" }},
		{name: "visibility above maximum", change: func(options *Options) { options.VisibilityDuration = "13h" }},
		{name: "sub-millisecond visibility", change: func(options *Options) { options.VisibilityDuration = "500us" }},
		{name: "oversized batch", change: func(options *Options) { options.DequeueBatchSize = 1001 }},
		{name: "negative initial delay", change: func(options *Options) { options.RetryPolicy.InitialDelay = "-1s" }},
		{name: "sub-millisecond initial delay", change: func(options *Options) { options.RetryPolicy.InitialDelay = "500us" }},
		{name: "max below initial", change: func(options *Options) { options.RetryPolicy.MaxDelay = "500ms" }},
		{name: "sub-millisecond max delay", change: func(options *Options) {
			options.RetryPolicy.InitialDelay = "0s"
			options.RetryPolicy.MaxDelay = "500us"
		}},
		{name: "small multiplier", change: func(options *Options) { options.RetryPolicy.Multiplier = 0.5 }},
		{name: "nan multiplier", change: func(options *Options) { options.RetryPolicy.Multiplier = math.NaN() }},
		{name: "infinite multiplier", change: func(options *Options) { options.RetryPolicy.Multiplier = math.Inf(1) }},
		{name: "negative jitter", change: func(options *Options) { options.RetryPolicy.Jitter = -0.1 }},
		{name: "large jitter", change: func(options *Options) { options.RetryPolicy.Jitter = 1.1 }},
		{name: "nan jitter", change: func(options *Options) { options.RetryPolicy.Jitter = math.NaN() }},
		{name: "infinite jitter", change: func(options *Options) { options.RetryPolicy.Jitter = math.Inf(1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid()
			test.change(&options)
			if _, err := Parse(options); err == nil {
				t.Fatalf("Parse(%+v) succeeded", options)
			}
		})
	}
}
