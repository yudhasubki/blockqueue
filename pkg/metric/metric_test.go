package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterIsIdempotentAndSupportsCustomRegistries(t *testing.T) {
	first := prometheus.NewRegistry()
	if err := Register(first); err != nil {
		t.Fatal(err)
	}
	if err := Register(first); err != nil {
		t.Fatal(err)
	}
	second := prometheus.NewRegistry()
	if err := Register(second); err != nil {
		t.Fatal(err)
	}
}
