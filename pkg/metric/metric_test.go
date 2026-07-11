package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestWriterGaugesAggregateMultipleQueues(t *testing.T) {
	first := RegisterWriter()
	second := RegisterWriter()
	t.Cleanup(func() {
		UnregisterWriter(first)
		UnregisterWriter(second)
	})
	SetWriterHealth(first, false)
	SetWriterHealth(second, true)
	if got := testutil.ToFloat64(WriterHealthy); got != 0 {
		t.Fatalf("WriterHealthy=%v, want 0 while any writer is unhealthy", got)
	}
	SetPersistenceLag(first, 2)
	SetPersistenceLag(second, 5)
	if got := testutil.ToFloat64(PersistenceLag); got != 5 {
		t.Fatalf("PersistenceLag=%v, want maximum 5", got)
	}
	UnregisterWriter(first)
	SetWriterHealth(second, true)
	if got := testutil.ToFloat64(WriterHealthy); got != 1 {
		t.Fatalf("WriterHealthy=%v, want 1", got)
	}
}

func TestRuntimeGaugesAggregateMultipleQueues(t *testing.T) {
	first := RegisterRuntime()
	second := RegisterRuntime()
	t.Cleanup(func() {
		UnregisterRuntime(first)
		UnregisterRuntime(second)
	})
	SetSchedulerHealth(first, false)
	SetDeliveryReaperHealth(second, false)
	SetDatabaseListenerHealth(first, false)
	SetSchedulerDueLag(first, 2)
	SetSchedulerDueLag(second, 7)
	if got := testutil.ToFloat64(SchedulerHealthy); got != 0 {
		t.Fatalf("SchedulerHealthy=%v, want 0", got)
	}
	if got := testutil.ToFloat64(DeliveryReaperHealthy); got != 0 {
		t.Fatalf("DeliveryReaperHealthy=%v, want 0", got)
	}
	if got := testutil.ToFloat64(DatabaseListenerHealthy); got != 0 {
		t.Fatalf("DatabaseListenerHealthy=%v, want 0", got)
	}
	if got := testutil.ToFloat64(SchedulerDueLag); got != 7 {
		t.Fatalf("SchedulerDueLag=%v, want 7", got)
	}
}
