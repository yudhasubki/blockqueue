package blockqueue

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

type reconnectNotificationDriver struct {
	store.Driver
	calls  atomic.Int32
	events chan string
}

func (driver *reconnectNotificationDriver) Dialect() store.Dialect { return store.DialectPostgres }

func (driver *reconnectNotificationDriver) Listen(context.Context, string) (<-chan string, error) {
	if driver.calls.Add(1) == 1 {
		return nil, errors.New("injected initial listener failure")
	}
	return driver.events, nil
}

func TestDatabaseEventsReconnectAfterInitialListenFailure(t *testing.T) {
	underlying, err := sqlite.Open(filepath.Join(t.TempDir(), "listener-reconnect.db"), sqlite.Config{})
	require.NoError(t, err)
	driver := &reconnectNotificationDriver{Driver: underlying, events: make(chan string)}
	queue := New(driver, Options{DisableMetrics: true})
	ctx, cancel := context.WithCancel(context.Background())
	queue.serverCtx = ctx
	done := make(chan struct{})
	go func() {
		queue.startDatabaseEvents()
		close(done)
	}()

	require.Eventually(t, func() bool {
		return driver.calls.Load() >= 2 && queue.listenerHealthy.Load()
	}, 3*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("database event loop did not stop")
	}
	require.NoError(t, underlying.Close())
}

func TestDatabaseListenerFailureDoesNotGateReadiness(t *testing.T) {
	queue, _, _ := setupQueue(t)
	queue.setListenerHealthy(false)

	require.False(t, queue.listenerHealthy.Load(), "listener health must remain observable")
	require.True(t, queue.Ready(context.Background()),
		"bounded database reconciliation is authoritative when notification hints are unavailable")
}

func TestRunRejectsPostgresPoolThatMaintenanceLeadershipWouldExhaust(t *testing.T) {
	underlying, err := sqlite.Open(filepath.Join(t.TempDir(), "postgres-pool-capacity.db"), sqlite.Config{})
	require.NoError(t, err)
	underlying.DB().SetMaxOpenConns(1)
	driver := &reconnectNotificationDriver{Driver: underlying, events: make(chan string)}
	queue := New(driver, Options{DisableMetrics: true})

	err = queue.Run(context.Background())
	require.ErrorContains(t, err, "at least 2")
	require.Equal(t, LifecycleStopped, queue.State())
}
