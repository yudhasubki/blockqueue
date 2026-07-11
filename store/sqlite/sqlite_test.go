package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

func TestOpenRejectsEmptyPath(t *testing.T) {
	_, err := Open("  ", Config{})
	if !errors.Is(err, ErrEmptyPath) {
		t.Fatalf("Open() error = %v, want ErrEmptyPath", err)
	}
}

func TestOpenRejectsNegativeConnectionSettings(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "invalid.db"), Config{BusyTimeout: -1}); err == nil {
		t.Fatal("expected negative busy timeout to fail")
	}
}

func TestOpenEscapesSQLiteURICharactersInPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue?name#fragment.db")
	driver, err := Open(path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database was not created at exact path: %v", err)
	}
}

func TestMemoryDatabaseUsesSingleConnection(t *testing.T) {
	driver, err := Open(":memory:", Config{MaxOpenConns: 10, MaxIdleConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	if got := driver.DB().Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections=%d, want 1", got)
	}
}

func TestOpenConfiguresEveryConnection(t *testing.T) {
	driver, err := Open(filepath.Join(t.TempDir(), "queue.db"), Config{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	database := sqlx.NewDb(driver.DB(), driver.DriverName())
	for i := 0; i < 2; i++ {
		conn, err := database.Connx(context.Background())
		if err != nil {
			t.Fatalf("Connx() error = %v", err)
		}
		var pages, synchronous int
		if err := conn.GetContext(context.Background(), &pages, "PRAGMA wal_autocheckpoint"); err != nil {
			t.Fatal(err)
		}
		if err := conn.GetContext(context.Background(), &synchronous, "PRAGMA synchronous"); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if pages != 0 || synchronous != 2 {
			t.Fatalf("wal_autocheckpoint=%d synchronous=%d, want 0 and FULL(2)", pages, synchronous)
		}
	}
}

func TestBalancedDurabilityIsExplicit(t *testing.T) {
	driver, err := Open(filepath.Join(t.TempDir(), "balanced.db"), Config{Durability: store.DurabilityBalanced})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Error(err)
		}
	}()
	var synchronous int
	if err := driver.DB().QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 1 {
		t.Fatalf("synchronous=%d, want NORMAL(1)", synchronous)
	}
}
