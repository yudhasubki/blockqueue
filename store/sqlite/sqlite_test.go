package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

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
	defer driver.Close()
	var synchronous int
	if err := driver.DB().QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 1 {
		t.Fatalf("synchronous=%d, want NORMAL(1)", synchronous)
	}
}
