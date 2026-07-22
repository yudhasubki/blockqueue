package blockqueue

import (
	"context"

	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/store"
)

var (
	ErrMigrationChecksum  = persistence.ErrMigrationChecksum
	ErrUnsupportedDialect = persistence.ErrUnsupportedDialect
)

// Migrate installs the current schema and applies future ordered migrations.
// Migrate applies the embedded, checksummed schema for driver's backend.
// Queue.Run invokes it automatically; standalone deployments may call it
// explicitly before serving traffic.
func Migrate(ctx context.Context, driver store.Driver) error {
	return persistence.Migrate(ctx, driver)
}
