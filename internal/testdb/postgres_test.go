package testdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGuardedURLRejectsUnsafeDatabase(t *testing.T) {
	_, _, err := guardedURL("postgres://localhost/production", "_test")
	require.ErrorContains(t, err, "must end with")

	parsed, database, err := guardedURL("postgres://localhost/blockqueue_test?sslmode=disable", "_test")
	require.NoError(t, err)
	require.Equal(t, "blockqueue_test", database)
	require.Equal(t, "disable", parsed.Query().Get("sslmode"))
}
