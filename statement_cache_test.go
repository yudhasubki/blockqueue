package blockqueue

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store/sqlite"
)

func TestPreparedStatementCacheIsBoundedAndClosedWithDatabase(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "statements.db"), sqlite.Config{})
	require.NoError(t, err)
	database := newDb(driver)
	for index := 0; index < defaultStatementCacheSize+20; index++ {
		_, err := database.statements.get(context.Background(), database.Conn(),
			fmt.Sprintf("SELECT 1 /* statement-%d */", index))
		require.NoError(t, err)
	}
	require.Equal(t, defaultStatementCacheSize, database.statements.len())
	require.NoError(t, database.close())
	require.Zero(t, database.statements.len())
	require.NoError(t, database.close(), "database close must be idempotent")
}

func TestPreparedWriterWorksWithSingleSQLiteConnection(t *testing.T) {
	driver, err := sqlite.Open(filepath.Join(t.TempDir(), "single-connection.db"), sqlite.Config{
		MaxOpenConns: 1, MaxIdleConns: 1,
	})
	require.NoError(t, err)
	queue := New(driver, Options{})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("single-connection")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "30s",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = queue.Publish(ctx, topic, Message{Message: "prepared"})
	require.NoError(t, err)
	require.NoError(t, queue.Shutdown(ctx))
}
