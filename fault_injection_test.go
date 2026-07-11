package blockqueue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/store"
)

type ambiguousCommitStore struct {
	database             *sql.DB
	name                 string
	armed                atomic.Bool
	commitErr            error
	scheduledReadFault   atomic.Pointer[scheduledReadFault]
	scheduledReadQueries atomic.Int64
}

type scheduledReadFault struct{ err error }

func openAmbiguousCommitStore(t *testing.T, commitErr error) *ambiguousCommitStore {
	t.Helper()
	storage := &ambiguousCommitStore{name: "blockqueue_commit_fault_" + uuid.NewString(), commitErr: commitErr}
	sql.Register(storage.name, &commitFaultDriver{
		inner: &sqlite3.SQLiteDriver{}, armed: &storage.armed, commitErr: commitErr,
		scheduledReadFault: &storage.scheduledReadFault, scheduledReadQueries: &storage.scheduledReadQueries,
	})
	dsn := fmt.Sprintf("file:%s?_synchronous=full&_journal_mode=wal&_foreign_keys=on&_busy_timeout=5000&_txlock=immediate&_auto_vacuum=2",
		filepath.Join(t.TempDir(), "ambiguous-commit.db"))
	database, err := sql.Open(storage.name, dsn)
	require.NoError(t, err)
	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(10)
	require.NoError(t, database.PingContext(context.Background()))
	storage.database = database
	return storage
}

func (storage *ambiguousCommitStore) DB() *sql.DB            { return storage.database }
func (storage *ambiguousCommitStore) Dialect() store.Dialect { return store.DialectSQLite }
func (storage *ambiguousCommitStore) DriverName() string     { return storage.name }
func (storage *ambiguousCommitStore) Close() error           { return storage.database.Close() }

type commitFaultDriver struct {
	inner                driver.Driver
	armed                *atomic.Bool
	commitErr            error
	scheduledReadFault   *atomic.Pointer[scheduledReadFault]
	scheduledReadQueries *atomic.Int64
}

func (fault *commitFaultDriver) Open(name string) (driver.Conn, error) {
	connection, err := fault.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &commitFaultConn{
		Conn: connection, armed: fault.armed, commitErr: fault.commitErr,
		scheduledReadFault: fault.scheduledReadFault, scheduledReadQueries: fault.scheduledReadQueries,
	}, nil
}

type commitFaultConn struct {
	driver.Conn
	armed                *atomic.Bool
	commitErr            error
	scheduledReadFault   *atomic.Pointer[scheduledReadFault]
	scheduledReadQueries *atomic.Int64
}

func (connection *commitFaultConn) Begin() (driver.Tx, error) {
	return connection.BeginTx(context.Background(), driver.TxOptions{})
}

func (connection *commitFaultConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	beginner, ok := connection.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, fmt.Errorf("wrapped driver does not implement ConnBeginTx")
	}
	transaction, err := beginner.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &commitFaultTx{Tx: transaction, armed: connection.armed, commitErr: connection.commitErr}, nil
}

func (connection *commitFaultConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := connection.Conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return connection.Prepare(query)
}

func (connection *commitFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if executor, ok := connection.Conn.(driver.ExecerContext); ok {
		return executor.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (connection *commitFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if fault := connection.scheduledReadFault.Load(); fault != nil &&
		strings.Contains(strings.ToLower(query), "select id, scheduled_at from messages") &&
		connection.scheduledReadFault.CompareAndSwap(fault, nil) {
		connection.scheduledReadQueries.Add(1)
		return nil, fault.err
	}
	if queryer, ok := connection.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (connection *commitFaultConn) Ping(ctx context.Context) error {
	if pinger, ok := connection.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (connection *commitFaultConn) ResetSession(ctx context.Context) error {
	if resetter, ok := connection.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (connection *commitFaultConn) IsValid() bool {
	if validator, ok := connection.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (connection *commitFaultConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := connection.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

type commitFaultTx struct {
	driver.Tx
	armed     *atomic.Bool
	commitErr error
}

func (transaction *commitFaultTx) Commit() error {
	if err := transaction.Tx.Commit(); err != nil {
		return err
	}
	if transaction.armed.CompareAndSwap(true, false) {
		return transaction.commitErr
	}
	return nil
}

func TestWriterRecoversFromAmbiguousCommitWithoutDuplicateFanout(t *testing.T) {
	for name, commitErr := range map[string]error{
		"bad_connection": driver.ErrBadConn,
		"eof":            io.EOF,
		"unexpected_eof": io.ErrUnexpectedEOF,
	} {
		t.Run(name, func(t *testing.T) {
			testWriterRecoversFromAmbiguousCommit(t, commitErr)
		})
	}
}

func testWriterRecoversFromAmbiguousCommit(t *testing.T, commitErr error) {
	t.Helper()
	driver := openAmbiguousCommitStore(t, commitErr)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 1, FlushInterval: time.Millisecond,
		RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("ambiguous-commit")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{
		MaxAttempts: 3, VisibilityDuration: "1m",
	})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		if queue.State() != LifecycleStopped {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = queue.Shutdown(ctx)
		}
	})

	driver.armed.Store(true)
	receipt, err := queue.PublishDurable(context.Background(), topic, Message{Message: "commit exactly once"})
	require.NoError(t, err)
	require.Equal(t, "persisted", receipt.State)
	require.NotNil(t, receipt.Duplicate)
	require.True(t, *receipt.Duplicate,
		"the retry must recognize the first, ambiguously reported commit")

	var messages, deliveries int
	require.NoError(t, queue.db.Conn().Get(&messages,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM messages WHERE id = ?"), receipt.MessageID))
	require.NoError(t, queue.db.Conn().Get(&deliveries,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM message_deliveries WHERE message_id = ?"), receipt.MessageID))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, deliveries)
}

func TestDurableDelayReceiptDoesNotDependOnPostCommitRead(t *testing.T) {
	driver := openAmbiguousCommitStore(t, nil)
	queue := New(driver, Options{Writer: WriterOptions{
		BatchSize: 1, FlushInterval: time.Millisecond,
	}})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("post-commit-read")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		if queue.State() != LifecycleStopped {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = queue.Shutdown(ctx)
		}
	})

	readErr := fmt.Errorf("injected post-commit scheduled_at read failure")
	driver.scheduledReadFault.Store(&scheduledReadFault{err: readErr})
	receipt, err := queue.PublishDurable(context.Background(), topic, Message{
		Message: "database-clock delay", IdempotencyKey: "database-clock-delay", Delay: "2s",
	})
	require.NoError(t, err)
	require.Equal(t, PublishStatePersisted, receipt.State)
	require.NotNil(t, receipt.Duplicate)
	require.False(t, *receipt.Duplicate)
	require.Zero(t, driver.scheduledReadQueries.Load(),
		"durable publish must carry scheduled_at out of its persistence transaction")

	// Prove the one-shot read fault was still armed after Publish returned. The
	// pre-fix post-commit restore query would have consumed this error and made a
	// committed message look like a failed publish.
	var ignored struct {
		ID          string    `db:"id"`
		ScheduledAt time.Time `db:"scheduled_at"`
	}
	err = queue.db.Conn().Get(&ignored,
		queue.db.Conn().Rebind("SELECT id, scheduled_at FROM messages WHERE id = ?"), receipt.MessageID)
	require.ErrorIs(t, err, readErr)
	require.EqualValues(t, 1, driver.scheduledReadQueries.Load())

	var persistedScheduledAt time.Time
	require.NoError(t, queue.db.Conn().Get(&persistedScheduledAt,
		queue.db.Conn().Rebind("SELECT scheduled_at FROM messages WHERE id = ?"), receipt.MessageID))
	require.Equal(t, persistedScheduledAt, receipt.ScheduledAt)

	retry, err := queue.PublishDurable(context.Background(), topic, Message{
		Message: "database-clock delay", IdempotencyKey: "database-clock-delay", Delay: "2s",
	})
	require.NoError(t, err)
	require.Equal(t, receipt.MessageID, retry.MessageID)
	require.NotNil(t, retry.Duplicate)
	require.True(t, *retry.Duplicate)
	require.Equal(t, persistedScheduledAt, retry.ScheduledAt)
	var messages, deliveries int
	require.NoError(t, queue.db.Conn().Get(&messages,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM messages WHERE id = ?"), receipt.MessageID))
	require.NoError(t, queue.db.Conn().Get(&deliveries,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM message_deliveries WHERE message_id = ?"), receipt.MessageID))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, deliveries)
}

func TestWithTxReportsAmbiguousCommitWithoutRetryingBusinessLogic(t *testing.T) {
	for name, commitErr := range map[string]error{
		"bad_connection":    driver.ErrBadConn,
		"context_canceled":  context.Canceled,
		"deadline_exceeded": context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			testWithTxReportsAmbiguousCommit(t, commitErr)
		})
	}
}

func testWithTxReportsAmbiguousCommit(t *testing.T, commitErr error) {
	t.Helper()
	driver := openAmbiguousCommitStore(t, commitErr)
	queue := New(driver, Options{})
	require.NoError(t, queue.Run(context.Background()))
	topic := NewTopic("ambiguous-business-transaction")
	subscriber := NewSubscriber(topic, "worker", SubscriberOptions{})
	require.NoError(t, queue.CreateTopic(context.Background(), topic, Subscribers{subscriber}))
	t.Cleanup(func() {
		if queue.State() != LifecycleStopped {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = queue.Shutdown(ctx)
		}
	})
	require.NoError(t, func() error {
		_, err := queue.db.Conn().Exec("CREATE TABLE business_events (id TEXT PRIMARY KEY)")
		return err
	}())

	driver.armed.Store(true)
	callbackCalls := 0
	err := queue.WithTx(context.Background(), nil, func(tx *sql.Tx) error {
		callbackCalls++
		if _, err := tx.Exec("INSERT INTO business_events (id) VALUES (?)", "paid-order"); err != nil {
			return err
		}
		_, err := queue.PublishTx(context.Background(), tx, topic, Message{
			Message: "order paid", IdempotencyKey: "paid-order",
		})
		return err
	})
	require.ErrorIs(t, err, ErrTransactionCommitUnknown)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, callbackCalls, "WithTx must never retry application code")

	var businessRows, messageRows int
	require.NoError(t, queue.db.Conn().Get(&businessRows, "SELECT COUNT(*) FROM business_events WHERE id = 'paid-order'"))
	require.NoError(t, queue.db.Conn().Get(&messageRows,
		queue.db.Conn().Rebind("SELECT COUNT(*) FROM messages WHERE idempotency_key = ?"), "paid-order"))
	require.Equal(t, 1, businessRows)
	require.Equal(t, 1, messageRows)
}
