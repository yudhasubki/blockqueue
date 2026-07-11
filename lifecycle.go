package blockqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Run validates durable state and builds the complete runtime snapshot before
// starting background workers.
func (q *Queue) Run(ctx context.Context) error {
	q.runMu.Lock()
	defer q.runMu.Unlock()
	if LifecycleState(q.state.Load()) != LifecycleNew {
		return fmt.Errorf("%w: state=%s", ErrQueueNotRunning, q.State())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if q.db.dialectErr != nil {
		q.state.Store(uint32(LifecycleStopped))
		return errors.Join(q.db.dialectErr, q.db.close())
	}
	if err := Migrate(ctx, q.db.Database); err != nil {
		q.state.Store(uint32(LifecycleStopped))
		return errors.Join(err, q.db.close())
	}
	if err := q.db.Conn().PingContext(ctx); err != nil {
		q.state.Store(uint32(LifecycleStopped))
		return errors.Join(fmt.Errorf("database startup check: %w", err), q.db.close())
	}
	topics, err := q.db.getTopics(ctx, TopicFilter{})
	if err != nil {
		q.state.Store(uint32(LifecycleStopped))
		return errors.Join(err, q.db.close())
	}

	// Replace the construction context only after storage and metadata have
	// been validated. Runtime snapshots are rebuilt from committed state.
	if q.cancel != nil {
		q.cancel()
	}
	qCtx, cancel := context.WithCancel(ctx)
	q.serverCtx = qCtx
	q.cancel = cancel
	q.registry.Store(&topicRegistry{byName: make(map[string]*topicRuntime), byID: make(map[uuid.UUID]*topicRuntime)})

	runtimes := make([]*topicRuntime, 0, len(topics))
	for _, topic := range topics {
		runtime, err := loadTopicRuntime(qCtx, topic, q.db)
		if err != nil {
			cancel()
			q.state.Store(uint32(LifecycleStopped))
			return errors.Join(err, q.db.close())
		}
		runtimes = append(runtimes, runtime)
	}

	writerOptions := q.options.Writer
	if writerOptions.BatchSize == 0 && writerOptions.FlushInterval == 0 &&
		writerOptions.MaxPendingMessages == 0 && writerOptions.MaxPendingBytes == 0 {
		writerOptions = DefaultWriterOptions()
	}
	writerOptions.notify = q.notify
	// Shutdown is coordinated explicitly so cancellation of the serving context
	// cannot make the writer abandon an admitted batch.
	q.writer = newWriter(context.Background(), q.db, writerOptions)
	for _, runtime := range runtimes {
		q.storeTopic(runtime)
	}
	q.state.Store(uint32(LifecycleRunning))

	q.workers.Add(5)
	go func() { defer q.workers.Done(); q.startCheckpointer() }()
	go func() { defer q.workers.Done(); q.startPruner() }()
	go func() { defer q.workers.Done(); q.startScheduler() }()
	go func() { defer q.workers.Done(); q.startDeliveryReaper() }()
	go func() { defer q.workers.Done(); q.startDatabaseEvents() }()
	go func() {
		<-qCtx.Done()
		if q.State() == LifecycleRunning {
			timeout := q.options.ShutdownTimeout
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
			defer shutdownCancel()
			_ = q.shutdown(shutdownCtx, true)
		}
	}()

	return nil
}

func (q *Queue) State() LifecycleState {
	return LifecycleState(q.state.Load())
}

func (s LifecycleState) String() string {
	switch s {
	case LifecycleNew:
		return "new"
	case LifecycleRunning:
		return "running"
	case LifecycleStopping:
		return "stopping"
	case LifecycleStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

func (q *Queue) Close() {
	timeout := q.options.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = q.shutdown(ctx, true)
}

// Shutdown stops admission, drains the writer, stops listeners and maintenance
// workers, performs the final checkpoint, and closes the database driver.
func (q *Queue) Shutdown(ctx context.Context) error {
	return q.shutdown(ctx, true)
}

func (q *Queue) shutdown(ctx context.Context, closeDB bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.shutdownMu.Lock()
	defer q.shutdownMu.Unlock()
	q.runMu.Lock()
	state := q.State()
	if state == LifecycleStopped {
		q.runMu.Unlock()
		if closeDB {
			return q.db.close()
		}
		return nil
	}
	if state == LifecycleNew {
		q.state.Store(uint32(LifecycleStopped))
		if q.cancel != nil {
			q.cancel()
		}
		q.runMu.Unlock()
		if closeDB {
			return q.db.close()
		}
		return nil
	}
	q.state.Store(uint32(LifecycleStopping))
	q.runMu.Unlock()

	q.admissionMu.Lock()
	var flushErr error
	if q.writer != nil {
		flushErr = q.writer.CloseContext(ctx)
	}
	q.admissionMu.Unlock()
	if flushErr != nil {
		q.writer.abortWrites()
	}

	if q.cancel != nil {
		q.cancel()
	}
	workersDone := make(chan struct{})
	go func() {
		q.workers.Wait()
		close(workersDone)
	}()
	var workersErr error
	select {
	case <-workersDone:
	case <-ctx.Done():
		workersErr = fmt.Errorf("shutdown workers: %w", ctx.Err())
	}
	if flushErr == nil && workersErr == nil && q.db.supportsSQLiteMaintenance() {
		q.checkpointSQLite(ctx, sqliteCheckpointTruncate)
	}

	q.state.Store(uint32(LifecycleStopped))
	var closeErr error
	if closeDB {
		closeErr = q.db.close()
	}
	return errors.Join(flushErr, workersErr, closeErr)
}

func (q *Queue) Live() bool {
	return q.State() != LifecycleStopped
}

func (q *Queue) WriterHealthy() bool {
	return q.writer != nil && q.writer.Healthy()
}

func (q *Queue) Ready(ctx context.Context) bool {
	if q.State() != LifecycleRunning || q.writer == nil || !q.writer.Healthy() ||
		!q.schedulerHealthy.Load() || !q.deliveryHealthy.Load() {
		return false
	}
	if err := q.db.Conn().PingContext(ctx); err != nil {
		return false
	}
	messages, bytes := q.writer.Pending()
	threshold := q.options.ReadinessBacklog
	if threshold <= 0 {
		threshold = q.writer.maxMessages * 9 / 10
	}
	return messages < threshold && bytes < q.writer.maxBytes*9/10
}
