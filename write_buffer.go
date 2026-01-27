package blockqueue

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// WriteBuffer collects messages and batch inserts them for improved throughput
type WriteBuffer struct {
	messages      chan writeRequest
	db            *db
	batchSize     int
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// writeRequest represents a single message to be enqueued
type writeRequest struct {
	TopicId   uuid.UUID
	MessageId string
	Message   string
	VisibleAt time.Time
}

// WriteBufferConfig holds configuration for the write buffer
type WriteBufferConfig struct {
	BatchSize     int           // Max messages before flush (default: 100)
	FlushInterval time.Duration // Max time before flush (default: 50ms)
	BufferSize    int           // Channel buffer size (default: 10000)
}

// DefaultWriteBufferConfig returns sensible defaults
func DefaultWriteBufferConfig() WriteBufferConfig {
	return WriteBufferConfig{
		BatchSize:     100,
		FlushInterval: 50 * time.Millisecond,
		BufferSize:    10000,
	}
}

// NewWriteBuffer creates a new write buffer
func NewWriteBuffer(ctx context.Context, database *db, config WriteBufferConfig) *WriteBuffer {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 50 * time.Millisecond
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 10000
	}

	bufCtx, cancel := context.WithCancel(ctx)

	wb := &WriteBuffer{
		messages:      make(chan writeRequest, config.BufferSize),
		db:            database,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		ctx:           bufCtx,
		cancel:        cancel,
	}

	wb.wg.Add(1)
	go wb.run()

	return wb
}

// Enqueue adds a message to the buffer for batch insertion
func (w *WriteBuffer) Enqueue(topicId uuid.UUID, messageId, message string, delay time.Duration) {
	select {
	case w.messages <- writeRequest{
		TopicId:   topicId,
		MessageId: messageId,
		Message:   message,
		VisibleAt: time.Now().UTC().Add(delay).Truncate(time.Second),
	}:
	case <-w.ctx.Done():
		slog.Warn("write buffer closed, message dropped",
			"topic_id", topicId,
			"message_id", messageId,
		)
	}
}

// Close gracefully shuts down the write buffer
func (w *WriteBuffer) Close() {
	w.cancel()
	w.wg.Wait()
}

// run is the main goroutine that batches and flushes messages
func (w *WriteBuffer) run() {
	defer w.wg.Done()

	batch := make([]writeRequest, 0, w.batchSize)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// Flush remaining messages before exit
			if len(batch) > 0 {
				w.flush(batch)
			}
			// Drain channel
			for {
				select {
				case req := <-w.messages:
					batch = append(batch, req)
					if len(batch) >= w.batchSize {
						w.flush(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						w.flush(batch)
					}
					return
				}
			}

		case req := <-w.messages:
			batch = append(batch, req)
			if len(batch) >= w.batchSize {
				w.flush(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush performs batch insert of messages
func (w *WriteBuffer) flush(batch []writeRequest) {
	if len(batch) == 0 {
		return
	}

	ctx := context.Background()
	err := w.db.batchEnqueueToSubscribers(ctx, batch)
	if err != nil {
		slog.Error("write buffer flush failed",
			logPrefixErr, err,
			"batch_size", len(batch),
		)
	} else {
		slog.Debug("write buffer flushed",
			"batch_size", len(batch),
		)
	}
}

// batchEnqueueToSubscribers performs efficient batch insert
func (d *db) batchEnqueueToSubscribers(ctx context.Context, requests []writeRequest) error {
	if len(requests) == 0 {
		return nil
	}

	// Group by topic for efficient insertion
	byTopic := make(map[uuid.UUID][]writeRequest)
	for _, req := range requests {
		byTopic[req.TopicId] = append(byTopic[req.TopicId], req)
	}

	return d.tx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		for topicId, topicRequests := range byTopic {
			// Get all subscribers for this topic (cached)
			subscribers, err := d.getSubscribersForTopicCached(ctx, topicId)
			if err != nil {
				return err
			}

			if len(subscribers) == 0 {
				continue
			}

			// Build multi-row INSERT statement
			// INSERT INTO subscriber_messages (subscriber_id, topic_id, message_id, message, status, visible_at)
			// VALUES (?, ?, ?, ?, 'pending', ?), (...), ...
			valueStrings := make([]string, 0, len(topicRequests)*len(subscribers))
			valueArgs := make([]interface{}, 0, len(topicRequests)*len(subscribers)*5)

			for _, req := range topicRequests {
				for _, subId := range subscribers {
					valueStrings = append(valueStrings, "(?, ?, ?, ?, 'pending', ?)")
					valueArgs = append(valueArgs, subId, topicId, req.MessageId, req.Message, req.VisibleAt.Format("2006-01-02 15:04:05"))
				}
			}

			query := fmt.Sprintf(
				"INSERT INTO subscriber_messages (subscriber_id, topic_id, message_id, message, status, visible_at) VALUES %s",
				strings.Join(valueStrings, ", "),
			)

			_, err = tx.ExecContext(ctx, tx.Rebind(query), valueArgs...)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// subscriberCache caches subscriber IDs per topic to avoid DB queries on every flush
// This is inspired by NATS/NSQ routing table caching pattern
var subscriberCache sync.Map // topicId -> cachedSubscribers

type cachedSubscribers struct {
	ids       []uuid.UUID
	expiresAt time.Time
}

const subscriberCacheTTL = 5 * time.Second

// getSubscribersForTopicCached returns subscriber IDs with caching
func (d *db) getSubscribersForTopicCached(ctx context.Context, topicId uuid.UUID) ([]uuid.UUID, error) {
	// Check cache first
	if cached, ok := subscriberCache.Load(topicId); ok {
		cs := cached.(cachedSubscribers)
		if time.Now().Before(cs.expiresAt) {
			return cs.ids, nil
		}
	}

	// Cache miss or expired - fetch from DB
	ids, err := d.getSubscribersForTopic(ctx, topicId)
	if err != nil {
		return nil, err
	}

	// Store in cache
	subscriberCache.Store(topicId, cachedSubscribers{
		ids:       ids,
		expiresAt: time.Now().Add(subscriberCacheTTL),
	})

	return ids, nil
}

// InvalidateSubscriberCache removes a topic from cache (call when subscribers change)
func InvalidateSubscriberCache(topicId uuid.UUID) {
	subscriberCache.Delete(topicId)
}

// getSubscribersForTopic returns subscriber IDs for a topic (uncached)
func (d *db) getSubscribersForTopic(ctx context.Context, topicId uuid.UUID) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	query := "SELECT id FROM topic_subscribers WHERE topic_id = ? AND deleted_at IS NULL"
	err := d.Database.Conn().SelectContext(ctx, &ids,
		d.Database.Conn().Rebind(query),
		topicId,
	)
	return ids, err
}
