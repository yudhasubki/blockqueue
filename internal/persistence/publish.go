package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func (d *db) persistWriteRequests(ctx context.Context, requests []writeRequest) ([]bool, error) {
	return d.persistWriteRequestsWithTx(ctx, nil, requests)
}

// persistWriteRequestsWithTx uses a caller-owned transaction when supplied.
// The caller retains commit/rollback ownership; a nil transaction preserves
// the normal writer-owned transaction boundary.
func (d *db) persistWriteRequestsWithTx(ctx context.Context, external *sql.Tx, requests []writeRequest) ([]bool, error) {
	if len(requests) == 0 {
		return []bool{}, nil
	}
	normalized := append([]writeRequest(nil), requests...)
	now := time.Now().UTC()
	for i := range normalized {
		if len(normalized[i].Headers) == 0 {
			normalized[i].Headers = []byte("{}")
		}
		if !json.Valid(normalized[i].Headers) {
			return nil, fmt.Errorf("%w: invalid headers JSON", ErrInvalidPublish)
		}
		if normalized[i].CreatedAt.IsZero() {
			normalized[i].CreatedAt = now
		}
	}
	messageChunk := d.dialect.messageChunkSize()
	messageStatements := make(map[int]*sqlx.Stmt, 2)
	if external == nil {
		for start := 0; start < len(normalized); start += messageChunk {
			rows := min(messageChunk, len(normalized)-start)
			if _, exists := messageStatements[rows]; exists {
				continue
			}
			statement, err := d.statements.get(ctx, d.Conn(), cachedMessageInsertQuery(d, rows))
			if err != nil {
				return nil, err
			}
			messageStatements[rows] = statement
		}
	}
	deliveryStatements := make(map[int]*sqlx.Stmt, 2)
	messagesByTopic := make(map[uuid.UUID]int)
	for _, request := range normalized {
		messagesByTopic[request.TopicID]++
	}
	if external == nil {
		for _, count := range messagesByTopic {
			for remaining := count; remaining > 0; remaining -= min(200, remaining) {
				rows := min(200, remaining)
				if _, exists := deliveryStatements[rows]; exists {
					continue
				}
				statement, err := d.statements.get(ctx, d.Conn(), cachedDeliveryInsertQuery(d, rows))
				if err != nil {
					return nil, err
				}
				deliveryStatements[rows] = statement
			}
		}
	}
	duplicates := make([]bool, len(requests))
	run := func(ctx context.Context, tx *sqlx.Tx) error {
		topicIDs := make([]uuid.UUID, 0)
		seenTopics := make(map[uuid.UUID]struct{})
		for _, request := range normalized {
			if _, exists := seenTopics[request.TopicID]; !exists {
				seenTopics[request.TopicID] = struct{}{}
				topicIDs = append(topicIDs, request.TopicID)
			}
		}
		sort.Slice(topicIDs, func(i, j int) bool { return topicIDs[i].String() < topicIDs[j].String() })
		for _, topicID := range topicIDs {
			query := `SELECT topics.id,
				(SELECT COUNT(*) FROM topic_subscribers subscribers
				 WHERE subscribers.topic_id = topics.id AND subscribers.deleted_at IS NULL) AS subscribers
				FROM topics WHERE topics.id = ? AND topics.deleted_at IS NULL`
			// Publishers share the topology fence with other publishers. Topology
			// mutations take FOR UPDATE, so they wait for the fan-out snapshot
			// without serializing normal or caller-owned publish transactions.
			query += d.dialect.topologyReadLockClause("topics")
			var state struct {
				ID          string `db:"id"`
				Subscribers int    `db:"subscribers"`
			}
			if err := tx.GetContext(ctx, &state, d.Conn().Rebind(query), topicID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrTopicNotFound
				}
				return err
			}
			if state.Subscribers == 0 {
				return ErrNoActiveSubscriber
			}
		}

		insertedIDs := make(map[string]struct{}, len(normalized))
		for start := 0; start < len(normalized); start += messageChunk {
			end := min(start+messageChunk, len(normalized))
			chunk := normalized[start:end]
			args := make([]any, 0, len(chunk)*9)
			for _, request := range chunk {
				args = append(args,
					request.MessageID, request.TopicID, request.Message, string(request.Headers),
					nullString(request.CorrelationID), nullString(request.IdempotencyKey),
					request.Priority, request.VisibleAt, request.CreatedAt,
				)
			}
			ids := make([]string, 0, len(chunk))
			if statement := messageStatements[len(chunk)]; statement != nil {
				if err := tx.StmtxContext(ctx, statement).SelectContext(ctx, &ids, args...); err != nil {
					return err
				}
			} else if err := tx.SelectContext(ctx, &ids, cachedMessageInsertQuery(d, len(chunk)), args...); err != nil {
				return err
			}
			for _, id := range ids {
				insertedIDs[id] = struct{}{}
			}
		}

		newRequests := make([]writeRequest, 0, len(insertedIDs))
		claimedInsert := make(map[string]struct{}, len(insertedIDs))
		for i, request := range normalized {
			_, inserted := insertedIDs[request.MessageID]
			if inserted {
				if _, alreadyClaimed := claimedInsert[request.MessageID]; !alreadyClaimed {
					claimedInsert[request.MessageID] = struct{}{}
					newRequests = append(newRequests, request)
					continue
				}
			}
			duplicates[i] = true
			var existing struct {
				ID             string         `db:"id"`
				TopicID        string         `db:"topic_id"`
				Message        string         `db:"message"`
				Headers        string         `db:"headers"`
				CorrelationID  sql.NullString `db:"correlation_id"`
				IdempotencyKey sql.NullString `db:"idempotency_key"`
				Priority       int            `db:"priority"`
				ScheduledAt    time.Time      `db:"scheduled_at"`
				CreatedAt      time.Time      `db:"created_at"`
			}
			query := `SELECT id, topic_id, message, headers, correlation_id,
				idempotency_key, priority, scheduled_at, created_at FROM messages WHERE id = ?`
			args := []any{request.MessageID}
			if request.IdempotencyKey != "" {
				query = `SELECT id, topic_id, message, headers, correlation_id,
					idempotency_key, priority, scheduled_at, created_at FROM messages
					WHERE topic_id = ? AND idempotency_key = ?`
				args = []any{request.TopicID, request.IdempotencyKey}
			}
			if err := tx.GetContext(ctx, &existing, d.Conn().Rebind(query), args...); err != nil {
				return err
			}
			existingScheduled := !existing.ScheduledAt.Equal(existing.CreatedAt)
			requestedScheduled := !request.VisibleAt.Equal(request.CreatedAt)
			if existing.TopicID != request.TopicID.String() || existing.Message != request.Message ||
				!equalJSON(existing.Headers, string(request.Headers)) || existing.CorrelationID.String != request.CorrelationID ||
				existing.IdempotencyKey.String != request.IdempotencyKey || existing.Priority != request.Priority ||
				existingScheduled != requestedScheduled ||
				(requestedScheduled && !existing.ScheduledAt.Equal(request.VisibleAt)) {
				return ErrIdempotencyConflict
			}
		}

		// Fanout is based only on messages inserted by this transaction.
		// Idempotent retries cannot acquire subscribers added later.
		byTopic := make(map[uuid.UUID][]writeRequest)
		for _, request := range newRequests {
			byTopic[request.TopicID] = append(byTopic[request.TopicID], request)
		}
		for topicID, topicRequests := range byTopic {
			for remaining := topicRequests; len(remaining) > 0; {
				take := min(200, len(remaining)) // 801 bind variables including topic.
				chunk := remaining[:take]
				remaining = remaining[take:]
				args := make([]any, 0, len(chunk)*4+1)
				for _, request := range chunk {
					args = append(args, request.MessageID, request.VisibleAt, request.Priority, request.CreatedAt)
				}
				args = append(args, topicID)
				var err error
				if statement := deliveryStatements[len(chunk)]; statement != nil {
					_, err = tx.StmtxContext(ctx, statement).ExecContext(ctx, args...)
				} else {
					_, err = tx.ExecContext(ctx, cachedDeliveryInsertQuery(d, len(chunk)), args...)
				}
				if err != nil {
					return err
				}
			}
			if err := d.notifyTx(ctx, tx, deliveryEvent(topicID.String())); err != nil {
				return err
			}
		}
		return nil
	}
	var err error
	if external == nil {
		err = d.tx(ctx, run)
	} else {
		err = run(ctx, &sqlx.Tx{Tx: external, Mapper: d.Conn().Mapper})
	}
	return duplicates, err
}

func cachedMessageInsertQuery(d *db, rows int) string {
	driverKey := d.Conn().DriverName() + ":messages:safe-retry"
	key := batchQueryKey{driver: driverKey, rows: rows}
	if query, ok := batchInsertQueries.Load(key); ok {
		return query.(string)
	}
	var query strings.Builder
	query.WriteString(`INSERT INTO messages
		(id, topic_id, message, headers, correlation_id, idempotency_key, priority, scheduled_at, created_at)
		VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?)")
	}
	// Stable message IDs fence an ambiguous COMMIT. If the connection is lost
	// after the server commits, retry observes the existing canonical row and
	// treats it as success instead of reporting a false permanent failure.
	query.WriteString(" ON CONFLICT DO NOTHING RETURNING id")
	rebound := d.Conn().Rebind(query.String())
	actual, _ := batchInsertQueries.LoadOrStore(key, rebound)
	return actual.(string)
}

func cachedDeliveryInsertQuery(d *db, rows int) string {
	key := batchQueryKey{driver: d.Conn().DriverName() + ":deliveries", rows: rows}
	if query, ok := batchInsertQueries.Load(key); ok {
		return query.(string)
	}
	var query strings.Builder
	query.WriteString("WITH batch(message_id, visible_at, priority, message_created_at) AS (VALUES ")
	rowPlaceholders := d.dialect.deliveryBatchRow()
	for i := 0; i < rows; i++ {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString(rowPlaceholders)
	}
	query.WriteString(`)
		INSERT INTO message_deliveries
			(message_id, subscriber_id, status, delivery_count, failure_count, visible_at, priority, message_created_at)
		SELECT batch.message_id, ts.id, 'pending', 0, 0, batch.visible_at,
		       batch.priority, batch.message_created_at
		FROM topic_subscribers ts CROSS JOIN batch
		WHERE ts.topic_id = ? AND ts.deleted_at IS NULL
		ON CONFLICT (message_id, subscriber_id) DO NOTHING`)
	rebound := d.Conn().Rebind(query.String())
	actual, _ := batchInsertQueries.LoadOrStore(key, rebound)
	return actual.(string)
}

type batchQueryKey struct {
	driver string
	rows   int
}

var batchInsertQueries sync.Map
