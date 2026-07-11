package blockqueue

import (
	"strings"
	"sync"
)

type deliveryQueryKey struct {
	driver    string
	operation string
	rows      int
}

var deliveryQueryCache sync.Map

func cachedDeliveryQuery(d *db, operation string, rows int, build func() string) string {
	key := deliveryQueryKey{driver: d.Conn().DriverName(), operation: operation, rows: rows}
	if query, ok := deliveryQueryCache.Load(key); ok {
		return query.(string)
	}
	rebound := d.Conn().Rebind(build())
	actual, _ := deliveryQueryCache.LoadOrStore(key, rebound)
	return actual.(string)
}

func cachedClaimSelectQuery(d *db) string {
	return cachedDeliveryQuery(d, "claim_select", 0, func() string {
		query := `
			SELECT deliveries.message_id, messages.message, messages.headers,
			       messages.correlation_id, deliveries.priority, deliveries.status,
			       deliveries.delivery_count, deliveries.failure_count,
			       deliveries.visible_at, deliveries.receipt_token,
			       deliveries.lease_expires_at, messages.created_at
			FROM message_deliveries deliveries
			JOIN messages ON messages.id = deliveries.message_id
			JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
			JOIN topics ON topics.id = messages.topic_id
			WHERE deliveries.subscriber_id = ? AND deliveries.status = 'pending'
			  AND deliveries.visible_at <= ?
			  AND subscribers.deleted_at IS NULL AND subscribers.paused = ` + boolLiteral(d, false) + `
			  AND topics.deleted_at IS NULL AND topics.paused = ` + boolLiteral(d, false) + `
			ORDER BY deliveries.priority DESC, deliveries.visible_at ASC,
			         deliveries.message_created_at ASC, deliveries.message_id ASC
			LIMIT ?`
		return query + d.dialect.lockClause("deliveries", true)
	})
}

func cachedClaimUpdateQuery(d *db, rows int) string {
	return cachedDeliveryQuery(d, "claim_update", rows, func() string {
		var query strings.Builder
		query.WriteString("WITH claimed(message_id, receipt_token) AS (VALUES ")
		for index := 0; index < rows; index++ {
			if index > 0 {
				query.WriteByte(',')
			}
			query.WriteString(d.dialect.deliveryIdentityRow())
		}
		query.WriteString(`)
			UPDATE message_deliveries
			SET status = 'delivered', delivery_count = delivery_count + 1,
			    receipt_token = (SELECT receipt_token FROM claimed
			        WHERE claimed.message_id = message_deliveries.message_id),
			    lease_expires_at = ?, delivered_at = ?, processed_at = NULL, last_error = NULL
			WHERE subscriber_id = ? AND status = 'pending'
			  AND message_id IN (SELECT message_id FROM claimed)
			RETURNING message_id`)
		return query.String()
	})
}

func cachedRequeueSelectQuery(d *db, scoped bool) string {
	rows := 0
	if scoped {
		rows = 1
	}
	return cachedDeliveryQuery(d, "requeue_select", rows, func() string {
		query := `
			SELECT deliveries.message_id, deliveries.subscriber_id, deliveries.failure_count,
			       subscribers.max_attempts, subscribers.retry_initial_delay_ms,
			       subscribers.retry_max_delay_ms, subscribers.retry_multiplier, subscribers.retry_jitter
			FROM message_deliveries deliveries
			JOIN topic_subscribers subscribers ON subscribers.id = deliveries.subscriber_id
			WHERE deliveries.status = 'delivered' AND deliveries.lease_expires_at <= ?`
		if scoped {
			query += " AND deliveries.subscriber_id = ?"
		}
		query += " ORDER BY deliveries.lease_expires_at, deliveries.message_id, deliveries.subscriber_id LIMIT ?"
		return query + d.dialect.lockClause("deliveries", true)
	})
}

func cachedRequeueUpdateQuery(d *db, rows int) string {
	return cachedDeliveryQuery(d, "requeue_update", rows, func() string {
		var query strings.Builder
		query.WriteString("WITH failed(message_id, subscriber_id, failure_count, next_status, visible_at, processed_at) AS (VALUES ")
		for index := 0; index < rows; index++ {
			if index > 0 {
				query.WriteByte(',')
			}
			query.WriteString(d.dialect.deliveryFailureRow())
		}
		query.WriteString(`)
			UPDATE message_deliveries
			SET failure_count = (SELECT failure_count FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    status = (SELECT next_status FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    visible_at = (SELECT visible_at FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    receipt_token = NULL, lease_expires_at = NULL,
			    processed_at = (SELECT processed_at FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id),
			    last_error = ?
			WHERE status = 'delivered' AND EXISTS (
			    SELECT 1 FROM failed WHERE failed.message_id = message_deliveries.message_id AND failed.subscriber_id = message_deliveries.subscriber_id
			)`)
		return query.String()
	})
}

func cachedAckUpdateQuery(d *db) string {
	return cachedDeliveryQuery(d, "ack_update", 0, func() string {
		return `UPDATE message_deliveries
			SET status = 'processed', processed_at = ?, lease_expires_at = NULL
			WHERE message_id = ? AND subscriber_id = ? AND status = 'delivered'
			  AND receipt_token = ? AND lease_expires_at > ?`
	})
}

func cachedAckStateQuery(d *db) string {
	return cachedDeliveryQuery(d, "ack_state", 0, func() string {
		return "SELECT status, receipt_token FROM message_deliveries WHERE message_id = ? AND subscriber_id = ?"
	})
}

func cachedNextDeliveryWakeQuery(d *db) string {
	return cachedDeliveryQuery(d, "next_delivery_wake", 0, func() string {
		return `SELECT MIN(wake_at) FROM (
			SELECT visible_at AS wake_at FROM message_deliveries
			WHERE subscriber_id = ? AND status = 'pending'
			UNION ALL
			SELECT lease_expires_at AS wake_at FROM message_deliveries
			WHERE subscriber_id = ? AND status = 'delivered'
		) wakeups`
	})
}

func cachedCompleteScheduleRunQuery(d *db) string {
	return cachedDeliveryQuery(d, "complete_schedule_run", 0, func() string {
		return `UPDATE schedule_runs
			SET status = 'completed', finished_at = CURRENT_TIMESTAMP
			WHERE status = 'running' AND message_id = ?
			  AND NOT EXISTS (
				SELECT 1 FROM message_deliveries
				WHERE message_deliveries.message_id = schedule_runs.message_id
				  AND message_deliveries.status NOT IN ('processed', 'dead_letter', 'cancelled')
			  )`
	})
}
