ALTER TABLE messages ADD COLUMN idempotency_hash VARCHAR(64);

DROP INDEX IF EXISTS idx_schedules_due;

CREATE INDEX idx_schedules_unowned_due
ON schedules(next_run_at, id)
WHERE paused = 0 AND owner_id IS NULL;

CREATE INDEX idx_schedules_leased_due
ON schedules(lease_expires_at, id)
WHERE paused = 0 AND owner_id IS NOT NULL;

CREATE INDEX idx_topics_page
ON topics(name, id)
WHERE deleted_at IS NULL;

CREATE INDEX idx_subscribers_page
ON topic_subscribers(topic_id, name, id)
WHERE deleted_at IS NULL;

CREATE INDEX idx_schedules_page
ON schedules(topic_id, name, id);
