CREATE TABLE topics (
    id UUID PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_topic_name_unique
ON topics(name) WHERE deleted_at IS NULL;

CREATE INDEX idx_topic_search_by_name
ON topics(name, deleted_at);

CREATE TABLE topic_subscribers (
    id UUID PRIMARY KEY,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    name VARCHAR(150) NOT NULL,
    option JSONB NOT NULL DEFAULT '{}'::jsonb,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    visibility_timeout_ms BIGINT NOT NULL DEFAULT 300000 CHECK (visibility_timeout_ms > 0),
    dequeue_batch_size INTEGER NOT NULL DEFAULT 10 CHECK (dequeue_batch_size BETWEEN 1 AND 1000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_subscriber_name_unique
ON topic_subscribers(name, topic_id) WHERE deleted_at IS NULL;

CREATE INDEX idx_topic_subscribers_search_by_topic_id
ON topic_subscribers(topic_id, deleted_at);

CREATE INDEX idx_topic_subscribers_search_by_name
ON topic_subscribers(name, deleted_at);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id VARCHAR(255),
    idempotency_key VARCHAR(128),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    scheduled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_messages_topic_idempotency
ON messages(topic_id, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_messages_topic_created
ON messages(topic_id, created_at, id);

CREATE TABLE message_deliveries (
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    subscriber_id UUID NOT NULL REFERENCES topic_subscribers(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'processed', 'dead_letter')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    visible_at TIMESTAMPTZ NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    message_created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    receipt_token UUID,
    lease_expires_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    last_error TEXT,
    CHECK (status <> 'delivered' OR (receipt_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (status <> 'pending' OR (receipt_token IS NULL AND lease_expires_at IS NULL)),
    CHECK (status NOT IN ('processed', 'dead_letter') OR processed_at IS NOT NULL),
    PRIMARY KEY (message_id, subscriber_id)
) WITH (fillfactor = 85, autovacuum_vacuum_scale_factor = 0.01);

CREATE INDEX idx_message_deliveries_dequeue
ON message_deliveries(subscriber_id, priority DESC, visible_at, message_created_at, message_id)
WHERE status = 'pending';

CREATE INDEX idx_message_deliveries_pending_wake
ON message_deliveries(subscriber_id, visible_at)
WHERE status = 'pending';

CREATE INDEX idx_message_deliveries_lease
ON message_deliveries(subscriber_id, lease_expires_at)
WHERE status = 'delivered';

CREATE INDEX idx_message_deliveries_global_lease
ON message_deliveries(lease_expires_at)
WHERE status = 'delivered';

CREATE INDEX idx_message_deliveries_subscriber
ON message_deliveries(subscriber_id);

CREATE INDEX idx_message_deliveries_dlq
ON message_deliveries(subscriber_id, message_created_at DESC, message_id DESC)
WHERE status = 'dead_letter';

CREATE INDEX idx_message_deliveries_dlq_retention
ON message_deliveries(processed_at)
WHERE status = 'dead_letter';

CREATE INDEX idx_message_deliveries_processed
ON message_deliveries(processed_at)
WHERE status = 'processed';

CREATE TABLE schedules (
    id UUID PRIMARY KEY,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    name VARCHAR(150) NOT NULL,
    cron_expression VARCHAR(255) NOT NULL,
    timezone VARCHAR(100) NOT NULL DEFAULT 'UTC',
    message TEXT NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id VARCHAR(255),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    misfire_policy VARCHAR(20) NOT NULL DEFAULT 'fire_once',
    overlap_policy VARCHAR(20) NOT NULL DEFAULT 'skip',
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    version INTEGER NOT NULL DEFAULT 1,
    next_run_at TIMESTAMPTZ NOT NULL,
    owner_id VARCHAR(100),
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic_id, name)
);

CREATE INDEX idx_schedules_due
ON schedules(paused, next_run_at, lease_expires_at);

CREATE TABLE schedule_runs (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    message_id UUID REFERENCES messages(id) ON DELETE SET NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'skipped', 'failed')),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (schedule_id, scheduled_for)
);

CREATE INDEX idx_schedule_runs_message
ON schedule_runs(message_id)
WHERE message_id IS NOT NULL;

CREATE INDEX idx_schedule_runs_running_message
ON schedule_runs(message_id)
WHERE status = 'running' AND message_id IS NOT NULL;

CREATE INDEX idx_schedule_runs_running_schedule
ON schedule_runs(schedule_id, scheduled_for)
WHERE status = 'running';
