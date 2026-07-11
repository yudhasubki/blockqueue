CREATE TABLE topics (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME
);

CREATE UNIQUE INDEX idx_topic_name_unique
ON topics(name) WHERE deleted_at IS NULL;

CREATE INDEX idx_topic_search_by_name
ON topics(name, deleted_at);

CREATE TABLE topic_subscribers (
    id VARCHAR(36) PRIMARY KEY,
    topic_id VARCHAR(36) NOT NULL,
    name VARCHAR(150) NOT NULL,
    option TEXT NOT NULL DEFAULT '{}',
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    visibility_timeout_ms INTEGER NOT NULL DEFAULT 300000 CHECK (visibility_timeout_ms > 0),
    dequeue_batch_size INTEGER NOT NULL DEFAULT 10 CHECK (dequeue_batch_size BETWEEN 1 AND 1000),
    retry_initial_delay_ms INTEGER NOT NULL DEFAULT 1000 CHECK (retry_initial_delay_ms >= 0),
    retry_max_delay_ms INTEGER NOT NULL DEFAULT 3600000 CHECK (retry_max_delay_ms >= retry_initial_delay_ms),
    retry_multiplier REAL NOT NULL DEFAULT 2 CHECK (retry_multiplier >= 1),
    retry_jitter REAL NOT NULL DEFAULT 0.2 CHECK (retry_jitter BETWEEN 0 AND 1),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_subscriber_name_unique
ON topic_subscribers(name, topic_id) WHERE deleted_at IS NULL;

CREATE INDEX idx_topic_subscribers_search_by_topic_id
ON topic_subscribers(topic_id, deleted_at);

CREATE INDEX idx_topic_subscribers_search_by_name
ON topic_subscribers(name, deleted_at);

CREATE TABLE messages (
    id VARCHAR(36) PRIMARY KEY,
    topic_id VARCHAR(36) NOT NULL,
    message TEXT NOT NULL,
    headers TEXT NOT NULL DEFAULT '{}',
    correlation_id VARCHAR(255),
    idempotency_key VARCHAR(128),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    scheduled_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_messages_topic_idempotency
ON messages(topic_id, idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_messages_topic_created
ON messages(topic_id, created_at, id);

CREATE TABLE message_deliveries (
    message_id VARCHAR(36) NOT NULL,
    subscriber_id VARCHAR(36) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'processed', 'dead_letter', 'cancelled')),
    delivery_count INTEGER NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    visible_at DATETIME NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    message_created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    receipt_token VARCHAR(36),
    lease_expires_at DATETIME,
    delivered_at DATETIME,
    processed_at DATETIME,
    cancelled_at DATETIME,
    cancel_reason TEXT,
    last_error TEXT,
    CHECK (status <> 'delivered' OR (receipt_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (status <> 'pending' OR (receipt_token IS NULL AND lease_expires_at IS NULL)),
    CHECK (status NOT IN ('processed', 'dead_letter', 'cancelled') OR processed_at IS NOT NULL),
    CHECK (status <> 'cancelled' OR cancelled_at IS NOT NULL),
    PRIMARY KEY (message_id, subscriber_id),
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (subscriber_id) REFERENCES topic_subscribers(id) ON DELETE CASCADE
);

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
WHERE status IN ('processed', 'cancelled');

CREATE TABLE delivery_errors (
    id VARCHAR(36) PRIMARY KEY,
    message_id VARCHAR(36) NOT NULL,
    subscriber_id VARCHAR(36) NOT NULL,
    failure_count INTEGER NOT NULL CHECK (failure_count > 0),
    error TEXT NOT NULL,
    failed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (message_id, subscriber_id)
        REFERENCES message_deliveries(message_id, subscriber_id) ON DELETE CASCADE
);

CREATE INDEX idx_delivery_errors_history
ON delivery_errors(subscriber_id, message_id, failed_at DESC, id DESC);

CREATE TABLE schedules (
    id VARCHAR(36) PRIMARY KEY,
    topic_id VARCHAR(36) NOT NULL,
    name VARCHAR(150) NOT NULL,
    cron_expression VARCHAR(255) NOT NULL,
    timezone VARCHAR(100) NOT NULL DEFAULT 'UTC',
    message TEXT NOT NULL,
    headers TEXT NOT NULL DEFAULT '{}',
    correlation_id VARCHAR(255),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    misfire_policy VARCHAR(20) NOT NULL DEFAULT 'fire_once',
    overlap_policy VARCHAR(20) NOT NULL DEFAULT 'skip',
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    version INTEGER NOT NULL DEFAULT 1,
    next_run_at DATETIME NOT NULL,
    owner_id VARCHAR(100),
    lease_expires_at DATETIME,
    fencing_token INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
    UNIQUE (topic_id, name)
);

CREATE INDEX idx_schedules_due
ON schedules(paused, next_run_at, lease_expires_at);

CREATE TABLE schedule_runs (
    id VARCHAR(36) PRIMARY KEY,
    schedule_id VARCHAR(36) NOT NULL,
    message_id VARCHAR(36),
    scheduled_for DATETIME NOT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME,
    status VARCHAR(20) NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'skipped', 'failed')),
    error TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE SET NULL,
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

CREATE INDEX idx_schedule_runs_retention
ON schedule_runs(COALESCE(finished_at, created_at), id)
WHERE status <> 'running';
