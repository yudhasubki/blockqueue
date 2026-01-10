CREATE TABLE IF NOT EXISTS subscriber_messages (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id VARCHAR(36) NOT NULL,
    topic_id VARCHAR(36) NOT NULL,
    message_id VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    status VARCHAR(15) DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    visible_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (subscriber_id) REFERENCES topic_subscribers(id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);

CREATE INDEX idx_subscriber_messages_poll ON subscriber_messages(subscriber_id, status, visible_at);

CREATE INDEX idx_subscriber_messages_ack ON subscriber_messages(message_id, subscriber_id);

CREATE INDEX idx_subscriber_messages_topic ON subscriber_messages(topic_id);
