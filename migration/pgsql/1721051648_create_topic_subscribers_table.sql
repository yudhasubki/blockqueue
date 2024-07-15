
CREATE TABLE IF NOT EXISTS topic_subscribers (
    id VARCHAR(36) PRIMARY KEY,
    topic_id VARCHAR(36) NOT NULL,
    name VARCHAR(150) NOT NULL,
    option JSON DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (topic_id) 
        REFERENCES topics (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);

CREATE UNIQUE INDEX idx_subscriber_name_unique ON topic_subscribers(name, topic_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_topic_subscribers_search_by_topic_id ON topic_subscribers (topic_id, deleted_at);
CREATE INDEX idx_topic_subscribers_search_by_name ON topic_subscribers (name, deleted_at);