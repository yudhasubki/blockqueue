CREATE TABLE IF NOT EXISTS topics (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL
);

CREATE UNIQUE INDEX idx_topic_name_unique ON topics(name) WHERE deleted_at IS NULL;
CREATE INDEX idx_topic_search_by_id ON topics (id);
CREATE INDEX idx_topic_search_by_name ON topics (name, deleted_at);