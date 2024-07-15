CREATE TABLE IF NOT EXISTS topic_messages (
    id VARCHAR(36) PRIMARY KEY,
    topic_id VARCHAR(36) NOT NULL,
    message TEXT NOT NULL,
    status VARCHAR DEFAULT 'waiting',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_id) 
        REFERENCES topics (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);

CREATE INDEX idx_topic_messages_search_by_topic_and_status ON topic_messages (topic_id, status);