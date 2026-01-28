CREATE INDEX IF NOT EXISTS idx_subscriber_messages_pending ON subscriber_messages(subscriber_id, visible_at) WHERE status = 'pending';
