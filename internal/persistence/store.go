package persistence

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

type DeliveryRow = deliveryRow
type CancellationRow = cancellationRow
type CheckpointMode = sqliteCheckpointMode
type CheckpointResult = sqliteCheckpointResult

const (
	CheckpointPassive  CheckpointMode = sqliteCheckpointPassive
	CheckpointTruncate CheckpointMode = sqliteCheckpointTruncate
)

// Store is the queue engine's concrete persistence boundary. It deliberately
// avoids a broad repository interface: SQLite and PostgreSQL share the same
// transactional implementation, with only genuine SQL differences delegated
// to the internal dialect strategy.
type Store struct {
	database *db
}

func New(driver store.Driver) *Store { return &Store{database: newDb(driver)} }

func (store *Store) Conn() *sqlx.DB      { return store.database.Conn() }
func (store *Store) DialectError() error { return store.database.dialectErr }
func (store *Store) Close() error        { return store.database.close() }
func (store *Store) SupportsSQLiteMaintenance() bool {
	return store.database.supportsSQLiteMaintenance()
}
func (store *Store) StatementCacheLen() int { return store.database.statements.len() }
func (store *Store) DatabaseNow(ctx context.Context) (time.Time, error) {
	return store.database.databaseNow(ctx)
}
func (store *Store) TryMaintenanceLeadership(ctx context.Context) (bool, func() error, error) {
	return store.database.tryMaintenanceLeadership(ctx)
}

func (store *Store) GetTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	return store.database.getTopics(ctx, filter)
}

func (store *Store) ListTopics(ctx context.Context, limit int, afterName, afterID string) (Topics, error) {
	return store.database.listTopics(ctx, limit, afterName, afterID)
}

func (store *Store) GetSubscribers(ctx context.Context, filter SubscriberFilter) (Subscribers, error) {
	return store.database.getSubscribers(ctx, filter)
}

func (store *Store) CreateTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return store.database.createTopic(ctx, topic, subscribers)
}

func (store *Store) CreateSubscribers(ctx context.Context, subscribers Subscribers) error {
	return store.database.createSubscribers(ctx, subscribers)
}

func (store *Store) DeleteTopic(ctx context.Context, topicID uuid.UUID) error {
	return store.database.deleteTopic(ctx, topicID)
}

func (store *Store) DeleteSubscriber(ctx context.Context, topicID, subscriberID uuid.UUID, name string) error {
	return store.database.deleteSubscriber(ctx, topicID, subscriberID, name)
}

func (store *Store) SetTopicPaused(ctx context.Context, topicID uuid.UUID, paused bool) error {
	return store.database.setTopicPaused(ctx, topicID, paused)
}

func (store *Store) SetSubscriberPaused(ctx context.Context, subscriberID uuid.UUID, paused bool) error {
	return store.database.setSubscriberPaused(ctx, subscriberID, paused)
}

func (store *Store) TopicSubscriberQueueStats(ctx context.Context, topicID uuid.UUID) (map[uuid.UUID]SubscriberQueueStats, error) {
	return store.database.getTopicSubscriberQueueStats(ctx, topicID)
}

func (store *Store) ListSubscriberStatuses(ctx context.Context, topicID uuid.UUID, limit int, afterName, afterID string) ([]SubscriberStatusRow, error) {
	return store.database.listSubscriberStatuses(ctx, topicID, limit, afterName, afterID)
}

func (store *Store) HasDeletedTopology(ctx context.Context) (bool, error) {
	return store.database.hasDeletedTopology(ctx)
}

func (store *Store) PruneDeletedTopology(ctx context.Context, budget time.Duration) (int64, bool, bool, error) {
	return store.database.pruneDeletedTopology(ctx, budget)
}

func (store *Store) PruneProcessedMessages(ctx context.Context, retention time.Duration) error {
	return store.database.pruneProcessedMessages(ctx, retention)
}

func (store *Store) PruneScheduleRuns(ctx context.Context, retention time.Duration) error {
	return store.database.pruneScheduleRuns(ctx, retention)
}

func (store *Store) PruneDeadLetters(ctx context.Context, retention time.Duration) error {
	return store.database.pruneDeadLetters(ctx, retention)
}

func (store *Store) CheckpointSQLite(ctx context.Context, mode CheckpointMode) (CheckpointResult, error) {
	return store.database.checkpointSQLite(ctx, mode)
}

func (store *Store) IncrementalVacuum(ctx context.Context) error {
	return store.database.incrementalVacuum(ctx)
}

func (store *Store) PersistWriteRequests(ctx context.Context, requests []WriteRequest) (PersistWriteResult, error) {
	return store.database.persistWriteRequests(ctx, requests)
}

func (store *Store) PersistWriteRequestsWithTx(ctx context.Context, tx *sql.Tx, requests []WriteRequest) (PersistWriteResult, error) {
	return store.database.persistWriteRequestsWithTx(ctx, tx, requests)
}

func (store *Store) ClaimDeliveries(ctx context.Context, subscriberID uuid.UUID, limit int, lease time.Duration) ([]DeliveryRow, error) {
	return store.database.claimDeliveries(ctx, subscriberID, limit, lease)
}

func (store *Store) ReapExpiredDeliveries(ctx context.Context, limit int) (int64, error) {
	return store.database.reapExpiredDeliveries(ctx, limit)
}

func (store *Store) NextLeaseExpiry(ctx context.Context) (time.Time, time.Time, bool, error) {
	return store.database.nextLeaseExpiry(ctx)
}

func (store *Store) AckDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string) error {
	return store.database.ackDelivery(ctx, subscriberID, messageID, receipt)
}

func (store *Store) AckDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string) error {
	return store.database.ackDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt)
}

func (store *Store) BatchAckDeliveries(ctx context.Context, subscriberID uuid.UUID, requests []BatchAckItem) ([]error, error) {
	return store.database.batchAckDeliveries(ctx, subscriberID, requests)
}

func (store *Store) NackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	return store.database.nackDelivery(ctx, subscriberID, messageID, receipt, delay, errorText)
}

func (store *Store) NackDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	return store.database.nackDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, delay, errorText)
}

func (store *Store) BatchNackDeliveries(ctx context.Context, subscriberID uuid.UUID, requests []BatchNackItem) ([]bool, []error, error) {
	return store.database.batchNackDeliveries(ctx, subscriberID, requests)
}

func (store *Store) ExtendDeliveryLease(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, extension time.Duration) (time.Time, error) {
	return store.database.extendDeliveryLease(ctx, subscriberID, messageID, receipt, extension)
}

func (store *Store) NextDeliveryWake(ctx context.Context, subscriberID uuid.UUID) (time.Time, time.Time, bool, error) {
	return store.database.nextDeliveryWake(ctx, subscriberID)
}

func (store *Store) ListDeliveries(ctx context.Context, subscriberID uuid.UUID, deadLetter bool, limit int, cursor string) ([]DeliveryRow, error) {
	return store.database.listDeliveries(ctx, subscriberID, deadLetter, limit, cursor)
}

func (store *Store) ReplayDeadLetter(ctx context.Context, subscriberID uuid.UUID, messageID string) (bool, error) {
	return store.database.replayDeadLetter(ctx, subscriberID, messageID)
}

func (store *Store) BatchReplayDeadLetters(ctx context.Context, subscriberID uuid.UUID, messageIDs []string) ([]bool, error) {
	return store.database.batchReplayDeadLetters(ctx, subscriberID, messageIDs)
}

func (store *Store) SnoozeDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration) (time.Time, error) {
	return store.database.snoozeDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, delay)
}

func (store *Store) CancelDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, reason string) (string, error) {
	return store.database.cancelDeliveryWithTx(ctx, tx, subscriberID, messageID, reason)
}

func (store *Store) CancelClaimedDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt, reason string) error {
	return store.database.cancelClaimedDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, reason)
}

func (store *Store) CancelMessageWithTx(ctx context.Context, tx *sql.Tx, topicID uuid.UUID, messageID, reason string) ([]CancellationRow, error) {
	return store.database.cancelMessageWithTx(ctx, tx, topicID, messageID, reason)
}

func (store *Store) ListDeliveryErrors(ctx context.Context, subscriberID uuid.UUID, messageID string, limit int, cursor string) ([]DeliveryError, error) {
	return store.database.listDeliveryErrors(ctx, subscriberID, messageID, limit, cursor)
}

func (store *Store) GetMessageStatus(ctx context.Context, topicID uuid.UUID, messageID string) (MessageStatus, error) {
	return store.database.getMessageStatus(ctx, topicID, messageID)
}

func (store *Store) ScheduleNameExists(ctx context.Context, topicID uuid.UUID, name string) (bool, error) {
	return store.database.scheduleNameExists(ctx, topicID, name)
}

func (store *Store) CreateSchedule(ctx context.Context, schedule Schedule) error {
	return store.database.createSchedule(ctx, schedule)
}

func (store *Store) GetSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) (Schedule, error) {
	return store.database.getSchedule(ctx, topicID, scheduleID)
}

func (store *Store) ListSchedules(ctx context.Context, topicID uuid.UUID) ([]Schedule, error) {
	return store.database.listSchedules(ctx, topicID)
}

func (store *Store) ListSchedulesPage(ctx context.Context, topicID uuid.UUID, limit int, afterName, afterID string) ([]Schedule, error) {
	return store.database.listSchedulesPage(ctx, topicID, limit, afterName, afterID)
}

func (store *Store) UpdateSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string, expectedVersion int, schedule Schedule) error {
	return store.database.updateSchedule(ctx, topicID, scheduleID, expectedVersion, schedule)
}

func (store *Store) DeleteSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) error {
	return store.database.deleteSchedule(ctx, topicID, scheduleID)
}

func (store *Store) SetSchedulePaused(ctx context.Context, topicID uuid.UUID, scheduleID string, paused bool) error {
	return store.database.setSchedulePaused(ctx, topicID, scheduleID, paused)
}

func (store *Store) ListScheduleRuns(ctx context.Context, scheduleID string, limit int, before time.Time, beforeID string) ([]ScheduleRun, error) {
	return store.database.listScheduleRuns(ctx, scheduleID, limit, before, beforeID)
}

func (store *Store) NextScheduleDue(ctx context.Context, now time.Time) (time.Time, time.Time, bool, error) {
	return store.database.nextScheduleDue(ctx, now)
}

func (store *Store) ClaimDueSchedule(ctx context.Context, owner string, now time.Time, lease time.Duration) (Schedule, bool, error) {
	return store.database.claimDueSchedule(ctx, owner, now, lease)
}

func (store *Store) PersistScheduleOccurrence(ctx context.Context, claimed Schedule, scheduledFor, nextRunAt time.Time, force, advance bool, owner string) (ScheduleRun, error) {
	return store.database.persistScheduleOccurrence(ctx, claimed, scheduledFor, nextRunAt, force, advance, owner)
}

func (store *Store) FailScheduleOccurrence(ctx context.Context, claimed Schedule, scheduledFor, nextRunAt time.Time, advance bool, owner, failure string) (ScheduleRun, error) {
	return store.database.failScheduleOccurrence(ctx, claimed, scheduledFor, nextRunAt, advance, owner, failure)
}
