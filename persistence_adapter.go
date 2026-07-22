package blockqueue

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/internal/persistence"
	"github.com/yudhasubki/blockqueue/store"
)

const (
	databaseEventChannel        = persistence.EventChannel
	databaseEventTopology       = persistence.EventTopology
	databaseEventScheduler      = persistence.EventScheduler
	databaseEventDeliveryPrefix = persistence.EventDeliveryPrefix
)

type databaseDialect struct{ backend store.Dialect }

func (dialect databaseDialect) kind() store.Dialect { return dialect.backend }
func (dialect databaseDialect) usesDatabaseEvents() bool {
	return dialect.backend == store.DialectPostgres
}

// db is the engine-facing adapter around internal persistence. It intentionally
// contains no SQL; conversions here keep public API types out of the storage
// package and avoid an import cycle.
type db struct {
	Database       store.Driver
	persistence    *persistence.Store
	dialect        databaseDialect
	dialectErr     error
	disableMetrics bool
}

func newDb(driver store.Driver) *db {
	storage := persistence.New(driver)
	return &db{
		Database:    driver,
		persistence: storage,
		dialect:     databaseDialect{backend: driver.Dialect()},
		dialectErr:  storage.DialectError(),
	}
}

func (d *db) Conn() *sqlx.DB                  { return d.persistence.Conn() }
func (d *db) close() error                    { return d.persistence.Close() }
func (d *db) supportsSQLiteMaintenance() bool { return d.persistence.SupportsSQLiteMaintenance() }
func (d *db) statementCacheLen() int          { return d.persistence.StatementCacheLen() }
func (d *db) tryMaintenanceLeadership(ctx context.Context) (bool, func() error, error) {
	return d.persistence.TryMaintenanceLeadership(ctx)
}
func (d *db) hasDeletedTopology(ctx context.Context) (bool, error) {
	return d.persistence.HasDeletedTopology(ctx)
}
func (d *db) setMetricsDisabled(disabled bool) {
	d.disableMetrics = disabled
}
func (d *db) checkpointSQLite(ctx context.Context, mode sqliteCheckpointMode) (sqliteCheckpointResult, error) {
	return d.persistence.CheckpointSQLite(ctx, mode)
}
func (d *db) incrementalVacuum(ctx context.Context) error {
	return d.persistence.IncrementalVacuum(ctx)
}

type sqliteCheckpointMode = persistence.CheckpointMode
type sqliteCheckpointResult = persistence.CheckpointResult

const (
	sqliteCheckpointPassive  = persistence.CheckpointPassive
	sqliteCheckpointTruncate = persistence.CheckpointTruncate
)

type deliveryRow = persistence.DeliveryRow
type persistWriteResult = persistence.PersistWriteResult

// SubscriberQueueStats contains persisted pending and leased delivery counts.
type SubscriberQueueStats struct {
	Pending   int `db:"pending"`
	Delivered int `db:"delivered"`
}

type subscriberStatusRow struct {
	ID        string
	Name      string
	Pending   int
	Delivered int
}

func toPersistenceTopic(topic Topic) persistence.Topic {
	return persistence.Topic{
		ID:        topic.ID,
		Name:      topic.Name,
		Paused:    topic.Paused,
		CreatedAt: topic.CreatedAt,
		DeletedAt: topic.DeletedAt,
	}
}

func fromPersistenceTopic(topic persistence.Topic) Topic {
	return Topic{
		ID:        topic.ID,
		Name:      topic.Name,
		Paused:    topic.Paused,
		CreatedAt: topic.CreatedAt,
		DeletedAt: topic.DeletedAt,
	}
}

func toPersistenceSubscriberOptions(options SubscriberOptions) persistence.SubscriberOptions {
	return persistence.SubscriberOptions{
		MaxAttempts:        options.MaxAttempts,
		VisibilityDuration: options.VisibilityDuration,
		DequeueBatchSize:   options.DequeueBatchSize,
		RetryPolicy: persistence.RetryPolicy{
			InitialDelay:  options.RetryPolicy.InitialDelay,
			MaxDelay:      options.RetryPolicy.MaxDelay,
			Multiplier:    options.RetryPolicy.Multiplier,
			Jitter:        options.RetryPolicy.Jitter,
			DisableJitter: options.RetryPolicy.DisableJitter,
		},
	}
}

func fromPersistenceSubscriberOptions(options persistence.SubscriberOptions) SubscriberOptions {
	return SubscriberOptions{
		MaxAttempts:        options.MaxAttempts,
		VisibilityDuration: options.VisibilityDuration,
		DequeueBatchSize:   options.DequeueBatchSize,
		RetryPolicy: RetryPolicy{
			InitialDelay:  options.RetryPolicy.InitialDelay,
			MaxDelay:      options.RetryPolicy.MaxDelay,
			Multiplier:    options.RetryPolicy.Multiplier,
			Jitter:        options.RetryPolicy.Jitter,
			DisableJitter: options.RetryPolicy.DisableJitter,
		},
	}
}

func toPersistenceSubscriber(subscriber Subscriber) persistence.Subscriber {
	return persistence.Subscriber{
		ID:        subscriber.ID,
		TopicID:   subscriber.TopicID,
		TopicName: subscriber.TopicName,
		Name:      subscriber.Name,
		Options:   toPersistenceSubscriberOptions(subscriber.Options),
		Paused:    subscriber.Paused,
		CreatedAt: subscriber.CreatedAt,
		DeletedAt: subscriber.DeletedAt,
	}
}

func fromPersistenceSubscriber(subscriber persistence.Subscriber) Subscriber {
	return Subscriber{
		ID:        subscriber.ID,
		TopicID:   subscriber.TopicID,
		TopicName: subscriber.TopicName,
		Name:      subscriber.Name,
		Options:   fromPersistenceSubscriberOptions(subscriber.Options),
		Paused:    subscriber.Paused,
		CreatedAt: subscriber.CreatedAt,
		DeletedAt: subscriber.DeletedAt,
	}
}

func toPersistenceSubscribers(subscribers Subscribers) persistence.Subscribers {
	result := make(persistence.Subscribers, len(subscribers))
	for index, subscriber := range subscribers {
		result[index] = toPersistenceSubscriber(subscriber)
	}
	return result
}

func (d *db) getTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	rows, err := d.persistence.GetTopics(ctx, persistence.TopicFilter{
		Names:       filter.Names,
		WithDeleted: filter.WithDeleted,
	})
	if err != nil {
		return nil, err
	}
	result := make(Topics, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceTopic(row)
	}
	return result, nil
}

func (d *db) listTopics(ctx context.Context, limit int, afterName, afterID string) (Topics, error) {
	rows, err := d.persistence.ListTopics(ctx, limit, afterName, afterID)
	if err != nil {
		return nil, err
	}
	result := make(Topics, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceTopic(row)
	}
	return result, nil
}

func (d *db) getSubscribers(ctx context.Context, filter subscriberFilter) (Subscribers, error) {
	rows, err := d.persistence.GetSubscribers(ctx, persistence.SubscriberFilter{
		TopicIDs:    filter.TopicIDs,
		Names:       filter.Names,
		WithDeleted: filter.WithDeleted,
	})
	if err != nil {
		return nil, err
	}
	result := make(Subscribers, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceSubscriber(row)
	}
	return result, nil
}

func (d *db) createTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return d.persistence.CreateTopic(ctx, toPersistenceTopic(topic), toPersistenceSubscribers(subscribers))
}

func (d *db) createSubscribers(ctx context.Context, subscribers Subscribers) error {
	return d.persistence.CreateSubscribers(ctx, toPersistenceSubscribers(subscribers))
}

func (d *db) deleteTopic(ctx context.Context, topicID uuid.UUID) error {
	return d.persistence.DeleteTopic(ctx, topicID)
}

func (d *db) deleteSubscriber(ctx context.Context, topicID, subscriberID uuid.UUID, name string) error {
	return d.persistence.DeleteSubscriber(ctx, topicID, subscriberID, name)
}

func (d *db) setTopicPaused(ctx context.Context, topicID uuid.UUID, paused bool) error {
	return d.persistence.SetTopicPaused(ctx, topicID, paused)
}

func (d *db) setSubscriberPaused(ctx context.Context, subscriberID uuid.UUID, paused bool) error {
	return d.persistence.SetSubscriberPaused(ctx, subscriberID, paused)
}

func (d *db) getTopicSubscriberQueueStats(ctx context.Context, topicID uuid.UUID) (map[uuid.UUID]SubscriberQueueStats, error) {
	rows, err := d.persistence.TopicSubscriberQueueStats(ctx, topicID)
	if err != nil {
		return nil, err
	}
	result := make(map[uuid.UUID]SubscriberQueueStats, len(rows))
	for id, row := range rows {
		result[id] = SubscriberQueueStats{Pending: row.Pending, Delivered: row.Delivered}
	}
	return result, nil
}

func (d *db) listSubscriberStatuses(ctx context.Context, topicID uuid.UUID, limit int, afterName, afterID string) ([]subscriberStatusRow, error) {
	rows, err := d.persistence.ListSubscriberStatuses(ctx, topicID, limit, afterName, afterID)
	if err != nil {
		return nil, err
	}
	result := make([]subscriberStatusRow, len(rows))
	for index, row := range rows {
		result[index] = subscriberStatusRow{
			ID:        row.ID,
			Name:      row.Name,
			Pending:   row.Pending,
			Delivered: row.Delivered,
		}
	}
	return result, nil
}

func (d *db) pruneDeletedTopology(ctx context.Context, budget time.Duration) (int64, bool, bool, error) {
	return d.persistence.PruneDeletedTopology(ctx, budget)
}
func (d *db) pruneProcessedMessages(ctx context.Context, retention time.Duration) error {
	return d.persistence.PruneProcessedMessages(ctx, retention)
}
func (d *db) pruneScheduleRuns(ctx context.Context, retention time.Duration) error {
	return d.persistence.PruneScheduleRuns(ctx, retention)
}
func (d *db) pruneDeadLetters(ctx context.Context, retention time.Duration) error {
	return d.persistence.PruneDeadLetters(ctx, retention)
}

func (d *db) persistWriteRequests(ctx context.Context, requests []writeRequest) (persistWriteResult, error) {
	return d.persistence.PersistWriteRequests(ctx, requests)
}
func (d *db) persistWriteRequestsWithTx(ctx context.Context, tx *sql.Tx, requests []writeRequest) (persistWriteResult, error) {
	return d.persistence.PersistWriteRequestsWithTx(ctx, tx, requests)
}

func (d *db) claimDeliveries(ctx context.Context, subscriberID uuid.UUID, limit int, lease time.Duration) ([]deliveryRow, error) {
	return d.persistence.ClaimDeliveries(ctx, subscriberID, limit, lease)
}
func (d *db) reapExpiredDeliveries(ctx context.Context, limit int) (int64, error) {
	return d.persistence.ReapExpiredDeliveries(ctx, limit)
}
func (d *db) nextLeaseExpiry(ctx context.Context) (time.Time, time.Time, bool, error) {
	return d.persistence.NextLeaseExpiry(ctx)
}
func (d *db) ackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string) error {
	return d.persistence.AckDelivery(ctx, subscriberID, messageID, receipt)
}
func (d *db) ackDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string) error {
	return d.persistence.AckDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt)
}
func (d *db) batchAckDeliveries(ctx context.Context, subscriberID uuid.UUID, requests []BatchAckItem) ([]error, error) {
	items := make([]persistence.BatchAckItem, len(requests))
	for index, request := range requests {
		items[index] = persistence.BatchAckItem{MessageID: request.MessageID, ReceiptToken: request.ReceiptToken}
	}
	return d.persistence.BatchAckDeliveries(ctx, subscriberID, items)
}
func (d *db) nackDelivery(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	return d.persistence.NackDelivery(ctx, subscriberID, messageID, receipt, delay, errorText)
}
func (d *db) nackDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration, errorText string) (bool, error) {
	return d.persistence.NackDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, delay, errorText)
}
func (d *db) batchNackDeliveries(ctx context.Context, subscriberID uuid.UUID, requests []BatchNackItem) ([]bool, []error, error) {
	items := make([]persistence.BatchNackItem, len(requests))
	for index, request := range requests {
		items[index] = persistence.BatchNackItem{
			MessageID:    request.MessageID,
			ReceiptToken: request.ReceiptToken,
			RetryDelay:   request.RetryDelay,
			Error:        request.Error,
		}
	}
	return d.persistence.BatchNackDeliveries(ctx, subscriberID, items)
}
func (d *db) extendDeliveryLease(ctx context.Context, subscriberID uuid.UUID, messageID, receipt string, extension time.Duration) (time.Time, error) {
	return d.persistence.ExtendDeliveryLease(ctx, subscriberID, messageID, receipt, extension)
}
func (d *db) nextDeliveryWake(ctx context.Context, subscriberID uuid.UUID) (time.Time, time.Time, bool, error) {
	return d.persistence.NextDeliveryWake(ctx, subscriberID)
}
func (d *db) listDeliveries(ctx context.Context, subscriberID uuid.UUID, deadLetter bool, limit int, cursor string) ([]deliveryRow, error) {
	return d.persistence.ListDeliveries(ctx, subscriberID, deadLetter, limit, cursor)
}
func (d *db) batchReplayDeadLetters(ctx context.Context, subscriberID uuid.UUID, messageIDs []string) ([]bool, error) {
	return d.persistence.BatchReplayDeadLetters(ctx, subscriberID, messageIDs)
}

func (d *db) snoozeDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt string, delay time.Duration) (time.Time, error) {
	return d.persistence.SnoozeDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, delay)
}
func (d *db) cancelDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, reason string) (string, error) {
	return d.persistence.CancelDeliveryWithTx(ctx, tx, subscriberID, messageID, reason)
}
func (d *db) cancelClaimedDeliveryWithTx(ctx context.Context, tx *sql.Tx, subscriberID uuid.UUID, messageID, receipt, reason string) error {
	return d.persistence.CancelClaimedDeliveryWithTx(ctx, tx, subscriberID, messageID, receipt, reason)
}
func (d *db) cancelMessageWithTx(ctx context.Context, tx *sql.Tx, topicID uuid.UUID, messageID, reason string) ([]persistence.CancellationRow, error) {
	return d.persistence.CancelMessageWithTx(ctx, tx, topicID, messageID, reason)
}
func (d *db) listDeliveryErrors(ctx context.Context, subscriberID uuid.UUID, messageID string, limit int, cursor string) ([]DeliveryError, error) {
	rows, err := d.persistence.ListDeliveryErrors(ctx, subscriberID, messageID, limit, cursor)
	if err != nil {
		return nil, err
	}
	result := make([]DeliveryError, len(rows))
	for index, row := range rows {
		result[index] = DeliveryError{
			ID:           row.ID,
			MessageID:    row.MessageID,
			SubscriberID: row.SubscriberID,
			FailureCount: row.FailureCount,
			Error:        row.Error,
			FailedAt:     row.FailedAt,
		}
	}
	return result, nil
}
func (d *db) getMessageStatus(ctx context.Context, topicID uuid.UUID, messageID string) (MessageStatus, error) {
	row, err := d.persistence.GetMessageStatus(ctx, topicID, messageID)
	if err != nil {
		return MessageStatus{}, err
	}
	deliveries := make([]MessageDeliveryStatus, len(row.Deliveries))
	for index, delivery := range row.Deliveries {
		deliveries[index] = MessageDeliveryStatus{
			SubscriberID:  delivery.SubscriberID,
			Subscriber:    delivery.Subscriber,
			Status:        delivery.Status,
			DeliveryCount: delivery.DeliveryCount,
			FailureCount:  delivery.FailureCount,
			VisibleAt:     delivery.VisibleAt,
			ProcessedAt:   delivery.ProcessedAt,
			CancelledAt:   delivery.CancelledAt,
			CancelReason:  delivery.CancelReason,
		}
	}
	return MessageStatus{
		ID:             row.ID,
		TopicID:        row.TopicID,
		Message:        row.Message,
		Headers:        row.Headers,
		CorrelationID:  row.CorrelationID,
		IdempotencyKey: row.IdempotencyKey,
		Priority:       row.Priority,
		ScheduledAt:    row.ScheduledAt,
		CreatedAt:      row.CreatedAt,
		Deliveries:     deliveries,
	}, nil
}

func toPersistenceSchedule(schedule Schedule) persistence.Schedule {
	return persistence.Schedule{
		ID:             schedule.ID,
		TopicID:        schedule.TopicID,
		Name:           schedule.Name,
		CronExpression: schedule.CronExpression,
		Timezone:       schedule.Timezone,
		Message:        schedule.Message,
		Headers:        schedule.Headers,
		CorrelationID:  schedule.CorrelationID,
		Priority:       schedule.Priority,
		MisfirePolicy:  schedule.MisfirePolicy,
		OverlapPolicy:  schedule.OverlapPolicy,
		Paused:         schedule.Paused,
		Version:        schedule.Version,
		NextRunAt:      schedule.NextRunAt,
		OwnerID:        schedule.OwnerID,
		LeaseExpiresAt: schedule.LeaseExpiresAt,
		FencingToken:   schedule.FencingToken,
		CreatedAt:      schedule.CreatedAt,
		UpdatedAt:      schedule.UpdatedAt,
		ClaimedAt:      schedule.claimedAt,
	}
}

func fromPersistenceSchedule(schedule persistence.Schedule) Schedule {
	return Schedule{
		ID:             schedule.ID,
		TopicID:        schedule.TopicID,
		Name:           schedule.Name,
		CronExpression: schedule.CronExpression,
		Timezone:       schedule.Timezone,
		Message:        schedule.Message,
		Headers:        schedule.Headers,
		CorrelationID:  schedule.CorrelationID,
		Priority:       schedule.Priority,
		MisfirePolicy:  schedule.MisfirePolicy,
		OverlapPolicy:  schedule.OverlapPolicy,
		Paused:         schedule.Paused,
		Version:        schedule.Version,
		NextRunAt:      schedule.NextRunAt,
		OwnerID:        schedule.OwnerID,
		LeaseExpiresAt: schedule.LeaseExpiresAt,
		FencingToken:   schedule.FencingToken,
		CreatedAt:      schedule.CreatedAt,
		UpdatedAt:      schedule.UpdatedAt,
		claimedAt:      schedule.ClaimedAt,
	}
}

func fromPersistenceScheduleRun(run persistence.ScheduleRun) ScheduleRun {
	return ScheduleRun{
		ID:           run.ID,
		ScheduleID:   run.ScheduleID,
		MessageID:    run.MessageID,
		ScheduledFor: run.ScheduledFor,
		StartedAt:    run.StartedAt,
		FinishedAt:   run.FinishedAt,
		Status:       run.Status,
		Error:        run.Error,
		CreatedAt:    run.CreatedAt,
	}
}

func (d *db) scheduleNameExists(ctx context.Context, topicID uuid.UUID, name string) (bool, error) {
	return d.persistence.ScheduleNameExists(ctx, topicID, name)
}
func (d *db) createSchedule(ctx context.Context, schedule Schedule) error {
	return d.persistence.CreateSchedule(ctx, toPersistenceSchedule(schedule))
}
func (d *db) getSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) (Schedule, error) {
	row, err := d.persistence.GetSchedule(ctx, topicID, scheduleID)
	return fromPersistenceSchedule(row), err
}
func (d *db) listSchedules(ctx context.Context, topicID uuid.UUID) ([]Schedule, error) {
	rows, err := d.persistence.ListSchedules(ctx, topicID)
	if err != nil {
		return nil, err
	}
	result := make([]Schedule, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceSchedule(row)
	}
	return result, nil
}

func (d *db) listSchedulesPage(ctx context.Context, topicID uuid.UUID, limit int, afterName, afterID string) ([]Schedule, error) {
	rows, err := d.persistence.ListSchedulesPage(ctx, topicID, limit, afterName, afterID)
	if err != nil {
		return nil, err
	}
	result := make([]Schedule, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceSchedule(row)
	}
	return result, nil
}
func (d *db) updateSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string, expectedVersion int, schedule Schedule) error {
	return d.persistence.UpdateSchedule(ctx, topicID, scheduleID, expectedVersion, toPersistenceSchedule(schedule))
}
func (d *db) deleteSchedule(ctx context.Context, topicID uuid.UUID, scheduleID string) error {
	return d.persistence.DeleteSchedule(ctx, topicID, scheduleID)
}
func (d *db) setSchedulePaused(ctx context.Context, topicID uuid.UUID, scheduleID string, paused bool) error {
	return d.persistence.SetSchedulePaused(ctx, topicID, scheduleID, paused)
}
func (d *db) listScheduleRuns(ctx context.Context, scheduleID string, limit int, before time.Time, beforeID string) ([]ScheduleRun, error) {
	rows, err := d.persistence.ListScheduleRuns(ctx, scheduleID, limit, before, beforeID)
	if err != nil {
		return nil, err
	}
	result := make([]ScheduleRun, len(rows))
	for index, row := range rows {
		result[index] = fromPersistenceScheduleRun(row)
	}
	return result, nil
}
func (d *db) nextScheduleDue(ctx context.Context, now time.Time) (time.Time, time.Time, bool, error) {
	return d.persistence.NextScheduleDue(ctx, now)
}
func (d *db) claimDueSchedule(ctx context.Context, owner string, now time.Time, lease time.Duration) (Schedule, bool, error) {
	row, claimed, err := d.persistence.ClaimDueSchedule(ctx, owner, now, lease)
	return fromPersistenceSchedule(row), claimed, err
}
func (d *db) persistScheduleOccurrence(ctx context.Context, claimed Schedule, scheduledFor, nextRunAt time.Time, force, advance bool, owner string) (ScheduleRun, error) {
	row, err := d.persistence.PersistScheduleOccurrence(
		ctx, toPersistenceSchedule(claimed), scheduledFor, nextRunAt, force, advance, owner,
	)
	return fromPersistenceScheduleRun(row), err
}

func (d *db) failScheduleOccurrence(ctx context.Context, claimed Schedule, scheduledFor, nextRunAt time.Time, advance bool, owner, failure string) (ScheduleRun, error) {
	row, err := d.persistence.FailScheduleOccurrence(
		ctx, toPersistenceSchedule(claimed), scheduledFor, nextRunAt, advance, owner, failure,
	)
	return fromPersistenceScheduleRun(row), err
}
