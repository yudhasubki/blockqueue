package blockqueue

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// topicMutation blocks admissions only for the affected topic. It registers
// itself before shutdown can begin, places a writer barrier after every older
// admission for that topic, and holds the registry mutex only for the final DB
// mutation and atomic runtime swap.
type topicMutation struct {
	queue          *Queue
	runtime        *topicRuntime
	registryLocked bool
}

func (q *Queue) beginControlOperation() error {
	q.admissionMu.RLock()
	defer q.admissionMu.RUnlock()
	if err := q.requireRunning(); err != nil {
		return err
	}
	q.controlOps.Add(1)
	return nil
}

func (q *Queue) beginTopicMutation(topic Topic) (*topicMutation, error) {
	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return nil, ErrTopicNotFound
	}
	runtime.admissionMu.Lock()
	if current := q.registry.Load().byName[topic.Name]; current != runtime || runtime.deleted.Load() {
		runtime.admissionMu.Unlock()
		return nil, ErrTopicNotFound
	}
	if err := q.beginControlOperation(); err != nil {
		runtime.admissionMu.Unlock()
		return nil, err
	}
	return &topicMutation{queue: q, runtime: runtime}, nil
}

func (mutation *topicMutation) barrierAndLock(ctx context.Context) error {
	if mutation.queue.writer != nil {
		if err := mutation.queue.writer.Barrier(ctx); err != nil {
			return err
		}
	}
	mutation.queue.mtx.Lock()
	mutation.registryLocked = true
	if current := mutation.queue.registry.Load().byName[mutation.runtime.name]; current != mutation.runtime || mutation.runtime.deleted.Load() {
		return ErrTopicNotFound
	}
	return nil
}

func (mutation *topicMutation) close() {
	if mutation.registryLocked {
		mutation.queue.mtx.Unlock()
	}
	mutation.queue.controlOps.Done()
	mutation.runtime.admissionMu.Unlock()
}

// GetTopics returns all topics matching filter. Prefer ListTopics for bounded
// operator-facing enumeration.
func (q *Queue) GetTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	return q.getTopics(ctx, filter)
}

// ListTopics returns a bounded cursor page ordered by name and ID.
func (q *Queue) ListTopics(ctx context.Context, limit int, cursor string) (TopicPage, error) {
	if err := q.requireRunning(); err != nil {
		return TopicPage{}, err
	}
	limit = normalizedResourcePageLimit(limit)
	var afterName, afterID string
	if cursor != "" {
		var err error
		afterName, afterID, err = decodeResourceCursor(cursor)
		if err != nil {
			return TopicPage{}, ErrInvalidCursor
		}
		if _, err := uuid.Parse(afterID); err != nil {
			return TopicPage{}, ErrInvalidCursor
		}
	}
	rows, err := q.db.listTopics(ctx, limit+1, afterName, afterID)
	if err != nil {
		return TopicPage{}, err
	}
	page := TopicPage{Topics: rows}
	if len(rows) > limit {
		last := rows[limit-1]
		page.Topics = rows[:limit]
		page.NextCursor = encodeResourceCursor(last.Name, last.ID.String())
	}
	return page, nil
}

// GetTopic reads one active topic from the immutable runtime registry.
func (q *Queue) GetTopic(topicName string) (Topic, bool) {
	topic, exists := q.registry.Load().byName[topicName]
	if !exists {
		return Topic{}, false
	}
	return Topic{
		ID:     topic.id,
		Name:   topic.name,
		Paused: topic.paused.Load(),
	}, true
}

// CreateTopic atomically persists a topic and its initial subscribers.
func (q *Queue) CreateTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return q.createTopic(ctx, topic, subscribers)
}

// DeleteTopic logically removes a topic after fencing earlier admissions.
// Physical rows are reclaimed asynchronously in bounded chunks.
func (q *Queue) DeleteTopic(ctx context.Context, topic Topic) error {
	return q.deleteTopic(ctx, topic)
}

// GetSubscribersStatus returns all subscriber queue-depth summaries.
func (q *Queue) GetSubscribersStatus(ctx context.Context, topic Topic) (SubscriberStatuses, error) {
	return q.getSubscribersStatus(ctx, topic)
}

// ListSubscriberStatuses returns a bounded cursor page ordered by name and ID.
func (q *Queue) ListSubscriberStatuses(ctx context.Context, topic Topic, limit int, cursor string) (SubscriberStatusPage, error) {
	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return SubscriberStatusPage{}, ErrTopicNotFound
	}
	limit = normalizedResourcePageLimit(limit)
	var afterName, afterID string
	if cursor != "" {
		var err error
		afterName, afterID, err = decodeResourceCursor(cursor)
		if err != nil {
			return SubscriberStatusPage{}, ErrInvalidCursor
		}
		if _, err := uuid.Parse(afterID); err != nil {
			return SubscriberStatusPage{}, ErrInvalidCursor
		}
	}
	rows, err := q.db.listSubscriberStatuses(ctx, runtime.id, limit+1, afterName, afterID)
	if err != nil {
		return SubscriberStatusPage{}, err
	}
	page := SubscriberStatusPage{Subscribers: make(SubscriberStatuses, 0, min(limit, len(rows)))}
	for index, row := range rows {
		if index == limit {
			break
		}
		page.Subscribers = append(page.Subscribers, SubscriberStatus{
			TopicID:            runtime.id,
			Name:               row.Name,
			UnpublishedMessage: row.Pending,
			UnackedMessage:     row.Delivered,
		})
	}
	if len(rows) > limit {
		last := rows[limit-1]
		page.NextCursor = encodeResourceCursor(last.Name, last.ID)
	}
	return page, nil
}

// CreateSubscribers atomically adds subscribers to an existing topic.
func (q *Queue) CreateSubscribers(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return q.createSubscribers(ctx, topic, subscribers)
}

// DeleteSubscriber logically removes one subscriber after fencing earlier
// admissions. Physical deliveries are reclaimed asynchronously.
func (q *Queue) DeleteSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.deleteSubscriber(ctx, topic, subscriber)
}

func (q *Queue) createTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	candidate, err := buildTopicRuntime(topic, subscribers)
	if err != nil {
		return fmt.Errorf("validate topic runtime: %w", err)
	}
	if err := q.beginControlOperation(); err != nil {
		return err
	}
	defer q.controlOps.Done()
	q.mtx.Lock()
	defer q.mtx.Unlock()
	if _, exists := q.registry.Load().byName[topic.Name]; exists {
		return ErrResourceConflict
	}
	if err := q.db.createTopic(ctx, topic, subscribers); err != nil {
		return err
	}
	q.storeTopicLocked(candidate)
	return nil
}

func (q *Queue) deleteTopic(ctx context.Context, topic Topic) error {
	mutation, err := q.beginTopicMutation(topic)
	if err != nil {
		return err
	}
	defer mutation.close()
	if err := mutation.barrierAndLock(ctx); err != nil {
		return err
	}
	runtime := mutation.runtime
	if err := q.db.deleteTopic(ctx, runtime.id); err != nil {
		return err
	}
	runtime.deleted.Store(true)
	for _, subscriber := range runtime.registry.Load().byID {
		select {
		case subscriber.deliveryWake <- struct{}{}:
		default:
		}
		subscriber.deleted.Store(true)
	}
	q.removeTopicLocked(runtime)
	q.signalPruner()
	return nil
}

func (q *Queue) getSubscribersStatus(ctx context.Context, topic Topic) (SubscriberStatuses, error) {
	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return SubscriberStatuses{}, ErrTopicNotFound
	}
	return runtime.subscriberStatus(ctx, q.db)
}

func (q *Queue) createSubscribers(ctx context.Context, topic Topic, subscribers Subscribers) error {
	mutation, err := q.beginTopicMutation(topic)
	if err != nil {
		return err
	}
	defer mutation.close()
	if err := mutation.barrierAndLock(ctx); err != nil {
		return err
	}
	runtime := mutation.runtime
	prepared, err := runtime.prepareSubscribers(subscribers)
	if err != nil {
		return err
	}
	if err := q.db.createSubscribers(ctx, subscribers); err != nil {
		return err
	}
	runtime.addPreparedSubscribers(prepared)
	q.topologyVersion.Add(1)
	runtime.notify()
	return nil
}

func (q *Queue) deleteSubscriber(ctx context.Context, topic Topic, subscriberName string) error {
	mutation, err := q.beginTopicMutation(topic)
	if err != nil {
		return err
	}
	defer mutation.close()
	if err := mutation.barrierAndLock(ctx); err != nil {
		return err
	}
	runtime := mutation.runtime
	subscriber, ok := runtime.subscriberByName(subscriberName)
	if !ok {
		return ErrSubscriberNotFound
	}
	if err := q.db.deleteSubscriber(ctx, runtime.id, subscriber.id, subscriber.name); err != nil {
		return err
	}
	runtime.removeSubscriber(subscriber)
	q.topologyVersion.Add(1)
	q.signalPruner()
	return nil
}

func (q *Queue) getTopicRuntime(topic Topic) (*topicRuntime, bool) {
	runtime, exists := q.registry.Load().byName[topic.Name]
	if !exists {
		return nil, false
	}
	return runtime, !runtime.deleted.Load()
}

func (q *Queue) getTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	return q.db.getTopics(ctx, filter)
}

func (q *Queue) notify(topicID uuid.UUID) {
	runtime := q.registry.Load().byID[topicID]
	if runtime != nil {
		runtime.notify()
	}
}

func (q *Queue) storeTopic(runtime *topicRuntime) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.storeTopicLocked(runtime)
}

func (q *Queue) storeTopicLocked(runtime *topicRuntime) {
	current := q.registry.Load()
	next := &topicRegistry{
		byName: make(map[string]*topicRuntime, len(current.byName)+1),
		byID:   make(map[uuid.UUID]*topicRuntime, len(current.byID)+1),
	}
	for name, existing := range current.byName {
		next.byName[name] = existing
	}
	for id, existing := range current.byID {
		next.byID[id] = existing
	}
	next.byName[runtime.name] = runtime
	next.byID[runtime.id] = runtime
	q.registry.Store(next)
	q.topologyVersion.Add(1)
}

func (q *Queue) removeTopicLocked(runtime *topicRuntime) {
	current := q.registry.Load()
	next := &topicRegistry{
		byName: make(map[string]*topicRuntime, len(current.byName)-1),
		byID:   make(map[uuid.UUID]*topicRuntime, len(current.byID)-1),
	}
	for name, existing := range current.byName {
		if name != runtime.name {
			next.byName[name] = existing
		}
	}
	for id, existing := range current.byID {
		if id != runtime.id {
			next.byID[id] = existing
		}
	}
	q.registry.Store(next)
	q.topologyVersion.Add(1)
}
