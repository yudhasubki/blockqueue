package blockqueue

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (q *Queue) GetTopics(ctx context.Context, filter TopicFilter) (Topics, error) {
	return q.getTopics(ctx, filter)
}

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

func (q *Queue) CreateTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return q.createTopic(ctx, topic, subscribers)
}

func (q *Queue) DeleteTopic(ctx context.Context, topic Topic) error {
	return q.deleteTopic(ctx, topic)
}

func (q *Queue) GetSubscribersStatus(ctx context.Context, topic Topic) (SubscriberStatuses, error) {
	return q.getSubscribersStatus(ctx, topic)
}

func (q *Queue) CreateSubscribers(ctx context.Context, topic Topic, subscribers Subscribers) error {
	return q.createSubscribers(ctx, topic, subscribers)
}

func (q *Queue) DeleteSubscriber(ctx context.Context, topic Topic, subscriber string) error {
	return q.deleteSubscriber(ctx, topic, subscriber)
}

func (q *Queue) createTopic(ctx context.Context, topic Topic, subscribers Subscribers) error {
	if q.State() != LifecycleRunning {
		return ErrQueueNotRunning
	}
	candidate, err := buildTopicRuntime(topic, subscribers)
	if err != nil {
		return fmt.Errorf("validate topic runtime: %w", err)
	}
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	if _, exists := q.registry.Load().byName[topic.Name]; exists {
		return ErrResourceConflict
	}
	if err := q.db.createTopic(ctx, topic, subscribers); err != nil {
		return err
	}
	q.storeTopic(candidate)
	return nil
}

func (q *Queue) deleteTopic(ctx context.Context, topic Topic) error {
	if q.State() == LifecycleStopping || q.State() == LifecycleStopped {
		return ErrQueueStopping
	}
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	if q.State() == LifecycleRunning && q.writer != nil {
		if err := q.writer.Barrier(ctx); err != nil {
			return err
		}
	}

	current := q.registry.Load()
	runtime, exists := current.byName[topic.Name]
	if !exists {
		return ErrTopicNotFound
	}
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
	q.removeTopic(runtime)
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
	if q.State() == LifecycleStopping || q.State() == LifecycleStopped {
		return ErrQueueStopping
	}
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return ErrTopicNotFound
	}
	if q.State() == LifecycleRunning && q.writer != nil {
		if err := q.writer.Barrier(ctx); err != nil {
			return err
		}
	}
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
	if q.State() == LifecycleStopping || q.State() == LifecycleStopped {
		return ErrQueueStopping
	}
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	runtime, exists := q.getTopicRuntime(topic)
	if !exists {
		return ErrTopicNotFound
	}
	if q.State() == LifecycleRunning && q.writer != nil {
		if err := q.writer.Barrier(ctx); err != nil {
			return err
		}
	}
	subscriber, ok := runtime.subscriberByName(subscriberName)
	if !ok {
		return ErrSubscriberNotFound
	}
	if err := q.db.deleteSubscriber(ctx, runtime.id, subscriber.id, subscriber.name); err != nil {
		return err
	}
	runtime.removeSubscriber(subscriber)
	q.topologyVersion.Add(1)
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

func (q *Queue) removeTopic(runtime *topicRuntime) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

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
