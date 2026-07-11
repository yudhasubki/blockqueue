package blockqueue

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/google/uuid"
)

// topicRuntime is an in-memory routing snapshot. Database transactions remain
// authoritative for persistence and delivery ownership.
type topicRuntime struct {
	id       uuid.UUID
	name     string
	paused   atomic.Bool
	deleted  atomic.Bool
	registry atomic.Pointer[subscriberRegistry]
	mu       sync.Mutex
}

type subscriberRegistry struct {
	byID   map[uuid.UUID]*subscriberRuntime
	byName map[string]*subscriberRuntime
}

func loadTopicRuntime(ctx context.Context, topic Topic, database *db) (*topicRuntime, error) {
	subscribers, err := database.getSubscribers(ctx, subscriberFilter{TopicIDs: []uuid.UUID{topic.ID}})
	if err != nil {
		return nil, err
	}
	return buildTopicRuntime(topic, subscribers)
}

// buildTopicRuntime validates every subscriber before a control-plane
// transaction commits. Publishing the resulting registry is then infallible.
func buildTopicRuntime(topic Topic, subscribers Subscribers) (*topicRuntime, error) {
	if topic.ID == uuid.Nil {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidTopic)
	}
	if !validResourceName(topic.Name) {
		return nil, fmt.Errorf("%w: name must be 1-150 bytes without slashes or control characters", ErrInvalidTopic)
	}
	runtime := &topicRuntime{id: topic.ID, name: topic.Name}
	runtime.paused.Store(topic.Paused)
	registry := &subscriberRegistry{
		byID:   make(map[uuid.UUID]*subscriberRuntime, len(subscribers)),
		byName: make(map[string]*subscriberRuntime, len(subscribers)),
	}
	for _, subscriber := range subscribers {
		if err := validateSubscriber(subscriber, topic.ID); err != nil {
			return nil, err
		}
		item, err := newSubscriberRuntime(subscriber)
		if err != nil {
			return nil, err
		}
		if _, exists := registry.byID[item.id]; exists {
			return nil, ErrResourceConflict
		}
		if _, exists := registry.byName[item.name]; exists {
			return nil, ErrResourceConflict
		}
		registry.byID[item.id] = item
		registry.byName[item.name] = item
	}
	runtime.registry.Store(registry)
	return runtime, nil
}

func (topic *topicRuntime) subscriberByName(name string) (*subscriberRuntime, bool) {
	subscriber, ok := topic.registry.Load().byName[name]
	return subscriber, ok
}

func (topic *topicRuntime) prepareSubscribers(subscribers Subscribers) ([]*subscriberRuntime, error) {
	prepared := make([]*subscriberRuntime, 0, len(subscribers))
	for _, subscriber := range subscribers {
		if err := validateSubscriber(subscriber, topic.id); err != nil {
			return nil, err
		}
		item, err := newSubscriberRuntime(subscriber)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, item)
	}
	current := topic.registry.Load()
	for _, item := range prepared {
		if _, exists := current.byID[item.id]; exists {
			return nil, ErrResourceConflict
		}
		if _, exists := current.byName[item.name]; exists {
			return nil, ErrResourceConflict
		}
	}
	return prepared, nil
}

func validateSubscriber(subscriber Subscriber, topicID uuid.UUID) error {
	if subscriber.ID == uuid.Nil {
		return fmt.Errorf("%w: id is required", ErrInvalidSubscriber)
	}
	if subscriber.TopicID != topicID {
		return fmt.Errorf("%w: topic id does not match", ErrInvalidSubscriber)
	}
	if !validResourceName(subscriber.Name) {
		return fmt.Errorf("%w: name must be 1-150 bytes without slashes or control characters", ErrInvalidSubscriber)
	}
	return nil
}

func validResourceName(name string) bool {
	if strings.TrimSpace(name) != name || name == "" || len([]byte(name)) > 150 || strings.ContainsRune(name, '/') {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// addPreparedSubscribers cannot fail. Callers validate the candidate snapshot
// before committing the database mutation and serialize topology changes with
// Queue.admissionMu.
func (topic *topicRuntime) addPreparedSubscribers(prepared []*subscriberRuntime) {
	topic.mu.Lock()
	defer topic.mu.Unlock()
	current := topic.registry.Load()
	next := &subscriberRegistry{
		byID:   make(map[uuid.UUID]*subscriberRuntime, len(current.byID)+len(prepared)),
		byName: make(map[string]*subscriberRuntime, len(current.byName)+len(prepared)),
	}
	for id, item := range current.byID {
		next.byID[id] = item
	}
	for name, item := range current.byName {
		next.byName[name] = item
	}
	for _, item := range prepared {
		next.byID[item.id] = item
		next.byName[item.name] = item
	}
	topic.registry.Store(next)
}

func (topic *topicRuntime) removeSubscriber(subscriber *subscriberRuntime) {
	topic.mu.Lock()
	defer topic.mu.Unlock()
	current := topic.registry.Load()
	next := &subscriberRegistry{
		byID:   make(map[uuid.UUID]*subscriberRuntime, len(current.byID)-1),
		byName: make(map[string]*subscriberRuntime, len(current.byName)-1),
	}
	for id, item := range current.byID {
		if id != subscriber.id {
			next.byID[id] = item
		}
	}
	for name, item := range current.byName {
		if name != subscriber.name {
			next.byName[name] = item
		}
	}
	select {
	case subscriber.deliveryWake <- struct{}{}:
	default:
	}
	subscriber.deleted.Store(true)
	topic.registry.Store(next)
}

func (topic *topicRuntime) subscriberStatus(ctx context.Context, database *db) (SubscriberStatuses, error) {
	stats, err := database.getTopicSubscriberQueueStats(ctx, topic.id)
	if err != nil {
		return nil, err
	}
	registry := topic.registry.Load()
	items := make([]*subscriberRuntime, 0, len(registry.byID))
	for _, subscriber := range registry.byID {
		items = append(items, subscriber)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].name < items[j].name })
	result := make(SubscriberStatuses, 0, len(items))
	for _, subscriber := range items {
		queueStats := stats[subscriber.id]
		result = append(result, SubscriberStatus{
			TopicID: topic.id, Name: subscriber.name,
			UnpublishedMessage: queueStats.Pending, UnackedMessage: queueStats.Delivered,
		})
	}
	return result, nil
}

func (topic *topicRuntime) notify() {
	for _, subscriber := range topic.registry.Load().byID {
		subscriber.notify()
	}
}
