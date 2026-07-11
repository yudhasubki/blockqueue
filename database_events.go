package blockqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yudhasubki/blockqueue/store"
)

// startDatabaseEvents consumes PostgreSQL notifications as wake-up hints. A
// bounded reconciliation loop remains authoritative when notifications are
// missing, disconnected, or unsupported.
func (q *Queue) startDatabaseEvents() {
	if !q.db.dialect.usesDatabaseEvents() {
		return
	}
	var events <-chan string
	if source, ok := q.db.Database.(store.NotificationSource); ok {
		channel, err := source.Listen(q.serverCtx, databaseEventChannel)
		if err != nil {
			slog.Warn("postgres notifications unavailable; using reconciliation polling", "error", err)
		} else {
			events = channel
		}
	}
	reconcile := time.NewTicker(5 * time.Second)
	defer reconcile.Stop()
	for {
		select {
		case <-q.serverCtx.Done():
			return
		case <-reconcile.C:
			if err := q.reloadRuntime(q.serverCtx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("topology reconciliation failed", "error", err)
			}
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			switch {
			case event == "topology":
				if err := q.reloadRuntime(q.serverCtx); err != nil && !errors.Is(err, context.Canceled) {
					slog.Warn("topology notification reload failed", "error", err)
				}
			case event == "scheduler":
				q.signalScheduler()
			case strings.HasPrefix(event, "delivery:"):
				if topicID, err := uuid.Parse(strings.TrimPrefix(event, "delivery:")); err == nil {
					q.notify(topicID)
				}
			}
			q.signalReaper()
		}
	}
}

func (q *Queue) reloadRuntime(ctx context.Context) error {
	version := q.topologyVersion.Load()
	topics, err := q.db.getTopics(ctx, TopicFilter{})
	if err != nil {
		return err
	}
	subscribers, err := q.db.getSubscribers(ctx, subscriberFilter{})
	if err != nil {
		return err
	}
	byTopic := subscribers.mapByTopic()
	next := &topicRegistry{
		byName: make(map[string]*topicRuntime, len(topics)),
		byID:   make(map[uuid.UUID]*topicRuntime, len(topics)),
	}
	for _, topic := range topics {
		runtime, err := buildTopicRuntime(topic, byTopic[topic.ID])
		if err != nil {
			return fmt.Errorf("build runtime for topic %s: %w", topic.Name, err)
		}
		next.byName[runtime.name] = runtime
		next.byID[runtime.id] = runtime
	}

	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	if q.topologyVersion.Load() != version {
		// A local topology commit won the race. A later reconciliation will
		// observe it; never replace the registry with this stale DB snapshot.
		return nil
	}
	q.registry.Store(reuseRuntimeObjects(q.registry.Load(), next))
	q.topologyVersion.Add(1)
	return nil
}

// reuseRuntimeObjects preserves delivery wake channels across PostgreSQL
// reconciliation. Only changed subscribers receive a new runtime object.
func reuseRuntimeObjects(current, candidate *topicRegistry) *topicRegistry {
	next := &topicRegistry{
		byName: make(map[string]*topicRuntime, len(candidate.byName)),
		byID:   make(map[uuid.UUID]*topicRuntime, len(candidate.byID)),
	}
	for id, freshTopic := range candidate.byID {
		existingTopic := current.byID[id]
		if existingTopic == nil || existingTopic.name != freshTopic.name {
			next.byID[id] = freshTopic
			next.byName[freshTopic.name] = freshTopic
			continue
		}
		existingTopic.paused.Store(freshTopic.paused.Load())
		existingTopic.deleted.Store(false)
		oldSubscribers := existingTopic.registry.Load()
		freshSubscribers := freshTopic.registry.Load()
		reconciled := &subscriberRegistry{
			byID:   make(map[uuid.UUID]*subscriberRuntime, len(freshSubscribers.byID)),
			byName: make(map[string]*subscriberRuntime, len(freshSubscribers.byName)),
		}
		for subscriberID, freshSubscriber := range freshSubscribers.byID {
			existingSubscriber := oldSubscribers.byID[subscriberID]
			if existingSubscriber != nil && existingSubscriber.name == freshSubscriber.name &&
				existingSubscriber.options == freshSubscriber.options {
				existingSubscriber.paused.Store(freshSubscriber.paused.Load())
				existingSubscriber.deleted.Store(false)
				reconciled.byID[subscriberID] = existingSubscriber
				reconciled.byName[existingSubscriber.name] = existingSubscriber
				continue
			}
			reconciled.byID[subscriberID] = freshSubscriber
			reconciled.byName[freshSubscriber.name] = freshSubscriber
		}
		for subscriberID, oldSubscriber := range oldSubscribers.byID {
			if replacement, exists := reconciled.byID[subscriberID]; exists && replacement == oldSubscriber {
				continue
			}
			oldSubscriber.notify()
			oldSubscriber.deleted.Store(true)
		}
		existingTopic.registry.Store(reconciled)
		next.byID[id] = existingTopic
		next.byName[existingTopic.name] = existingTopic
	}
	for id, oldTopic := range current.byID {
		if _, exists := next.byID[id]; exists {
			continue
		}
		oldTopic.deleted.Store(true)
		oldTopic.notify()
	}
	return next
}
