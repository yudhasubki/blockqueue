package blockqueue

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/jmoiron/sqlx"
)

const defaultStatementCacheSize = 64

type statementCacheEntry struct {
	query string
	stmt  *sqlx.Stmt
}

// statementCache is a bounded LRU of database-level prepared statements.
// Transaction-scoped statements are derived from these entries and are closed
// by their transaction. Queue shutdown closes every database-level statement.
type statementCache struct {
	mu       sync.Mutex
	capacity int
	lru      *list.List
	entries  map[string]*list.Element
	closed   bool
}

func newStatementCache(capacity int) *statementCache {
	if capacity <= 0 {
		capacity = defaultStatementCacheSize
	}
	return &statementCache{
		capacity: capacity,
		lru:      list.New(),
		entries:  make(map[string]*list.Element, capacity),
	}
}

func (cache *statementCache) get(ctx context.Context, connection *sqlx.DB, query string) (*sqlx.Stmt, error) {
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, ErrWriterClosed
	}
	if element, ok := cache.entries[query]; ok {
		cache.lru.MoveToFront(element)
		statement := element.Value.(*statementCacheEntry).stmt
		cache.mu.Unlock()
		return statement, nil
	}
	cache.mu.Unlock()

	prepared, err := connection.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		_ = prepared.Close()
		return nil, ErrWriterClosed
	}
	// Another goroutine may have prepared the same statement while this one was
	// outside the lock. Keep the existing entry and close the redundant handle.
	if element, ok := cache.entries[query]; ok {
		_ = prepared.Close()
		cache.lru.MoveToFront(element)
		return element.Value.(*statementCacheEntry).stmt, nil
	}
	element := cache.lru.PushFront(&statementCacheEntry{query: query, stmt: prepared})
	cache.entries[query] = element
	if cache.lru.Len() > cache.capacity {
		oldest := cache.lru.Back()
		entry := oldest.Value.(*statementCacheEntry)
		delete(cache.entries, entry.query)
		cache.lru.Remove(oldest)
		_ = entry.stmt.Close()
	}
	return prepared, nil
}

func (cache *statementCache) close() error {
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil
	}
	cache.closed = true
	statements := make([]*sqlx.Stmt, 0, len(cache.entries))
	for _, element := range cache.entries {
		statements = append(statements, element.Value.(*statementCacheEntry).stmt)
	}
	cache.entries = make(map[string]*list.Element)
	cache.lru.Init()
	cache.mu.Unlock()

	var result error
	for _, statement := range statements {
		result = errors.Join(result, statement.Close())
	}
	return result
}

func (cache *statementCache) len() int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.lru.Len()
}
