package memlru

import (
	"sync"
	"time"
)

type expirationQueueItem struct {
	key       string
	expiresAt time.Time
}

type expirationQueue struct {
	keys []*expirationQueueItem
	mu   sync.Mutex
}

func (e *expirationQueue) Push(key string, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	newItem := &expirationQueueItem{
		key:       key,
		expiresAt: now.Add(ttl),
	}

	e.keys = append(e.keys, nil) // make room for a new item

	after := len(e.keys) // append at the end by default
	for i, key := range e.keys {
		if key != nil && key.expiresAt.Before(newItem.expiresAt) {
			continue
		}
		after = i // found a better position
		break
	}

	copy(e.keys[after+1:], e.keys[after:]) // re-organize tail
	e.keys[after] = newItem                // insert item in place
}

func (e *expirationQueue) Len() int {
	return len(e.keys)
}

func (e *expirationQueue) Expired() []string {
	return e.expiredAt(time.Now())
}

func (e *expirationQueue) expiredAt(t time.Time) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	keys := []string{}

	for _, key := range e.keys {
		if key.expiresAt.After(t) {
			break
		}
		keys = append(keys, key.key)
	}

	// keep keys newer than t
	e.keys = e.keys[len(keys):]
	return keys
}

func newExpirationQueue() *expirationQueue {
	return &expirationQueue{keys: []*expirationQueueItem{}}
}
