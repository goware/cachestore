package memlru

import (
	"sync"
	"time"
)

type expirationQueue struct {
	keys      []expirationQueueItem
	lastCheck time.Time
	mu        sync.RWMutex
}

type expirationQueueItem struct {
	key       string
	expiresAt time.Time
}

func newExpirationQueue() *expirationQueue {
	return &expirationQueue{keys: []expirationQueueItem{}}
}

func (e *expirationQueue) Push(key string, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	newItem := expirationQueueItem{
		key:       key,
		expiresAt: now.Add(ttl),
	}

	index := 0
	for i := 0; i < len(e.keys); i++ {
		key := e.keys[i]

		if key.key == newItem.key {
			// found a key with the same name, remove it
			e.keys = append(e.keys[:i], e.keys[i+1:]...)
			i--
			continue
		}

		if key.expiresAt.Before(newItem.expiresAt) {
			index = i + 1
		}
	}

	e.keys = append(e.keys, expirationQueueItem{}) // make room for a new item
	copy(e.keys[index+1:], e.keys[index:])         // re-organize tail
	e.keys[index] = newItem                        // insert item in place
}

func (e *expirationQueue) Len() int {
	return len(e.keys)
}

func (e *expirationQueue) Expired() []string {
	return e.expiredAt(time.Now())
}

func (e *expirationQueue) ShouldExpire() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	return e.lastCheck.Add(lastExpiryCheckInterval).Before(now)
}

func (e *expirationQueue) UpdateLastCheckTime() {
	e.mu.Lock()
	e.lastCheck = time.Now()
	e.mu.Unlock()
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
