package memlru

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/goware/cachestore"
)

type mutexMap struct {
	mu       sync.Mutex
	keyLocks map[string]struct{}

	minRetryDelay time.Duration
	maxRetryDelay time.Duration
}

func newMutexMap(options cachestore.StoreOptions) *mutexMap {
	mm := &mutexMap{
		keyLocks:      make(map[string]struct{}),
		minRetryDelay: options.LockMinRetryDelay,
		maxRetryDelay: options.LockMaxRetryDelay,
	}
	if mm.maxRetryDelay == 0 {
		mm.maxRetryDelay = 150 * time.Millisecond
	}
	return mm
}

func (mm *mutexMap) TryLock(key string) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	_, locked := mm.keyLocks[key]
	if locked {
		return false
	}

	mm.keyLocks[key] = struct{}{}
	return true
}

func (mm *mutexMap) WaitForRetry(ctx context.Context, retryNumber int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(mm.nextDelay(retryNumber)):
		return nil
	}
}

func (mm *mutexMap) Unlock(key string) {
	mm.mu.Lock()
	delete(mm.keyLocks, key)
	mm.mu.Unlock()
}

func (mm *mutexMap) nextDelay(retryNumber int) time.Duration {
	minDelay, maxDelay := int(mm.minRetryDelay)*(retryNumber+1), int(mm.maxRetryDelay)
	return time.Duration(rand.Intn(maxDelay-minDelay) + minDelay)
}
