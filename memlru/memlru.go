package memlru

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goware/cachestore"
	lru "github.com/hashicorp/golang-lru"
)

var _ cachestore.Store[any] = &MemLRU[any]{}

// determines the minimum time between every TTL-based removal
var lastExpiryCheckInterval = time.Second * 5

const defaultLRUSize = 512

type MemLRU[V any] struct {
	backend         *lru.Cache
	expirationQueue *expirationQueue
	lastExpiryCheck time.Time
	mu              sync.RWMutex
}

func New[V any]() (cachestore.Store[V], error) {
	return NewWithSize[V](defaultLRUSize)
}

func NewWithSize[V any](size int) (cachestore.Store[V], error) {
	if size < 1 {
		return nil, errors.New("size must be greater or equal to 1")
	}

	backend, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	memLRU := &MemLRU[V]{
		backend:         backend,
		expirationQueue: newExpirationQueue(),
	}

	return memLRU, nil
}

func (m *MemLRU[V]) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	m.removeExpiredKeys()
	_, exists := m.backend.Peek(key)
	m.mu.Unlock()
	return exists, nil
}

func (m *MemLRU[V]) Set(ctx context.Context, key string, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.setKeyValue(ctx, key, value); err != nil {
		return err
	}
	return nil
}

func (m *MemLRU[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.setKeyValue(ctx, key, value); err != nil {
		return err
	}

	m.expirationQueue.Push(key, ttl)
	return nil
}

func (m *MemLRU[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, key := range keys {
		m.backend.Add(key, values[i])
	}
	return nil
}

func (m *MemLRU[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, key := range keys {
		m.backend.Add(key, values[i])
		m.expirationQueue.Push(key, ttl) // TODO: probably a more efficient way to do this batch push
	}

	return nil
}

func (m *MemLRU[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	m.mu.Lock()
	m.removeExpiredKeys()
	v, ok := m.backend.Get(key)
	m.mu.Unlock()

	if !ok {
		// key not found, respond with no data
		return out, false, nil
	}
	b, ok := v.(V)
	if !ok {
		return out, false, fmt.Errorf("memlru#Get: value of key %s is not of type ", key)
	}

	return b, true, nil
}

func (m *MemLRU[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	vals := make([]V, 0, len(keys))
	oks := make([]bool, 0, len(keys))
	var out V
	m.mu.Lock()
	m.removeExpiredKeys()

	for _, key := range keys {
		v, ok := m.backend.Get(key)
		if !ok {
			// key not found, add empty/default value
			vals = append(vals, out)
			oks = append(oks, false)
			continue
		}

		b, ok := v.(V)
		if !ok {
			m.mu.Unlock()
			return nil, nil, fmt.Errorf("memlru#Get: value of key %s is not a []byte", key)
		}
		vals = append(vals, b)
		oks = append(oks, true)
	}
	m.mu.Unlock()

	return vals, oks, nil
}

func (m *MemLRU[V]) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	present := m.backend.Remove(key)
	m.mu.Unlock()

	// NOTE/TODO: we do not check for presence, prob okay
	_ = present
	return nil
}

func (m *MemLRU[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range m.backend.Keys() {
		if _, ok := key.(string); !ok {
			continue
		}
		if strings.HasPrefix(key.(string), keyPrefix) {
			m.backend.Remove(key)
		}
	}

	return nil
}

func (m *MemLRU[V]) ClearAll(ctx context.Context) error {
	m.backend.Purge()
	return nil
}

func (m *MemLRU[V]) setKeyValue(ctx context.Context, key string, value V) error {
	if len(key) > cachestore.MaxKeyLength {
		return cachestore.ErrKeyLengthTooLong
	}
	if len(key) == 0 {
		return cachestore.ErrInvalidKey
	}
	m.backend.Add(key, value)
	return nil
}

func (m *MemLRU[V]) removeExpiredKeys() {
	now := time.Now()
	if m.lastExpiryCheck.Add(lastExpiryCheckInterval).After(now) {
		// another removal happened recently
		return
	}
	expiredKeys := m.expirationQueue.Expired()
	for _, key := range expiredKeys {
		m.backend.Remove(key)
	}
	m.lastExpiryCheck = now
}
