package memlru

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/singleflight"
	lru "github.com/hashicorp/golang-lru/v2"
)

var _ cachestore.Store[any] = &MemLRU[any]{}

// determines the minimum time between every TTL-based removal
var lastExpiryCheckInterval = time.Second * 5

const defaultLRUSize = 512

type MemLRU[V any] struct {
	options         cachestore.StoreOptions
	backend         *lru.Cache[string, V]
	expirationQueue *expirationQueue

	singleflight singleflight.Group[string, V]
}

func Backend(size int, opts ...cachestore.StoreOptions) cachestore.Backend {
	return &Config{
		StoreOptions: cachestore.ApplyOptions(opts...),
		Size:         size,
	}
}

func NewWithBackend[V any](backend cachestore.Backend, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	cfg, ok := backend.(*Config)
	if !ok {
		return nil, fmt.Errorf("memlru: invalid backend config supplied")
	}
	for _, opt := range opts {
		opt.Apply(&cfg.StoreOptions)
	}
	return NewWithSize[V](cfg.Size, cfg.StoreOptions)
}

func New[V any](opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	return NewWithSize[V](defaultLRUSize, opts...)
}

func NewWithSize[V any](size int, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	if size < 1 {
		return nil, errors.New("size must be greater or equal to 1")
	}

	backend, err := lru.New[string, V](size)
	if err != nil {
		return nil, err
	}

	options := cachestore.ApplyOptions(opts...)
	if options.LockRetryTimeout == 0 {
		options.LockRetryTimeout = 8 * time.Second
	}

	memLRU := &MemLRU[V]{
		options:         options,
		backend:         backend,
		expirationQueue: newExpirationQueue(),
	}

	return memLRU, nil
}

func (m *MemLRU[V]) Exists(ctx context.Context, key string) (bool, error) {
	m.removeExpiredKeys()
	_, exists := m.backend.Peek(key)
	return exists, nil
}

func (m *MemLRU[V]) Set(ctx context.Context, key string, value V) error {
	return m.SetEx(ctx, key, value, m.options.DefaultKeyExpiry)
}

func (m *MemLRU[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if err := m.setKeyValue(ctx, key, value); err != nil {
		return err
	}
	if ttl > 0 {
		m.expirationQueue.Push(key, ttl)
	}

	return nil
}

func (m *MemLRU[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return m.BatchSetEx(ctx, keys, values, m.options.DefaultKeyExpiry)
}

func (m *MemLRU[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	for i, key := range keys {
		m.backend.Add(key, values[i])
		if ttl > 0 {
			m.expirationQueue.Push(key, ttl) // TODO: probably a more efficient way to do this batch push
		}
	}

	return nil
}

func (m *MemLRU[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	m.removeExpiredKeys()
	v, ok := m.backend.Get(key)

	if !ok {
		// key not found, respond with no data
		return out, false, nil
	}

	return v, true, nil
}

func (m *MemLRU[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	vals := make([]V, 0, len(keys))
	oks := make([]bool, 0, len(keys))
	var out V
	m.removeExpiredKeys()

	for _, key := range keys {
		v, ok := m.backend.Get(key)
		if !ok {
			// key not found, add empty/default value
			vals = append(vals, out)
			oks = append(oks, false)
			continue
		}

		vals = append(vals, v)
		oks = append(oks, true)
	}

	return vals, oks, nil
}

func (m *MemLRU[V]) Delete(ctx context.Context, key string) error {
	present := m.backend.Remove(key)

	// NOTE/TODO: we do not check for presence, prob okay
	_ = present
	return nil
}

func (m *MemLRU[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	for _, key := range m.backend.Keys() {
		if strings.HasPrefix(key, keyPrefix) {
			m.backend.Remove(key)
		}
	}

	return nil
}

func (m *MemLRU[V]) ClearAll(ctx context.Context) error {
	m.backend.Purge()
	return nil
}

func (m *MemLRU[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return m.GetOrSetWithLockEx(ctx, key, getter, m.options.DefaultKeyExpiry)
}

func (m *MemLRU[V]) GetOrSetWithLockEx(
	ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration,
) (V, error) {
	var out V

	m.removeExpiredKeys()

	ctx, cancel := context.WithTimeout(ctx, m.options.LockRetryTimeout)
	defer cancel()

	v, ok := m.backend.Get(key)
	if ok {
		return v, nil
	}

	v, err, _ := m.singleflight.Do(key, func() (V, error) {
		v, err := getter(ctx, key)
		if err != nil {
			return out, fmt.Errorf("getter error: %w", err)
		}

		if err := m.setKeyValue(ctx, key, v); err != nil {
			return out, err
		}
		if ttl > 0 {
			m.expirationQueue.Push(key, ttl)
		}
		return v, nil
	})
	if err != nil {
		return out, fmt.Errorf("cachestore/memlru: singleflight error: %w", err)
	}

	return v, nil
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
	if !m.expirationQueue.ShouldExpire() {
		// another removal happened recently
		return
	}
	expiredKeys := m.expirationQueue.Expired()
	for _, key := range expiredKeys {
		m.backend.Remove(key)
	}
	m.expirationQueue.UpdateLastCheckTime()
}
