package noopcache

import (
	"context"
	"time"

	"github.com/goware/cachestore"
)

var _ cachestore.Store[any] = &NoopCache[any]{}

type NoopCache[V any] struct{}

type Config struct {
	cachestore.StoreOptions
}

func (c *Config) Apply(options *cachestore.StoreOptions) {
	c.StoreOptions.Apply(options)
}

// func Backend() cachestore.Backend {
// 	return &Config{
// 		StoreOptions: cachestore.StoreOptions{},
// 	}
// }

func New[V any]() (cachestore.Store[V], error) {
	return &NoopCache[V]{}, nil
}

func (s *NoopCache[V]) Name() string {
	return "noopcache"
}

func (s *NoopCache[V]) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (s *NoopCache[V]) Set(ctx context.Context, key string, value V) error {
	return nil
}

func (s *NoopCache[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	return nil
}

func (s *NoopCache[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return nil
}

func (s *NoopCache[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	return nil
}

func (s *NoopCache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	return out, false, nil
}

func (s *NoopCache[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return nil, nil, nil
}

func (s *NoopCache[V]) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *NoopCache[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	return nil
}

func (s *NoopCache[V]) ClearAll(ctx context.Context) error {
	return nil
}

func (s *NoopCache[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return getter(ctx, key)
}

func (s *NoopCache[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	return getter(ctx, key)
}
