package nostore

import (
	"context"
	"time"

	"github.com/goware/cachestore"
)

var _ cachestore.Store[any] = &NoStore[any]{}

type NoStore[V any] struct{}

type Config struct {
	cachestore.StoreOptions
}

func (c *Config) Apply(options *cachestore.StoreOptions) {
	c.StoreOptions.Apply(options)
}

func Backend() cachestore.Backend {
	return &Config{
		StoreOptions: cachestore.StoreOptions{},
	}
}

func New[V any]() (cachestore.Store[V], error) {
	return &NoStore[V]{}, nil
}

func (s *NoStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (s *NoStore[V]) Set(ctx context.Context, key string, value V) error {
	return nil
}

func (s *NoStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	return nil
}

func (c *NoStore[V]) GetEx(ctx context.Context, key string) (V, *time.Duration, bool, error) {
	var out V
	ttl := time.Duration(0)
	return out, &ttl, false, nil
}

func (s *NoStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return nil
}

func (s *NoStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	return nil
}

func (s *NoStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	return out, false, nil
}

func (s *NoStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return nil, nil, nil
}

func (s *NoStore[V]) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *NoStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	return nil
}

func (s *NoStore[V]) ClearAll(ctx context.Context) error {
	return nil
}

func (s *NoStore[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return getter(ctx, key)
}

func (s *NoStore[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	return getter(ctx, key)
}
