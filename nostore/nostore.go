package nostore

import (
	"context"
	"time"

	"github.com/goware/cachestore"
)

var _ cachestore.Store[any] = &NoStore[any]{}

type NoStore[V any] struct{}

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
