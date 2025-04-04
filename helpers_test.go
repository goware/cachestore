package cachestore_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/goware/cachestore/v2"
)

type MockStore[V any] struct {
	store map[string]V
}

var _ cachestore.Store[any] = &MockStore[any]{}

func NewMockStore[V any]() *MockStore[V] {
	return &MockStore[V]{store: make(map[string]V)}
}

func (s *MockStore[V]) Name() string {
	return "mockcache"
}

func (s *MockStore[V]) Set(ctx context.Context, key string, value V) error {
	s.store[key] = value
	return nil
}

func (s *MockStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	return s.store[key], true, nil
}

func (s *MockStore[V]) Delete(ctx context.Context, key string) error {
	delete(s.store, key)
	return nil
}

func (s *MockStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	for k := range s.store {
		if strings.HasPrefix(k, keyPrefix) {
			delete(s.store, k)
		}
	}
	return nil
}

func (s *MockStore[V]) ClearAll(ctx context.Context) error {
	clear(s.store)
	return nil
}

func (s *MockStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := s.store[key]
	return ok, nil
}

func (s *MockStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	// NOTE: ttl is ignored
	s.store[key] = value
	return nil
}

func (s *MockStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	for i, key := range keys {
		s.store[key] = values[i]
	}
	return nil
}

func (s *MockStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	// NOTE: ttl is ignored
	return s.BatchSet(ctx, keys, values)
}

func (s *MockStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	values := make([]V, len(keys))
	exists := make([]bool, len(keys))
	for i, key := range keys {
		value, ok := s.store[key]
		values[i] = value
		exists[i] = ok
	}
	return values, exists, nil
}

func (s *MockStore[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	value, ok := s.store[key]
	if ok {
		return value, nil
	}
	return getter(ctx, key)
}

func (s *MockStore[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	// NOTE: ttl is ignored
	return s.GetOrSetWithLock(ctx, key, getter)
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func assertTrue(t *testing.T, value bool) {
	t.Helper()
	if !value {
		t.Fatalf("expected true, got false")
	}
}

func assertFalse(t *testing.T, value bool) {
	t.Helper()
	if value {
		t.Fatalf("expected false, got true")
	}
}

func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
