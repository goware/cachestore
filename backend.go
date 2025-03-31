package cachestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Backend interface {
	// Store[any]
	Name() string
	// TODO: .. prob can do a few more things.. like config apply maybe..?
	// ...
}

func OpenStore[T any](backend Backend) Store[T] {
	store, ok := backend.(Store[any])
	if !ok {
		return &backendAdapter[T]{anyStore: nil}
	} else {
		return newBackendAdapter[T](store)
	}
}

// type Backend interface {
// 	// TOOD: lets try this in play3.go ..

// 	// maybe Register() ..?
// 	Apply(*StoreOptions) // TODO: rename.. ? ApplyStoreOptions() ..? ..?
// }

var ErrBackendAdapterNil = fmt.Errorf("cachestore: backend adapter is nil")
var ErrBackendTypeCast = fmt.Errorf("cachestore: backend type cast failure")

func newBackendAdapter[T any](anyStore Store[any]) Store[T] {
	adapter := &backendAdapter[T]{
		anyStore: anyStore,
	}
	return adapter
}

type backendAdapter[T any] struct {
	anyStore Store[any]
}

func (s *backendAdapter[T]) Name() string {
	if s.anyStore == nil {
		return ""
	}
	return s.anyStore.Name()
}

func (s *backendAdapter[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var v T

	if s.anyStore == nil {
		return v, false, ErrBackendAdapterNil
	}

	bs, ok := s.anyStore.(ByteStoreGetter)
	if ok {
		// If the underlining store implements ByteStoreGetter,
		// then we assume the Get will return []byte types, and we will
		// handle serialization here. This is used by cachestore-redis
		// and all other external stores.
		byteStore := bs.ByteStore()

		bv, ok, err := byteStore.Get(ctx, key)
		if err != nil {
			return v, false, err
		}
		if !ok {
			return v, false, nil
		}

		deserialized, err := Deserialize[T](bv)
		if err != nil {
			return v, false, err
		}
		return deserialized, true, nil
	} else {
		// Otherwise, we just use the underlying store's Get method,
		// and type cast to the generic type. This is used by cachestore-mem.
		bv, ok, err := s.anyStore.Get(ctx, key)
		if err != nil {
			return v, ok, err
		}
		if !ok {
			return v, false, nil
		}
		v, ok = bv.(T)
		if !ok {
			return v, false, fmt.Errorf("cachestore: failed to cast value to type %T: %w", v, ErrBackendTypeCast)
		}
		return v, ok, nil
	}
}

func (s *backendAdapter[T]) Set(ctx context.Context, key string, value T) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}

	// See comments in Get()
	bs, ok := s.anyStore.(ByteStoreGetter)
	if ok {
		byteStore := bs.ByteStore()

		serialized, err := Serialize(value)
		if err != nil {
			return err
		}

		return byteStore.Set(ctx, key, serialized)
	} else {
		return s.anyStore.Set(ctx, key, value)
	}
}

func (s *backendAdapter[T]) BatchGet(ctx context.Context, keys []string) ([]T, []bool, error) {
	// return s.backend.BatchGet(ctx, keys)
	return nil, nil, nil
}

func (s *backendAdapter[T]) BatchSet(ctx context.Context, keys []string, values []T) error {
	// return s.backend.BatchSet(ctx, keys, values)
	return nil
}

func (s *backendAdapter[T]) BatchSetEx(ctx context.Context, keys []string, values []T, ttl time.Duration) error {
	// return s.backend.BatchSetEx(ctx, keys, values, ttl)
	return nil
}

func (s *backendAdapter[T]) Delete(ctx context.Context, key string) error {
	return s.anyStore.Delete(ctx, key)
}

func (s *backendAdapter[T]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	return s.anyStore.DeletePrefix(ctx, keyPrefix)
}

func (s *backendAdapter[T]) ClearAll(ctx context.Context) error {
	return s.anyStore.ClearAll(ctx)
}

func (s *backendAdapter[T]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (T, error)) (T, error) {
	// return s.backend.GetOrSetWithLock(ctx, key, getter)
	var v T
	return v, nil
}

func (s *backendAdapter[T]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (T, error), ttl time.Duration) (T, error) {
	// return s.backend.GetOrSetWithLockEx(ctx, key, getter, ttl)
	var v T
	return v, nil
}

func (s *backendAdapter[T]) Exists(ctx context.Context, key string) (bool, error) {
	return s.anyStore.Exists(ctx, key)
}

func (s *backendAdapter[T]) SetEx(ctx context.Context, key string, value T, ttl time.Duration) error {
	return s.anyStore.SetEx(ctx, key, value, ttl)
}

func (s *backendAdapter[T]) GetEx(ctx context.Context, key string) (T, *time.Duration, bool, error) {
	// return s.backend.GetEx(ctx, key)
	var v T
	var ttl *time.Duration
	var ok bool
	return v, ttl, ok, nil
}

func (s *backendAdapter[T]) CleanExpiredEvery(ctx context.Context, d time.Duration, onError func(err error)) {
	// s.backend.CleanExpiredEvery(ctx, d, onError)
	// hmm..
}

func Serialize[V any](value V) ([]byte, error) {
	switch v := any(value).(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("cachestore: failed to marshal data: %w", err)
		}
		return out, nil
	}
}

func Deserialize[V any](data []byte) (V, error) {
	var out V
	switch any(out).(type) {
	case string:
		str := string(data)
		out = any(str).(V)
		return out, nil
	case []byte:
		out = any(data).(V)
		return out, nil
	default:
		err := json.Unmarshal(data, &out)
		if err != nil {
			return out, fmt.Errorf("cachestore: failed to unmarshal data: %w", err)
		}
		return out, nil
	}
}
