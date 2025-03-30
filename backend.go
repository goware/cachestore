package cachestore

import (
	"context"
	"encoding/json"
	"time"
)

type Backend interface {
	// Store[any]
	Name() string
	// TODO: .. prob can do a few more things.. like config apply maybe..?
}

func OpenStore[T any](backend Backend) Store[T] {
	store, ok := backend.(Store[any])
	if !ok {
		panic("zzzz") // TODO ..
	}
	return newBackendStoreAdapter[T](store)
}

// type Backend interface {
// 	// TOOD: lets try this in play3.go ..

// 	// maybe Register() ..?
// 	Apply(*StoreOptions) // TODO: rename.. ? ApplyStoreOptions() ..? ..?
// }

func newBackendStoreAdapter[T any](anyStore Store[any]) Store[T] {
	adapter := &backendAdapter[T]{
		store: anyStore,
		// store:   Open[T](backend),
	}
	return adapter
}

type backendAdapter[T any] struct {
	store Store[any]
}

func (s *backendAdapter[T]) Name() string {
	return s.store.Name()
}

func (s *backendAdapter[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var v T

	bs, ok := s.store.(ByteStoreGetter)
	if ok {
		byteStore := bs.ByteStore()

		bv, ok, err := byteStore.Get(ctx, key)
		if err != nil {
			return v, false, err
		}
		_ = ok

		// byteSerializer, ok := byteStore.(ByteSerializer)
		// if !ok {
		// 	panic("nooopp...")
		// }

		// deserialized, err := byteSerializer.Deserialize(bv)
		// if err != nil {
		// 	return v, false, err
		// }

		deserialized, err := Deserialize[T](bv)
		if err != nil {
			return v, false, err
		}

		return deserialized, true, nil

	} else {
		bv, ok, err := s.store.Get(ctx, key)
		if err != nil {
			return v, ok, err
		}
		v, ok = bv.(T)
		return v, ok, nil
	}
}

func (s *backendAdapter[T]) Set(ctx context.Context, key string, value T) error {

	// fmt.Println("SUP?")

	bs, ok := s.store.(ByteStoreGetter)
	if ok {
		byteStore := bs.ByteStore()

		serialized, err := Serialize(value)
		if err != nil {
			return err
		}

		// serializer, ok := bs.(ByteSerializer)
		// if !ok {
		// 	panic("the backend does not implement ByteSerializer")
		// }

		// serialized, err := serializer.Serialize(value)
		// if err != nil {
		// 	panic(err)
		// }

		return byteStore.Set(ctx, key, serialized)
	} else {
		return s.store.Set(ctx, key, value)
	}

	// return byteStore.Set(ctx, key, value)
	return nil
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
	return s.store.Delete(ctx, key)
}

func (s *backendAdapter[T]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	return s.store.DeletePrefix(ctx, keyPrefix)
}

func (s *backendAdapter[T]) ClearAll(ctx context.Context) error {
	return s.store.ClearAll(ctx)
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
	return s.store.Exists(ctx, key)
}

func (s *backendAdapter[T]) SetEx(ctx context.Context, key string, value T, ttl time.Duration) error {
	return s.store.SetEx(ctx, key, value, ttl)
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

func Deserialize[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func Serialize[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

//--

// func serialize(value any) ([]byte, error) {
// 	// return the value directly if the type is a []byte or string,
// 	// otherwise assume its json and unmarshal it
// 	switch v := value.(type) {
// 	case string:
// 		return []byte(v), nil
// 	case []byte:
// 		return v, nil
// 	default:
// 		panic("mmm")
// 		return json.Marshal(value) // wrap the error
// 	}
// }

// func deserialize(data []byte) (any, error) {
// 	var out any

// 	spew.Dump(data)
// 	spew.Dump(out)

// 	switch any(out).(type) {
// 	case string:
// 		outv := reflect.ValueOf(&out).Elem()
// 		outv.SetString(string(data))
// 		return out, nil
// 	case []byte:
// 		outv := reflect.ValueOf(&out).Elem()
// 		outv.SetBytes(data)
// 		return out, nil
// 	default:
// 		t := reflect.TypeOf(&out)
// 		fmt.Println("t:", t)

// 		err := json.Unmarshal(data, &out)
// 		if err != nil {
// 			return out, fmt.Errorf("rediscache:failed to unmarshal data: %w", err)
// 		}
// 		return out, nil
// 	}
// }
