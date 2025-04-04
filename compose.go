package cachestore

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type composeStore[V any] struct {
	stores []Store[V]
	name   string
}

func ComposeStores[V any](stores ...Store[V]) (Store[V], error) {
	if len(stores) == 0 {
		return nil, fmt.Errorf("cachestore: attempting to compose with empty store list")
	}
	if len(stores) == 1 {
		return stores[0], nil
	}
	names := make([]string, len(stores))
	for _, s := range stores {
		names = append(names, s.Name())
	}
	cs := &composeStore[V]{
		stores: stores,
		name:   strings.Join(names, ">"),
	}
	return cs, nil
}

func ComposeBackends[V any](backends ...Backend) (Store[V], error) {
	if len(backends) == 0 {
		return nil, fmt.Errorf("cachestore: attempting to compose with empty backend list")
	}
	if len(backends) == 1 {
		return OpenStore[V](backends[0]), nil
	}
	stores := make([]Store[V], len(backends))
	for i, backend := range backends {
		stores[i] = OpenStore[V](backend)
	}
	return ComposeStores(stores...)
}

func (cs *composeStore[V]) Name() string {
	return cs.name
}

func (cs *composeStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	for _, s := range cs.stores {
		exists, err := s.Exists(ctx, key)
		if err != nil {
			return false, err
		}
		if exists {
			return exists, nil
		}
	}
	return false, nil
}

func (cs *composeStore[V]) Set(ctx context.Context, key string, value V) error {
	for _, s := range cs.stores {
		err := s.Set(ctx, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	for _, s := range cs.stores {
		err := s.SetEx(ctx, key, value, ttl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	for _, s := range cs.stores {
		err := s.BatchSet(ctx, keys, values)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	for _, s := range cs.stores {
		err := s.BatchSetEx(ctx, keys, values, ttl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	var exists bool
	var err error

	for _, s := range cs.stores {
		out, exists, err = s.Get(ctx, key)
		if err != nil {
			return out, exists, err
		}
		if exists {
			return out, exists, nil
		}
	}
	return out, exists, err
}

func (cs *composeStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	fout := make([]V, len(keys))
	fexists := make([]bool, len(keys))

	// TODO: in the future, we can actually call BatchSet() on difference stores
	// which are missing the keys, upward on the chain.. maybe downward too,
	// but we'd probably need ComposeOptions in the constructor

	fn := func(store Store[V], ctx context.Context) (bool, error) {
		idx := make([]int, 0, len(keys))
		k := make([]string, 0, len(keys))

		for i, e := range fexists {
			if e == false {
				k = append(k, keys[i])
				idx = append(idx, i)
			}
		}

		if len(idx) == 0 {
			// nothing to do, we done..
			return true, nil
		}

		out, exists, err := store.BatchGet(ctx, k)
		if err != nil {
			return false, err
		}

		// todo, check all exists, etc..
		for i, e := range exists {
			if e {
				fout[idx[i]] = out[i]
				fexists[idx[i]] = true
			}
		}

		done := true
		for _, e := range fexists {
			if !e {
				done = false
				break
			}
		}

		return done, nil
	}

	for _, s := range cs.stores {
		done, err := fn(s, ctx)
		if err != nil {
			return fout, fexists, err
		}
		if done {
			break
		}
	}

	return fout, fexists, nil
}

func (cs *composeStore[V]) Delete(ctx context.Context, key string) error {
	for _, s := range cs.stores {
		err := s.Delete(ctx, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if len(keyPrefix) < 4 {
		return fmt.Errorf("cachestore: DeletePrefix keyPrefix '%s' must be at least 4 characters long", keyPrefix)
	}

	for _, s := range cs.stores {
		err := s.DeletePrefix(ctx, keyPrefix)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) ClearAll(ctx context.Context) error {
	for _, s := range cs.stores {
		err := s.ClearAll(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *composeStore[V]) GetOrSetWithLock(
	ctx context.Context, key string, getter func(context.Context, string) (V, error),
) (V, error) {
	// Skip all intermediate stores and use only the last one as usually it's the most reliable one
	return cs.stores[len(cs.stores)-1].GetOrSetWithLock(ctx, key, getter)
}

func (cs *composeStore[V]) GetOrSetWithLockEx(
	ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration,
) (V, error) {
	// Skip all intermediate stores and use only the last one as usually it's the most reliable one
	return cs.stores[len(cs.stores)-1].GetOrSetWithLockEx(ctx, key, getter, ttl)
}
