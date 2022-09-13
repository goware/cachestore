package cachestore

import (
	"context"
	"fmt"
	"time"
)

type ComposeStore[V any] struct {
	stores []Store[V]
}

func Compose[V any](stores ...Store[V]) (Store[V], error) {
	if len(stores) == 0 {
		return nil, fmt.Errorf("cachestore: attempting to compose with empty store list")
	}
	cs := &ComposeStore[V]{
		stores: stores,
	}
	return cs, nil
}

func (cs *ComposeStore[V]) Exists(ctx context.Context, key string) (bool, error) {
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

func (cs *ComposeStore[V]) Set(ctx context.Context, key string, value V) error {
	for _, s := range cs.stores {
		err := s.Set(ctx, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	for _, s := range cs.stores {
		err := s.SetEx(ctx, key, value, ttl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	for _, s := range cs.stores {
		err := s.BatchSet(ctx, keys, values)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	for _, s := range cs.stores {
		err := s.BatchSetEx(ctx, keys, values, ttl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
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

func (cs *ComposeStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
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

func (cs *ComposeStore[V]) Delete(ctx context.Context, key string) error {
	for _, s := range cs.stores {
		err := s.Delete(ctx, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	for _, s := range cs.stores {
		err := s.DeletePrefix(ctx, keyPrefix)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *ComposeStore[V]) ClearAll(ctx context.Context) error {
	for _, s := range cs.stores {
		err := s.ClearAll(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
