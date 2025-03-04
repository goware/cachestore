package invcache

import (
	"context"
	"errors"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/pubsub"
)

const (
	ChannelID = "cache_invalidation"
)

type CacheInvalidationMessage struct {
	Key    string `json:"key"`
	Origin string `json:"origin"`
}

type InvalidatingCache[V any] struct {
	store      cachestore.Store[V]
	pubsub     pubsub.PubSub[CacheInvalidationMessage]
	instanceID string
}

func NewInvalidatingCache[V any](store cachestore.Store[V], ps pubsub.PubSub[CacheInvalidationMessage], instanceID string) *InvalidatingCache[V] {
	return &InvalidatingCache[V]{
		store:      store,
		pubsub:     ps,
		instanceID: instanceID,
	}
}

func (ic *InvalidatingCache[V]) Exists(ctx context.Context, key string) (bool, error) {
	return ic.store.Exists(ctx, key)
}

func (ic *InvalidatingCache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	return ic.store.Get(ctx, key)
}

func (ic *InvalidatingCache[V]) GetEx(ctx context.Context, key string) (V, *time.Duration, bool, error) {
	return ic.store.GetEx(ctx, key)
}

func (ic *InvalidatingCache[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return ic.store.BatchGet(ctx, keys)
}

func (ic *InvalidatingCache[V]) Set(ctx context.Context, key string, value V) error {
	if err := ic.store.Set(ctx, key, value); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, key)
}

func (ic *InvalidatingCache[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if err := ic.store.SetEx(ctx, key, value, ttl); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, key)
}

func (ic *InvalidatingCache[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if err := ic.store.BatchSet(ctx, keys, values); err != nil {
		return err
	}

	var joinedErr error
	for _, k := range keys {
		if err := ic.publishInvalidation(ctx, k); err != nil {
			joinedErr = errors.Join(joinedErr, err)
		}
	}

	return joinedErr
}

func (ic *InvalidatingCache[V]) BatchSetEx(
	ctx context.Context,
	keys []string,
	values []V,
	ttl time.Duration,
) error {
	if err := ic.store.BatchSetEx(ctx, keys, values, ttl); err != nil {
		return err
	}

	var joinedErr error
	for _, k := range keys {
		if err := ic.publishInvalidation(ctx, k); err != nil {
			joinedErr = errors.Join(joinedErr, err)
		}
	}

	return joinedErr
}

func (ic *InvalidatingCache[V]) Delete(ctx context.Context, key string) error {
	ic.delete(ctx, key)

	return ic.publishInvalidation(ctx, key)
}

func (ic *InvalidatingCache[V]) delete(ctx context.Context, key string) error {
	if err := ic.store.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingCache[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.store.DeletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, keyPrefix)
}

func (ic *InvalidatingCache[V]) ClearAll(ctx context.Context) error {
	if err := ic.store.ClearAll(ctx); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, "*")
}

func (ic *InvalidatingCache[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	value, err := ic.store.GetOrSetWithLock(ctx, key, getter)
	if err == nil {
		ic.publishInvalidation(ctx, key)
	}
	return value, err
}

func (ic *InvalidatingCache[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	value, err := ic.store.GetOrSetWithLockEx(ctx, key, getter, ttl)
	if err == nil {
		return value, ic.publishInvalidation(ctx, key)
	}
	return value, err
}

// Publish invalidation event
func (ic *InvalidatingCache[V]) publishInvalidation(ctx context.Context, key string) error {
	msg := CacheInvalidationMessage{
		Key:    key,
		Origin: ic.instanceID, // we need this to skip invalidating the cache for the same key from the same instance
	}
	return ic.pubsub.Publish(ctx, ChannelID, msg)
}
