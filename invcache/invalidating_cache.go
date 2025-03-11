package invcache

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goware/cachestore"
	"github.com/goware/pubsub"
)

const (
	DefaultChannelID = "cache_invalidation"
)

type InstanceID string

func newInstanceID() InstanceID {
	return InstanceID(uuid.NewString())
}

type CacheInvalidationMessage struct {
	Keys   []string   `json:"keys"`
	Origin InstanceID `json:"origin"`
}

type InvalidatingCache[V any] struct {
	store      cachestore.Store[V]
	pubsub     pubsub.PubSub[CacheInvalidationMessage]
	instanceID InstanceID
}

func NewInvalidatingCache[V any](store cachestore.Store[V], ps pubsub.PubSub[CacheInvalidationMessage]) *InvalidatingCache[V] {
	return &InvalidatingCache[V]{
		store:      store,
		pubsub:     ps,
		instanceID: newInstanceID(),
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
	return ic.publishInvalidation(ctx, []string{key})
}

func (ic *InvalidatingCache[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if err := ic.store.SetEx(ctx, key, value, ttl); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{key})
}

func (ic *InvalidatingCache[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if err := ic.store.BatchSet(ctx, keys, values); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, keys)
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
	return ic.publishInvalidation(ctx, keys)
}

func (ic *InvalidatingCache[V]) Delete(ctx context.Context, key string) error {
	if err := ic.delete(ctx, key); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{key})
}

func (ic *InvalidatingCache[V]) delete(ctx context.Context, key string) error {
	if err := ic.store.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingCache[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.deletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{fmt.Sprintf("%s*", keyPrefix)})
}

func (ic *InvalidatingCache[V]) deletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.store.DeletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingCache[V]) ClearAll(ctx context.Context) error {
	if err := ic.clearAll(ctx); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{"*"})
}

func (ic *InvalidatingCache[V]) clearAll(ctx context.Context) error {
	if err := ic.store.ClearAll(ctx); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingCache[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLock(ctx, key, wrappedGetter)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		if err := ic.publishInvalidation(ctx, []string{key}); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ic *InvalidatingCache[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLockEx(ctx, key, wrappedGetter, ttl)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		if err := ic.publishInvalidation(ctx, []string{key}); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ic InvalidatingCache[V]) GetInstanceID() InstanceID {
	return ic.instanceID
}

func wrapGetterWasCalled[V any](
	getter func(context.Context, string) (V, error),
) (wrapped func(context.Context, string) (V, error), wasCalled *bool) {
	var called bool

	wrapped = func(ctx context.Context, key string) (V, error) {
		called = true
		return getter(ctx, key)
	}
	return wrapped, &called
}

// Publish invalidation event
func (ic *InvalidatingCache[V]) publishInvalidation(ctx context.Context, keys []string) error {
	msg := CacheInvalidationMessage{
		Keys:   keys,
		Origin: ic.instanceID, // we need this to skip invalidating the cache for the same key from the same instance
	}
	return ic.pubsub.Publish(ctx, DefaultChannelID, msg)
}
