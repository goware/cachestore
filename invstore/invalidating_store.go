package invstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goware/cachestore"
	"github.com/goware/pubsub"
)

const (
	DefaultChannelID = "store_invalidation"
)

type InstanceID string

func newInstanceID() InstanceID {
	return InstanceID(uuid.NewString())
}

type StoreInvalidationMessage struct {
	Keys        []string   `json:"keys"`
	Origin      InstanceID `json:"origin"`
	ContentHash string     `json:"content_hash"`
}

type InvalidatingStore[V any] struct {
	store      cachestore.Store[V]
	pubsub     pubsub.PubSub[StoreInvalidationMessage]
	instanceID InstanceID
}

func NewInvalidatingStore[V any](store cachestore.Store[V], ps pubsub.PubSub[StoreInvalidationMessage]) *InvalidatingStore[V] {
	return &InvalidatingStore[V]{
		store:      store,
		pubsub:     ps,
		instanceID: newInstanceID(),
	}
}

func (ic *InvalidatingStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	return ic.store.Exists(ctx, key)
}

func (ic *InvalidatingStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	return ic.store.Get(ctx, key)
}

func (ic *InvalidatingStore[V]) GetEx(ctx context.Context, key string) (V, *time.Duration, bool, error) {
	return ic.store.GetEx(ctx, key)
}

func (ic *InvalidatingStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return ic.store.BatchGet(ctx, keys)
}

func (ic *InvalidatingStore[V]) Set(ctx context.Context, key string, value V) error {
	if err := ic.store.Set(ctx, key, value); err != nil {
		return err
	}
	hash, err := ComputeHash(value)
	if err != nil {
		hash = ""
	}
	return ic.publishInvalidation(ctx, []string{key}, hash)
}

func (ic *InvalidatingStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if err := ic.store.SetEx(ctx, key, value, ttl); err != nil {
		return err
	}
	hash, err := ComputeHash(value)
	if err != nil {
		hash = ""
	}
	return ic.publishInvalidation(ctx, []string{key}, hash)
}

func (ic *InvalidatingStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if err := ic.store.BatchSet(ctx, keys, values); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, keys, "")
}

func (ic *InvalidatingStore[V]) BatchSetEx(
	ctx context.Context,
	keys []string,
	values []V,
	ttl time.Duration,
) error {
	if err := ic.store.BatchSetEx(ctx, keys, values, ttl); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, keys, "")
}

func (ic *InvalidatingStore[V]) Delete(ctx context.Context, key string) error {
	if err := ic.delete(ctx, key); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{key}, "")
}

func (ic *InvalidatingStore[V]) delete(ctx context.Context, key string) error {
	if err := ic.store.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.deletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{fmt.Sprintf("%s*", keyPrefix)}, "")
}

func (ic *InvalidatingStore[V]) deletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.store.DeletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingStore[V]) ClearAll(ctx context.Context) error {
	if err := ic.clearAll(ctx); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []string{"*"}, "")
}

func (ic *InvalidatingStore[V]) clearAll(ctx context.Context) error {
	if err := ic.store.ClearAll(ctx); err != nil {
		return err
	}
	return nil
}

func (ic *InvalidatingStore[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLock(ctx, key, wrappedGetter)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		hash, err := ComputeHash(value)
		if err != nil {
			hash = ""
		}
		if err := ic.publishInvalidation(ctx, []string{key}, hash); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ic *InvalidatingStore[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLockEx(ctx, key, wrappedGetter, ttl)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		hash, err := ComputeHash(value)
		if err != nil {
			hash = ""
		}
		if err := ic.publishInvalidation(ctx, []string{key}, hash); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ic InvalidatingStore[V]) GetInstanceID() InstanceID {
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
func (ic *InvalidatingStore[V]) publishInvalidation(ctx context.Context, keys []string, hash string) error {
	msg := StoreInvalidationMessage{
		Keys:        keys,
		Origin:      ic.instanceID, // we need this to skip invalidating the store for the same key from the same instance
		ContentHash: hash,
	}
	return ic.pubsub.Publish(ctx, DefaultChannelID, msg)
}

func ComputeHash[V any](v V) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data: %w", err)
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:]), nil
}
