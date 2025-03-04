package invcache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/invcache"
	"github.com/goware/cachestore/memlru"
	"github.com/goware/pubsub"
	"github.com/stretchr/testify/require"
)

const (
	N = 20
)

func TestInvalidatingCache_Set_Success(t *testing.T) {
	ctx := context.Background()
	store, err := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	publishCalled := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			publishCalled = true
			require.Equal(t, invcache.ChannelID, channelID)
			require.Equal(t, "test-key", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}

	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err = ic.Set(ctx, "test-key", "test-value")
	require.NoError(t, err)

	val, ok, err := store.Get(ctx, "test-key")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "test-value", val)
	require.True(t, publishCalled)
}

func TestInvalidatingCache_Set_PublishError(t *testing.T) {
	ctx := context.Background()

	store, err := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	publishErr := errors.New("publish failed: Set")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			return publishErr
		},
	}

	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err = ic.Set(ctx, "myKey", "myVal")
	require.ErrorIs(t, err, publishErr)

	val, ok, err := store.Get(ctx, "myKey")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "myVal", val)
}

func TestInvalidatingCache_SetEx_Success(t *testing.T) {
	ctx := context.Background()
	store, err := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "exKey", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}

	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err = ic.SetEx(ctx, "exKey", "exVal", 5*time.Second)
	require.NoError(t, err)

	val, ok, err := store.Get(ctx, "exKey")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "exVal", val)
	require.True(t, published)
}

func TestInvalidatingCache_SetEx_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	publishErr := errors.New("publish failed: SetEx")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			return publishErr
		},
	}
	ic := invcache.NewInvalidatingCache[string](store, mockPS, "test-instance")

	err := ic.SetEx(ctx, "exKey", "exVal", 5*time.Second)
	require.ErrorIs(t, err, publishErr)

	v, ok, getErr := store.Get(ctx, "exKey")
	require.NoError(t, getErr)
	require.True(t, ok)
	require.Equal(t, "exVal", v)
}

func TestInvalidatingCache_BatchSet_Success(t *testing.T) {
	ctx := context.Background()
	store, err := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	var publishedKeys []string
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			publishedKeys = append(publishedKeys, msg.Key)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	keys := []string{"k1", "k2"}
	vals := []string{"v1", "v2"}
	err = ic.BatchSet(ctx, keys, vals)
	require.NoError(t, err)

	for i, k := range keys {
		got, ok, err := store.Get(ctx, k)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, vals[i], got)
	}

	require.ElementsMatch(t, []string{"k1", "k2"}, publishedKeys)
}

func TestInvalidatingCache_BatchSet_PublishError(t *testing.T) {
	ctx := context.Background()
	store, err := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			if msg.Key == "k2" {
				return errors.New("publish error for k2")
			}
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache[string](store, mockPS, "test-instance")

	keys := []string{"k1", "k2"}
	vals := []string{"v1", "v2"}
	err = ic.BatchSet(ctx, keys, vals)
	require.Error(t, err)
	require.Contains(t, err.Error(), "publish error for k2")

	for i, k := range keys {
		got, ok, errGet := store.Get(ctx, k)
		require.NoError(t, errGet)
		require.True(t, ok)
		require.Equal(t, vals[i], got)
	}
}

func TestInvalidatingCache_BatchSetEx_Success(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))

	var published []string
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = append(published, msg.Key)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	keys := []string{"bx1", "bx2"}
	vals := []string{"vx1", "vx2"}
	err := ic.BatchSetEx(ctx, keys, vals, 2*time.Second)
	require.NoError(t, err)

	for i, k := range keys {
		got, ok, errGet := store.Get(ctx, k)
		require.NoError(t, errGet)
		require.True(t, ok)
		require.Equal(t, vals[i], got)
	}
	require.ElementsMatch(t, []string{"bx1", "bx2"}, published)
}

func TestInvalidatingCache_BatchSetEx_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))

	publishErr := errors.New("publish fail: batchSetEx")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			return publishErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	keys := []string{"bx1", "bx2"}
	vals := []string{"vx1", "vx2"}
	err := ic.BatchSetEx(ctx, keys, vals, 2*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "publish fail: batchSetEx")

	for i, k := range keys {
		v, ok, _ := store.Get(ctx, k)
		require.True(t, ok)
		require.Equal(t, vals[i], v)
	}
}

func TestInvalidatingCache_Delete_Success(t *testing.T) {
	ctx := context.Background()
	store, err := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)

	_ = store.Set(ctx, "delKey", "delVal")

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "delKey", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err = ic.Delete(ctx, "delKey")
	require.NoError(t, err)
	require.True(t, published)

	_, ok, _ := store.Get(ctx, "delKey")
	require.False(t, ok)
}

func TestInvalidatingCache_Delete_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	_ = store.Set(ctx, "delKey", "delVal")

	pubErr := errors.New("delete publish error")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			return pubErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err := ic.Delete(ctx, "delKey")
	require.ErrorIs(t, err, pubErr)

	_, ok, _ := store.Get(ctx, "delKey")
	require.False(t, ok)
}

func TestInvalidatingCache_DeletePrefix_Success(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	_ = store.Set(ctx, "abc1", "val1")
	_ = store.Set(ctx, "abc2", "val2")
	_ = store.Set(ctx, "xyz", "val3")

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "abc", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err := ic.DeletePrefix(ctx, "abc")
	require.NoError(t, err)
	require.True(t, published)

	_, ok1, err := store.Get(ctx, "abc1")
	require.NoError(t, err)
	_, ok2, err := store.Get(ctx, "abc2")
	require.NoError(t, err)
	_, ok3, err := store.Get(ctx, "xyz")
	require.NoError(t, err)
	require.False(t, ok1)
	require.False(t, ok2)
	require.True(t, ok3)
}

func TestInvalidatingCache_DeletePrefix_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	_ = store.Set(ctx, "abc1", "val1")

	pubErr := errors.New("deletePrefix publish fail")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, ch string, msg invcache.CacheInvalidationMessage) error {
			return pubErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err := ic.DeletePrefix(ctx, "abc")
	require.ErrorIs(t, err, pubErr)

	_, ok1, _ := store.Get(ctx, "abc1")
	require.False(t, ok1)
}

func TestInvalidatingCache_ClearAll_Success(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	_ = store.Set(ctx, "key1", "val1")
	_ = store.Set(ctx, "key2", "val2")

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "*", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache[string](store, mockPS, "test-instance")

	err := ic.ClearAll(ctx)
	require.NoError(t, err)
	require.True(t, published)

	exists1, _ := store.Exists(ctx, "key1")
	exists2, _ := store.Exists(ctx, "key2")
	require.False(t, exists1)
	require.False(t, exists2)
}

func TestInvalidatingCache_ClearAll_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	_ = store.Set(ctx, "c1", "v1")

	pubErr := errors.New("clearAll publish fail")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, ch string, msg invcache.CacheInvalidationMessage) error {
			return pubErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	err := ic.ClearAll(ctx)
	require.ErrorIs(t, err, pubErr)

	exists, _ := store.Exists(ctx, "c1")
	require.False(t, exists)
}

func TestInvalidatingCache_GetOrSetWithLock_Success(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "lockedKey", msg.Key)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache[string](store, mockPS, "test-instance")

	val, err := ic.GetOrSetWithLock(ctx, "lockedKey", func(ctx context.Context, key string) (string, error) {
		return "valFromGetter", nil
	})
	require.NoError(t, err)
	require.Equal(t, "valFromGetter", val)
	require.True(t, published)

	got, ok, _ := store.Get(ctx, "lockedKey")
	require.True(t, ok)
	require.Equal(t, "valFromGetter", got)
}

func TestInvalidatingCache_GetOrSetWithLock_Error(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			t.Error("Publish should NOT be called if getter fails")
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	getterErr := errors.New("getter failed")
	_, err := ic.GetOrSetWithLock(ctx, "failKey", func(ctx context.Context, key string) (string, error) {
		return "", getterErr
	})
	require.ErrorIs(t, err, getterErr)
}

func TestInvalidatingCache_GetOrSetWithLock_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	pubErr := errors.New("publish fail: lock")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, ch string, msg invcache.CacheInvalidationMessage) error {
			return pubErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	val, err := ic.GetOrSetWithLock(ctx, "lockKey", func(ctx context.Context, key string) (string, error) {
		return "valFromGetter", nil
	})
	require.ErrorIs(t, err, pubErr)
	require.Equal(t, "valFromGetter", val)

	storedVal, ok, _ := store.Get(ctx, "lockKey")
	require.True(t, ok)
	require.Equal(t, "valFromGetter", storedVal)
}

func TestInvalidatingCache_GetOrSetWithLockEx_Success(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	published := false
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			published = true
			require.Equal(t, "lockExKey", msg.Key)
			require.Equal(t, "test-instance", msg.Origin)
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	val, err := ic.GetOrSetWithLockEx(ctx, "lockExKey", func(ctx context.Context, key string) (string, error) {
		return "valExFromGetter", nil
	}, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, "valExFromGetter", val)
	require.True(t, published)

	got, ok, _ := store.Get(ctx, "lockExKey")
	require.True(t, ok)
	require.Equal(t, "valExFromGetter", got)
}

func TestInvalidatingCache_GetOrSetWithLockEx_Error(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, channelID string, msg invcache.CacheInvalidationMessage) error {
			t.Error("Publish should NOT be called if getter fails")
			return nil
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	getterErr := errors.New("lockEx getter fail")
	_, err := ic.GetOrSetWithLockEx(ctx, "lockExKeyErr", func(ctx context.Context, key string) (string, error) {
		return "", getterErr
	}, 2*time.Second)
	require.ErrorIs(t, err, getterErr)
}

func TestInvalidatingCache_GetOrSetWithLockEx_PublishError(t *testing.T) {
	ctx := context.Background()
	store, _ := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))

	pubErr := errors.New("publish fail: lockEx")
	mockPS := &mockPubSub[invcache.CacheInvalidationMessage]{
		publishFunc: func(ctx context.Context, ch string, msg invcache.CacheInvalidationMessage) error {
			return pubErr
		},
	}
	ic := invcache.NewInvalidatingCache(store, mockPS, "test-instance")

	val, err := ic.GetOrSetWithLockEx(ctx, "lockExKey", func(ctx context.Context, key string) (string, error) {
		return "valExFromGetter", nil
	}, 3*time.Second)
	require.ErrorIs(t, err, pubErr)
	require.Equal(t, "valExFromGetter", val)

	got, ok, _ := store.Get(ctx, "lockExKey")
	require.True(t, ok)
	require.Equal(t, "valExFromGetter", got)
}

// mockPubSub is a mock implementation of pubsub.PubSub.
type mockPubSub[M any] struct {
	publishFunc   func(ctx context.Context, channelID string, message M) error
	subscribeFunc func(ctx context.Context, channelID string, optSubcriptionID ...string) (pubsub.Subscription[M], error)
}

func (m mockPubSub[M]) IsRunning() bool {
	panic("unimplemented")
}

func (m mockPubSub[M]) NumSubscribers(channelID string) (int, error) {
	panic("unimplemented")
}

func (m mockPubSub[M]) Publish(ctx context.Context, channelID string, message M) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, channelID, message)
	}
	return nil
}

func (m mockPubSub[M]) Run(ctx context.Context) error {
	panic("unimplemented")
}

func (m mockPubSub[M]) Stop() {
	panic("unimplemented")
}

func (m mockPubSub[M]) Subscribe(ctx context.Context, channelID string, optSubcriptionID ...string) (pubsub.Subscription[M], error) {
	if m.subscribeFunc != nil {
		return m.subscribeFunc(ctx, channelID, optSubcriptionID...)
	}
	return nil, fmt.Errorf("SubscribeFunc not set")
}

// mockSubscription is a mock implementation of pubsub.Subscription.
type mockSubscription[M any] struct {
	msgCh chan M
}

func (m *mockSubscription[M]) ChannelID() string {
	panic("unimplemented")
}

func (m *mockSubscription[M]) SendMessage(ctx context.Context, message M) error {
	panic("unimplemented")
}

func (m *mockSubscription[M]) ReadMessage() <-chan M {
	return m.msgCh
}

func (m *mockSubscription[M]) Done() <-chan struct{} {
	panic("unimplemented")
}

func (m *mockSubscription[M]) Err() error {
	panic("unimplemented")
}

func (m *mockSubscription[M]) Unsubscribe() {
	close(m.msgCh)
}
