package invcache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/invcache"
	"github.com/goware/cachestore/memlru"
	"github.com/goware/pubsub"
)

func TestCacheInvalidator_Listen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := &mockSubscription[invcache.CacheInvalidationMessage]{
		msgCh: make(chan invcache.CacheInvalidationMessage, 5),
	}
	mps := &mockPubSub[invcache.CacheInvalidationMessage]{
		subscribeFunc: func(ctx context.Context, chID string, optSubscriptionID ...string) (pubsub.Subscription[invcache.CacheInvalidationMessage], error) {
			require.Equal(t, "cache_invalidation", chID)
			return sub, nil
		},
	}

	store, err := memlru.NewWithSize[string](5, cachestore.WithDefaultKeyExpiry(time.Minute))
	require.NoError(t, err)
	ic := invcache.NewInvalidatingCache(store, mps, "local-instance")
	ci := invcache.NewCacheInvalidator(*ic, mps, "local-instance")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ci.Listen(ctx)
	}()

	require.NoError(t, ic.Set(ctx, "foo", "bar"))

	_, ok, err := ic.Get(ctx, "foo")
	require.NoError(t, err)
	require.True(t, ok, "foo should be in cache")

	// Invalidate cache from remote message
	sub.msgCh <- invcache.CacheInvalidationMessage{
		Key:    "foo",
		Origin: "remote-instance",
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, ok, err := ic.Get(ctx, "foo")
		require.NoError(c, err)
		require.False(c, ok, "foo should be invalidated from remote message")
	}, 10*time.Second, 1*time.Second, "foo has not been invalidated; still in cache")
}
