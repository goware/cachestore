package invstore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/invstore"
	"github.com/goware/cachestore/memlru"
	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

const (
	RemoteInstance = invstore.InstanceID("remote-instance")
	LocalInstance  = invstore.InstanceID("local-instance")
)

func TestCacheInvalidator_Listen(t *testing.T) {
	type testCase struct {
		name          string
		initial       map[string]string
		msg           invstore.StoreInvalidationMessage
		expectRemoved []string
		expectRemain  []string
	}

	getHash := func(val string) string {
		h, err := invstore.ComputeHash(val)
		require.NoError(t, err)
		return h
	}

	tests := []testCase{
		{
			name: "Single key, no hash",
			initial: map[string]string{
				"key": "val",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"key"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{"foo"},
		},
		{
			name: "Single key, matching hash",
			initial: map[string]string{
				"key": "val",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:        []string{"key"},
				Origin:      RemoteInstance,
				ContentHash: getHash("val"),
			},
			expectRemain: []string{"key"},
		},
		{
			name: "Single key, mismatching hash",
			initial: map[string]string{
				"key": "val",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:        []string{"key"},
				Origin:      RemoteInstance,
				ContentHash: getHash("oldVal"),
			},
			expectRemoved: []string{"key"},
		},
		{
			name: "Multiple keys",
			initial: map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"key1", "key2"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{"key1", "key2"},
			expectRemain:  []string{"key3"},
		},
		{
			name: "ClearAll success",
			initial: map[string]string{
				"key1": "val1",
				"key2": "val2",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"*"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{"key1", "key2"},
		},
		{
			name: "DeletePrefix success",
			initial: map[string]string{
				"abc1": "val1",
				"abc2": "val2",
				"xyz":  "zzz",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"abc*"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{"abc1", "abc2"},
			expectRemain:  []string{"xyz"},
		},
		{
			name: "ClearAll fail: batch",
			initial: map[string]string{
				"key1": "val1",
				"key2": "val2",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"key", "*"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{},
			expectRemain:  []string{"key1", "key2"},
		},
		{
			name: "DeletePrefix fail: batch",
			initial: map[string]string{
				"key1": "val1",
				"key2": "val2",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"key", "key*"},
				Origin: RemoteInstance,
			},
			expectRemoved: []string{},
			expectRemain:  []string{"key1", "key2"},
		},
		{
			name: "Local origin: skip",
			initial: map[string]string{
				"localKey": "val",
			},
			msg: invstore.StoreInvalidationMessage{
				Keys:   []string{"localKey"},
				Origin: LocalInstance,
			},
			expectRemain: []string{"localKey"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sub := &mockSubscription[invstore.StoreInvalidationMessage]{
				msgCh:  make(chan invstore.StoreInvalidationMessage, 10),
				doneCh: make(chan struct{}),
			}
			mps := &mockPubSub[invstore.StoreInvalidationMessage]{
				subscribeFunc: func(ctx context.Context, chID string, opt ...string) (pubsub.Subscription[invstore.StoreInvalidationMessage], error) {
					require.Equal(t, "store_invalidation", chID)
					return sub, nil
				},
			}

			store, err := memlru.NewWithSize[string](10, cachestore.WithDefaultKeyExpiry(time.Minute))
			require.NoError(t, err)

			for k, v := range tc.initial {
				require.NoError(t, store.Set(ctx, k, v))
			}

			logger := logger.Nop()
			ic := invstore.NewInvalidatingStore(store, mps)
			ci := invstore.NewStoreInvalidator(logger, *ic, mps)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ci.Listen(ctx)
				require.NoError(t, err)
			}()

			sub.msgCh <- tc.msg

			for _, remainKey := range tc.expectRemain {
				val, ok, err := store.Get(ctx, remainKey)
				require.NoError(t, err)
				require.True(t, ok)
				if expectedVal, had := tc.initial[remainKey]; had {
					require.Equal(t, expectedVal, val)
				}
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				for _, removedKey := range tc.expectRemoved {
					_, ok, err := store.Get(ctx, removedKey)
					require.NoError(t, err)
					assert.False(t, ok, "%s should be removed from remote message", removedKey)
				}
			}, 10*time.Second, 1*time.Second, "foo has not been invalidated; still in cache")

			cancel()
			wg.Wait()
		})
	}
}
