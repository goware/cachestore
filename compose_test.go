package cachestore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/memlru"
	"github.com/stretchr/testify/require"
)

func TestCompose(t *testing.T) {
	n := 20

	m1s, err := memlru.NewWithSize[string](n, cachestore.WithDefaultKeyExpiry(6*time.Second))
	require.NoError(t, err)

	m2s, err := memlru.NewWithSize[string](n, cachestore.WithDefaultKeyExpiry(9*time.Second))
	require.NoError(t, err)

	cs, err := cachestore.Compose(m1s, m2s)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < n; i++ {
		err := m1s.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("a%d", i))
		require.NoError(t, err)
	}
	for i := 0; i < n; i++ {
		err := cs.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("b%d", i))
		require.NoError(t, err)
	}

	// Get some keys
	{
		key := "foo:10"

		val, ok, err := m1s.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "b10", val)

		val, ok, err = m2s.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "b10", val)
	}

	// Overwrite random keys
	{
		m1s.Set(ctx, "foo:8", "c8")
		m2s.Set(ctx, "foo:8", "d8")
		m2s.Set(ctx, "foo:7", "d7")
		m2s.Set(ctx, "foo:6", "d6")
		cs.Set(ctx, "foo:6", "c6")

		val, ok, err := cs.Get(ctx, "foo:8")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "c8", val)

		val, ok, err = cs.Get(ctx, "foo:7")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "b7", val)

		val, ok, err = cs.Get(ctx, "foo:6")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "c6", val)
	}

	// Batch get, etc.
	{
		// first lets delete some random keys in both sets
		// so when we query we should get all values
		m1s.Delete(ctx, "foo:2")
		m1s.Delete(ctx, "foo:5")
		m1s.Delete(ctx, "foo:8")
		m2s.Delete(ctx, "foo:3")
		m2s.Delete(ctx, "foo:4")
		m2s.Delete(ctx, "foo:10")

		vals, exists, err := cs.BatchGet(ctx, []string{
			"foo:1", "foo:2", "foo:3", "foo:4", "foo:5", "foo:6", "foo:7", "foo:8", "foo:9", "foo:10",
		})
		require.NoError(t, err)
		for _, e := range exists {
			require.True(t, e)
		}
		for _, v := range vals {
			require.NotEmpty(t, v)
		}
	}

	// Wait for expiry..
	time.Sleep(7 * time.Second)

	{
		val, ok, err := m1s.Get(ctx, "foo:1")
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, val)

		val, ok, err = m2s.Get(ctx, "foo:1")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEmpty(t, val)

		val, ok, err = cs.Get(ctx, "foo:1")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEmpty(t, val)
	}

}
