package memlru

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestExpirationQueue(t *testing.T) {
	q := newExpirationQueue()

	q.Push("a", time.Millisecond*100)
	q.Push("b", time.Millisecond*150)
	q.Push("c", time.Millisecond*50)
	q.Push("d", time.Millisecond*350)
	q.Push("e", time.Millisecond*300)
	q.Push("f", time.Millisecond*50)

	require.Equal(t, 6, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			require.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	q.Push("f", time.Millisecond*500)

	require.Equal(t, 6, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			require.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	{
		keys := q.expiredAt(time.Now())

		require.Equal(t, 6, q.Len())
		require.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Second * -1))

		require.Equal(t, 6, q.Len())
		require.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Millisecond * 200))

		require.Equal(t, 3, q.Len())
		require.Equal(t, 3, len(keys))
	}

	for i := 0; i < 100; i++ {
		q.Push("z", time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	require.Equal(t, 4, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			require.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	for i := 0; i < 100; i++ {
		q.Push(fmt.Sprintf("key-%d", i), time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	require.Equal(t, 104, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			require.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	{
		keys := q.expiredAt(time.Now())
		require.Equal(t, 104, q.Len())
		require.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Second * 10))
		require.Equal(t, 0, q.Len())
		require.Equal(t, 104, len(keys))
	}

	for i := 0; i < 100; i++ {
		q.Push(fmt.Sprintf("key-%d", i), time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	{
		var lastTime time.Time
		for _, key := range q.keys {
			require.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	require.Equal(t, 100, q.Len())
}

func TestSetEx(t *testing.T) {
	ctx := context.Background()

	c, err := NewWithSize[[]byte](50)
	require.NoError(t, err)

	{
		keys := []string{}
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%d", i)

			// SetEx with time 0 is the same as just a Set, because there is no expiry time
			// aka, the key doesn't expire.
			err := c.SetEx(ctx, key, []byte("a"), time.Duration(0))
			require.NoError(t, err)
			keys = append(keys, key)
		}

		for _, key := range keys {
			buf, exists, err := c.Get(ctx, key)
			require.True(t, exists)
			require.NoError(t, err)
			require.NotNil(t, buf)

			exists, err = c.Exists(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
		}

		values, batchExists, err := c.BatchGet(ctx, keys)
		require.NoError(t, err)
		for i := range values {
			require.NotNil(t, values[i])
			require.True(t, batchExists[i])
		}
	}

	{
		keys := []string{}
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%d", i)
			err := c.SetEx(ctx, key, []byte("a"), time.Second*10) // a key that expires in 10 seconds
			require.NoError(t, err)
			keys = append(keys, key)
		}

		for _, key := range keys {
			buf, exists, err := c.Get(ctx, key)
			require.NoError(t, err)
			require.NotNil(t, buf)
			require.True(t, exists)

			exists, err = c.Exists(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
		}

		values, batchExists, err := c.BatchGet(ctx, keys)
		require.NoError(t, err)

		for i := range values {
			require.NotNil(t, values[i])
			require.True(t, batchExists[i])
		}
	}
}

func TestGetEx(t *testing.T) {
	ctx := context.Background()

	cache, err := NewWithSize[[]byte](50)
	require.NoError(t, err)

	err = cache.SetEx(ctx, "hi", []byte("bye"), 10*time.Second)
	require.NoError(t, err)

	v, ttl, exists, err := cache.GetEx(ctx, "hi")
	require.NoError(t, err)
	require.True(t, exists)
	require.InDelta(t, 10*time.Second, ttl, float64(1*time.Second), "TTL are not equal within the allowed delta")
	require.Equal(t, []byte("bye"), v)

	v, ttl, exists, err = cache.GetEx(ctx, "not-found")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, time.Duration(0), ttl)

	err = cache.Set(ctx, "without-ttl", []byte("hello"))
	require.NoError(t, err)

	v, ttl, exists, err = cache.GetEx(ctx, "without-ttl")
	require.Error(t, err)
}
