package memlru

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, 6, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			assert.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	q.Push("f", time.Millisecond*500)

	assert.Equal(t, 6, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			assert.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	{
		keys := q.expiredAt(time.Now())

		assert.Equal(t, 6, q.Len())
		assert.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Second * -1))

		assert.Equal(t, 6, q.Len())
		assert.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Millisecond * 200))

		assert.Equal(t, 3, q.Len())
		assert.Equal(t, 3, len(keys))
	}

	for i := 0; i < 100; i++ {
		q.Push("z", time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	assert.Equal(t, 4, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			assert.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	for i := 0; i < 100; i++ {
		q.Push(fmt.Sprintf("key-%d", i), time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	assert.Equal(t, 104, q.Len())

	{
		var lastTime time.Time
		for _, key := range q.keys {
			assert.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	{
		keys := q.expiredAt(time.Now())
		assert.Equal(t, 104, q.Len())
		assert.Equal(t, 0, len(keys))
	}

	{
		keys := q.expiredAt(time.Now().Add(time.Second * 10))
		assert.Equal(t, 0, q.Len())
		assert.Equal(t, 104, len(keys))
	}

	for i := 0; i < 100; i++ {
		q.Push(fmt.Sprintf("key-%d", i), time.Millisecond*time.Duration(50+rand.Intn(500)))
	}

	{
		var lastTime time.Time
		for _, key := range q.keys {
			assert.LessOrEqual(t, lastTime, key.expiresAt)
			lastTime = key.expiresAt
		}
	}

	assert.Equal(t, 100, q.Len())
}

func TestSetEx(t *testing.T) {
	ctx := context.Background()

	c, err := NewWithSize[[]byte](50)
	assert.NoError(t, err)

	{
		keys := []string{}
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%d", i)
			err := c.SetEx(ctx, key, []byte("a"), time.Duration(0)) // a key that expires immediately
			assert.NoError(t, err)
			keys = append(keys, key)
		}

		for _, key := range keys {
			buf, exists, err := c.Get(ctx, key)
			assert.False(t, exists)
			assert.NoError(t, err)
			assert.Nil(t, buf)

			exists, err = c.Exists(ctx, key)
			assert.NoError(t, err)
			assert.False(t, exists)
		}

		values, batchExists, err := c.BatchGet(ctx, keys)
		assert.NoError(t, err)

		for i := range values {
			assert.Nil(t, values[i])
			assert.False(t, batchExists[i])
		}
	}

	{
		keys := []string{}
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key-%d", i)
			err := c.SetEx(ctx, key, []byte("a"), time.Second*10) // a key that expires in 10 seconds
			assert.NoError(t, err)
			keys = append(keys, key)
		}

		for _, key := range keys {
			buf, exists, err := c.Get(ctx, key)
			assert.NoError(t, err)
			assert.NotNil(t, buf)
			assert.True(t, exists)

			exists, err = c.Exists(ctx, key)
			assert.NoError(t, err)
			assert.True(t, exists)
		}

		values, batchExists, err := c.BatchGet(ctx, keys)
		assert.NoError(t, err)

		for i := range values {
			assert.NotNil(t, values[i])
			assert.True(t, batchExists[i])
		}
	}
}
