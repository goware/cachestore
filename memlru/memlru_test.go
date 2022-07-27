package memlru

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goware/cachestore"
	"github.com/stretchr/testify/require"
)

func TestCacheInt(t *testing.T) {
	c, err := NewWithSize[int](50, cachestore.WithDefaultKeyExpiry(12*time.Second))

	mc, ok := c.(*MemLRU[int])
	require.True(t, ok)
	require.True(t, mc.options.DefaultKeyExpiry.Seconds() == 12)

	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		c.Set(ctx, fmt.Sprintf("i%d", i), i)
	}
	for i := 0; i < 10; i++ {
		v, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %d to be in cache", i)
		}
		if v != i {
			t.Errorf("expected %d to be %d", v, i)
		}
	}
	for i := 0; i < 10; i++ {
		c.Delete(ctx, fmt.Sprintf("i%d", i))
	}
	for i := 0; i < 10; i++ {
		_, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %d to not be in cache", i)
		}
	}
}

func TestCacheString(t *testing.T) {
	c, err := NewWithSize[string](50)

	mc, ok := c.(*MemLRU[string])
	require.True(t, ok)
	require.True(t, mc.options.DefaultKeyExpiry == 0)

	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		c.Set(ctx, fmt.Sprintf("i%d", i), fmt.Sprintf("v%d", i))
	}
	for i := 0; i < 10; i++ {
		v, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %d to be in cache", i)
		}
		if v != fmt.Sprintf("v%d", i) {
			t.Errorf("expected %s to be %s", v, fmt.Sprintf("v%d", i))
		}
	}
	for i := 0; i < 10; i++ {
		c.Delete(ctx, fmt.Sprintf("i%d", i))
	}
	for i := 0; i < 10; i++ {
		_, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %d to not be in cache", i)
		}
	}
}

func TestCacheObject(t *testing.T) {
	type custom struct {
		value int
		data  string
	}

	c, err := NewWithSize[custom](50)
	ctx := context.Background()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		c.Set(ctx, fmt.Sprintf("i%d", i), custom{i, fmt.Sprintf("v%d", i)})
	}
	for i := 0; i < 10; i++ {
		v, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %d to be in cache", i)
		}
		value := custom{
			i,
			fmt.Sprintf("v%d", i),
		}
		if v != value {
			t.Errorf("expected %v to be %v", v, custom{i, fmt.Sprintf("v%d", i)})
		}
	}
	for i := 0; i < 10; i++ {
		c.Delete(ctx, fmt.Sprintf("i%d", i))
	}
	for i := 0; i < 10; i++ {
		l, _, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %v to not be in cache", l)
		}
	}
}

type obj struct {
	A string
	B string
}

func TestBasicBatchObjects(t *testing.T) {
	cache, err := NewWithSize[*obj](50)
	if err != nil {
		t.Fatal(err)
	}

	var keys = []string{
		"test-obj3-a", "test-obj3-b",
	}

	var in = []*obj{
		{A: "3a", B: "3a"},
		{A: "3b", B: "3b"},
	}

	ctx := context.Background()
	err = cache.BatchSet(ctx, keys, in)
	require.NoError(t, err)

	// adding some keys which will not exist
	fetchKeys := []string{"no1"}
	fetchKeys = append(fetchKeys, keys...)
	fetchKeys = append(fetchKeys, []string{"no2", "no3"}...)

	out, exists, err := cache.BatchGet(ctx, fetchKeys)
	require.NoError(t, err)
	require.Equal(t, []*obj{nil, in[0], in[1], nil, nil}, out)
	require.Equal(t, []bool{false, true, true, false, false}, exists)
}

func TestExpiryOptions(t *testing.T) {
	ctx := context.Background()

	cache, err := New[string](cachestore.WithDefaultKeyExpiry(1 * time.Second))
	require.NoError(t, err)

	rcache, ok := cache.(*MemLRU[string])
	require.True(t, ok)
	require.True(t, rcache.options.DefaultKeyExpiry.Seconds() == 1)

	err = cache.Set(ctx, "hi", "bye")
	require.NoError(t, err)

	err = cache.SetEx(ctx, "another", "longer", 20*time.Second)
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "bye", value)

	// pause to wait for expiry.. we have to wait at least 5 seconds
	// as memLRU does expiry cycles that amount of time
	time.Sleep(6 * time.Second)

	value, exists, err = cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, "", value)

	value, exists, err = cache.Get(ctx, "another")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "longer", value)
}
