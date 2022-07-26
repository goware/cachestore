package redis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicString(t *testing.T) {
	ctx := context.Background()

	cache, err := New[string](&Config{Host: "localhost"})
	require.NoError(t, err)

	err = cache.Set(ctx, "hi", "bye")
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "bye", value)
}

func TestBasicBytes(t *testing.T) {
	ctx := context.Background()

	cache, err := New[[]byte](&Config{Host: "localhost"})
	require.NoError(t, err)

	err = cache.Set(ctx, "test-bytes", []byte{1, 2, 3, 4})
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "test-bytes")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, []byte{1, 2, 3, 4}, value)
}

type obj struct {
	A string
	B string
}

func TestBasicObject(t *testing.T) {
	ctx := context.Background()

	cache, err := New[obj](&Config{Host: "localhost"})
	require.NoError(t, err)

	var in = obj{A: "hi", B: "bye"}

	err = cache.Set(ctx, "test-obj", in)
	require.NoError(t, err)

	out, exists, err := cache.Get(ctx, "test-obj")
	require.True(t, exists)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestBasicObject2(t *testing.T) {
	ctx := context.Background()

	cache, err := New[*obj](&Config{Host: "localhost"})
	require.NoError(t, err)

	var in = &obj{A: "hi", B: "bye"}

	err = cache.Set(ctx, "test-obj2", in)
	require.NoError(t, err)

	out, exists, err := cache.Get(ctx, "test-obj2")
	require.True(t, exists)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestBasicBatchObjects(t *testing.T) {
	ctx := context.Background()

	cache, err := New[*obj](&Config{Host: "localhost"})
	require.NoError(t, err)

	var keys = []string{
		"test-obj3-a", "test-obj3-b",
	}

	var in = []*obj{
		{A: "3a", B: "3a"},
		{A: "3b", B: "3b"},
	}

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

func TestBasicBatchObjectEmptyKeys(t *testing.T) {
	ctx := context.Background()

	cache, err := New[*obj](&Config{Host: "localhost"})
	require.NoError(t, err)

	var keys = []string{}

	var in = []*obj{}

	err = cache.BatchSet(ctx, keys, in)
	require.Error(t, err)

}
