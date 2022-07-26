package memlru

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheInt(t *testing.T) {
	c, err := NewWithSize[int](50)
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
