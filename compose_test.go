package cachestore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/goware/cachestore"
)

func TestCompose(t *testing.T) {
	n := 20

	m1s := NewMockStore[string]()
	m2s := NewMockStore[string]()

	cs, err := cachestore.Compose(m1s, m2s)
	assertNoError(t, err)

	ctx := context.Background()

	for i := 0; i < n; i++ {
		err := m1s.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("a%d", i))
		assertNoError(t, err)
	}
	for i := 0; i < n; i++ {
		err := cs.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("b%d", i))
		assertNoError(t, err)
	}

	// Get some keys
	{
		key := "foo:10"

		val, ok, err := m1s.Get(ctx, key)
		assertNoError(t, err)
		assertTrue(t, ok)
		assertEqual(t, "b10", val)

		val, ok, err = m2s.Get(ctx, key)
		assertNoError(t, err)
		assertTrue(t, ok)
		assertEqual(t, "b10", val)
	}

	// Overwrite random keys
	{
		m1s.Set(ctx, "foo:8", "c8")
		m2s.Set(ctx, "foo:8", "d8")
		m2s.Set(ctx, "foo:7", "d7")
		m2s.Set(ctx, "foo:6", "d6")
		cs.Set(ctx, "foo:6", "c6")

		val, ok, err := cs.Get(ctx, "foo:8")
		assertNoError(t, err)
		assertTrue(t, ok)
		assertEqual(t, "c8", val)

		val, ok, err = cs.Get(ctx, "foo:7")
		assertNoError(t, err)
		assertTrue(t, ok)
		assertEqual(t, "b7", val)

		val, ok, err = cs.Get(ctx, "foo:6")
		assertNoError(t, err)
		assertTrue(t, ok)
		assertEqual(t, "c6", val)
	}

	// Batch get, etc.
	{
		// first lets normalize all the values again
		for i := 0; i < n; i++ {
			err := cs.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("z%d", i))
			assertNoError(t, err)
		}

		// then lets delete some random keys in both sets
		// so when we query we should get all values
		m1s.Delete(ctx, "foo:2")
		m1s.Delete(ctx, "foo:5")
		m1s.Delete(ctx, "foo:8")
		m2s.Delete(ctx, "foo:3")
		m2s.Delete(ctx, "foo:4")
		m2s.Delete(ctx, "foo:10")

		vals, exists, err := cs.BatchGet(ctx, []string{
			"foo:0", "foo:1", "foo:2", "foo:3", "foo:4", "foo:5", "foo:6", "foo:7", "foo:8", "foo:9", "foo:10",
		})
		assertNoError(t, err)
		for _, e := range exists {
			assertTrue(t, e)
		}
		for i, v := range vals {
			assertTrue(t, len(v) > 0)
			assertEqual(t, fmt.Sprintf("z%d", i), v)
		}
	}

}
