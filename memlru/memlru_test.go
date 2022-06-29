package memlru

import (
	"context"
	"fmt"
	"testing"
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
		v, err := c.Get(ctx, fmt.Sprintf("i%d", i))
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
		_, err := c.Get(ctx, fmt.Sprintf("i%d", i))
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
		v, err := c.Get(ctx, fmt.Sprintf("i%d", i))
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
		_, err := c.Get(ctx, fmt.Sprintf("i%d", i))
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
		v, err := c.Get(ctx, fmt.Sprintf("i%d", i))
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
		l, err := c.Get(ctx, fmt.Sprintf("i%d", i))
		if err != nil {
			t.Errorf("expected %v to not be in cache", l)
		}
	}
}
