package main

import (
	"context"
	"fmt"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/cachestorectl"
	"github.com/goware/cachestore/redis"
)

func main() {
	cfg := &redis.Config{
		Enabled: true,
		Host:    "localhost",
		Port:    6379,
	}

	backend := redis.Backend(cfg) //, cachestore.WithDefaultKeyExpiry(1*time.Second))

	store, err := cachestorectl.Open[string](backend, cachestore.WithDefaultKeyExpiry(1*time.Second))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// Set
	for i := 0; i < 100; i++ {
		err = store.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("value-%d", i))
		if err != nil {
			panic(err)
		}
	}

	store.SetEx(ctx, "foo:999", "value-999", 10*time.Minute)

	// Get
	v, ok, err := store.Get(ctx, "foo:10")
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("unexpected")
	}
	fmt.Println("=> get(foo:10) =", v)

	time.Sleep(2 * time.Second)

	// should expire based on rule above
	v, ok, err = store.Get(ctx, "foo:10")
	if err != nil {
		panic(err)
	}
	if ok {
		panic("unexpected")
	}
	fmt.Println("=> get(foo:10) =", v)

	// should still have
	v, ok, err = store.Get(ctx, "foo:999")
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("unexpected")
	}
	fmt.Println("=> get(foo:999) =", v)

	// DeletePrefix
	err = store.DeletePrefix(ctx, "foo:")
	if err != nil {
		panic(err)
	}

	// be gone
	_, ok, _ = store.Get(ctx, "foo:999")
	if ok {
		panic("unexpected")
	}

	fmt.Println("done.")
	fmt.Println("")
}
