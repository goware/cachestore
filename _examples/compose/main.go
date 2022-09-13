package main

import (
	"context"
	"fmt"
	"log"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/memlru"
	"github.com/goware/cachestore/redis"
)

func main() {

	mstore, err := memlru.New[string]()
	if err != nil {
		panic(err)
	}

	rstore, err := redis.New[string](&redis.Config{
		Host: "localhost",
		Port: 6379,
	})
	if err != nil {
		panic(err)
	}

	// Compose a store chain where we set/get keys in order of:
	// -> memlru -> redis store
	//
	// This allows you to combine cachestores easily, so you can have a quick
	// local cache in memory, and search the remote redis cache if its not in memory.
	store, err := cachestore.Compose(mstore, rstore)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	{
		fmt.Println("Setting bunch of values in both stores..\n")

		for i := 0; i < 100; i++ {
			err = store.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("value-%d", i))
			if err != nil {
				panic(err)
			}
		}
	}

	{
		key := "foo:42"
		fmt.Printf("Get value from memlru, key=%s\n", key)

		val, ok, err := mstore.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal("expecting memlru value")
		}
		fmt.Println("got value:", val, "\n")
	}

	{
		key := "foo:42"
		fmt.Printf("Get value from redis, key=%s\n", key)

		val, ok, err := rstore.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal("expecting redis value")
		}
		fmt.Println("got value:", val, "\n")
	}

	{
		key := "foo:42"
		fmt.Printf("Get value from composed store, key=%s\n", key)

		val, ok, err := store.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal("expecting composed store value")
		}
		fmt.Println("got value:", val, "\n")
	}

	{
		// clearing redis
		rstore.DeletePrefix(ctx, "foo")
		key := "foo:42"
		_, ok, err := rstore.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			log.Fatal("expecting value to be gone from redis")
		}

		// ask composed store
		fmt.Printf("Get value from composed store (part 2), key=%s\n", key)

		val, ok, err := store.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			log.Fatal("expecting composed store value")
		}
		fmt.Println("got value:", val, "\n")
	}

	{
		err = store.DeletePrefix(ctx, "foo")
		if err != nil {
			log.Fatal(err)
		}

		// ask composed store
		key := "foo:42"
		fmt.Printf("Get value from composed store (part 3), key=%s\n", key)

		val, ok, err := store.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if ok || val != "" {
			log.Fatal("not expecting composed store value")
		}
	}

	fmt.Println("ok")
}
