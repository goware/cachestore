package main

import (
	"context"
	"fmt"

	"github.com/goware/cachestore/cachestorectl"
	"github.com/goware/cachestore/redis"
)

func main() {
	cfg := &redis.Config{
		Enabled: true,
		Host:    "localhost",
		Port:    6379,
	}

	backend := redis.Backend(cfg)

	store, err := cachestorectl.Open[string](backend)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	{
		for i := 0; i < 100; i++ {
			err = store.Set(ctx, fmt.Sprintf("foo:%d", i), fmt.Sprintf("value-%d", i))
			if err != nil {
				panic(err)
			}
		}

		err = store.DeletePrefix(ctx, "foo")
		if err != nil {
			panic(err)
		}

		fmt.Println("done.")
		fmt.Println("")
	}

}
