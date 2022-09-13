package main

import (
	"context"
	"fmt"

	"github.com/goware/cachestore/redis"
)

func main() {
	store, err := redis.New[string](&redis.Config{
		Host: "localhost",
		Port: 6379,
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

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

	fmt.Println("ok")

}
