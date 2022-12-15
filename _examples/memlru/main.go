package main

import (
	"context"
	"fmt"

	"github.com/goware/cachestore/cachestorectl"
	"github.com/goware/cachestore/memlru"
)

func main() {
	backend := memlru.Backend(5)

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
