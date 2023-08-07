package main

import (
	"context"
	"log"
	"time"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/cachestorectl"
	"github.com/goware/cachestore/redis"
	"golang.org/x/sync/errgroup"
)

func main() {
	cfg := &redis.Config{
		Enabled: true,
		Host:    "localhost",
		Port:    6379,
	}

	backend := redis.Backend(cfg) //, cachestore.WithDefaultKeyExpiry(1*time.Second))

	store, err := cachestorectl.Open[string](backend, cachestore.WithDefaultKeyExpiry(10*time.Second))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	rs := store.(*redis.RedisStore[string])

	getter := func(ctx context.Context, key string) (string, error) {
		log.Println("CACHE MISS")
		select {
		case <-time.After(3 * time.Second):
			return "result of " + key, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	var wg errgroup.Group
	for i := 0; i < 15; i++ {
		wg.Go(func() error {
			out, err := rs.GetOrSetWithLock(ctx, "totally-random-key-1234567890", getter)
			if err != nil {
				return err
			}
			log.Printf("Got response: %q\n", out)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		panic(err)
	}
}
