package cachestorectl

import (
	"fmt"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/memlru"
	"github.com/goware/cachestore/nostore"
	"github.com/goware/cachestore/redis"
)

func Open[T any](backend cachestore.Backend) (cachestore.Store[T], error) {
	switch t := backend.(type) {

	case *memlru.Config:
		return memlru.NewWithBackend[T](backend)

	case *redis.Config:
		return redis.NewWithBackend[T](backend)

	case *nostore.Config:
		return nostore.New[T]()

	default:
		return nil, fmt.Errorf("cachestorectl: unknown cachestore backend %T", t)

	}
}
