package cachestorectl

import (
	"fmt"

	"github.com/goware/cachestore"
	"github.com/goware/cachestore/memlru"
)

func Open[T any](backend cachestore.Backend) (cachestore.Store[T], error) {
	config := backend.Config()

	switch t := config.(type) {

	case *memlru.Config:
		return memlru.NewWithBackend[T](backend)

	default:
		return nil, fmt.Errorf("cachestorectl: unknown cachestore backend %T", t)

	}
}
