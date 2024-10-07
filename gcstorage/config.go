package gcstorage

import (
	"github.com/goware/cachestore"
)

type Config struct {
	cachestore.StoreOptions

	Bucket string
	Prefix string
}

func (c *Config) Apply(options *cachestore.StoreOptions) {
	c.StoreOptions.Apply(options)
}
