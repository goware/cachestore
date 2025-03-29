package memlru

import "github.com/goware/cachestore"

type Config struct {
	cachestore.StoreOptions
	Size int
}

func (c *Config) Apply(options *cachestore.StoreOptions) {
	c.StoreOptions.Apply(options)
}
