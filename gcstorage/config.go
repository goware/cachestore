package gcstorage

import (
	"github.com/goware/cachestore"
	"golang.org/x/oauth2/google"
)

type Config struct {
	cachestore.StoreOptions

	Credentials *google.Credentials

	Bucket    string
	KeyPrefix string
}

func (c *Config) Apply(options *cachestore.StoreOptions) {
	c.StoreOptions.Apply(options)
}
