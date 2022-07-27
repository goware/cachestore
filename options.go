package cachestore

import "time"

func ApplyOptions(opts ...StoreOptions) StoreOptions {
	if len(opts) == 0 {
		return StoreOptions{}
	}
	so := StoreOptions{}
	for _, opt := range opts {
		opt.Apply(&so)
	}
	return so
}

func WithDefaultKeyExpiry(keyExpiry time.Duration) StoreOptions {
	return StoreOptions{
		Apply: func(opts *StoreOptions) {
			opts.DefaultKeyExpiry = keyExpiry
		},
	}
}

// NOTE: currently not in use, but we could add it
// func WithDefaultKeyPrefix(keyPrefix string) StoreOptions {
// 	return StoreOptions{
// 		Apply: func(opts *StoreOptions) {
// 			opts.DefaultKeyPrefix = keyPrefix
// 		},
// 	}
// }

type StoreOptions struct {
	Apply            func(*StoreOptions)
	DefaultKeyExpiry time.Duration
	// DefaultKeyPrefix string
}
