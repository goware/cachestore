package cachestore

import "time"

func ApplyOptions(opts ...StoreOptions) StoreOptions {
	if len(opts) == 0 {
		return StoreOptions{
			Apply: func(opts *StoreOptions) {},
		}
	}
	so := opts[0]
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

func WithLockExpiry(lockExpiry time.Duration) StoreOptions {
	return StoreOptions{
		Apply: func(opts *StoreOptions) {
			opts.LockExpiry = lockExpiry
		},
	}
}

func WithLockRetryTimeout(retryTimeout time.Duration) StoreOptions {
	return StoreOptions{
		Apply: func(opts *StoreOptions) {
			opts.LockRetryTimeout = retryTimeout
		},
	}
}

func WithLockRetryDelay(minDelay, maxDelay time.Duration) StoreOptions {
	return StoreOptions{
		Apply: func(opts *StoreOptions) {
			opts.LockMinRetryDelay = minDelay
			opts.LockMaxRetryDelay = maxDelay
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

	LockExpiry        time.Duration
	LockRetryTimeout  time.Duration
	LockMinRetryDelay time.Duration
	LockMaxRetryDelay time.Duration
}
