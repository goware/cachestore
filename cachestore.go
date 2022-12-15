package cachestore

import (
	"context"
	"errors"
	"time"
)

var (
	MaxKeyLength = 80

	ErrKeyLengthTooLong = errors.New("cachestore: key length is too long")
	ErrInvalidKey       = errors.New("cachestore: invalid key")
)

type Store[V any] interface {
	// Returns true if the key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Set stores the given value associated to the key.
	Set(ctx context.Context, key string, value V) error

	// SetEx stores the given value associated to the key and sets an expiry ttl
	// for that key.
	SetEx(ctx context.Context, key string, value V, ttl time.Duration) error

	// BatchSet sets all the values associated to the given keys.
	BatchSet(ctx context.Context, keys []string, values []V) error

	// BatchSetEx sets all the values associated to the given keys and sets an
	// expiry ttl for each key.
	BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error

	// Get returns a stored value, or nil if the value is not assigned.
	Get(ctx context.Context, key string) (V, bool, error)

	// BatchGet returns the values of all the given keys at once. If any of the
	// keys has no value, nil is returned instead.
	BatchGet(ctx context.Context, keys []string) ([]V, []bool, error)

	// Delete removes a key and its associated value.
	Delete(ctx context.Context, key string) error

	// DeletePrefix removes keys with the given prefix.
	DeletePrefix(ctx context.Context, keyPrefix string) error

	// ClearAll removes all data from the cache. Only use this during debugging,
	// or testing, and never in practice.
	ClearAll(ctx context.Context) error
}

type Backend interface {
	Config() any
}
