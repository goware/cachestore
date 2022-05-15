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

type Store interface {
	// Returns true if the key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Set stores the given value associated to the key.
	Set(ctx context.Context, key string, value []byte) error

	// SetEx stores the given value associated to the key and sets an expiry ttl
	// for that key.
	SetEx(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// BatchSet sets all the values associated to the given keys.
	BatchSet(ctx context.Context, keys []string, values [][]byte) error

	// BatchSetEx sets all the values associated to the given keys and sets an
	// expiry ttl for each key.
	BatchSetEx(ctx context.Context, keys []string, values [][]byte, ttl time.Duration) error

	// Get returns a stored value, or nil if the value is not assigned.
	Get(ctx context.Context, key string) ([]byte, error)

	// BatchGet returns the values of all the given keys at once. If any of the
	// keys has no value, nil is returned instead.
	BatchGet(ctx context.Context, keys []string) ([][]byte, error)

	// Delete removes a key and its associated value.
	Delete(ctx context.Context, key string) error
}
