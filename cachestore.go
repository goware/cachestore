package cachestore

import (
	"context"
	"errors"
	"time"
)

var (
	MaxKeyLength = 200

	ErrKeyLengthTooLong = errors.New("cachestore: key length is too long")
	ErrInvalidKey       = errors.New("cachestore: invalid key")
	ErrInvalidKeyPrefix = errors.New("cachestore: invalid key prefix")
	ErrNotSupported     = errors.New("cachestore: not supported")
)

type Store[V any] interface {
	// Name returns the name of the store.
	Name() string

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

	// GetOrSetWithLock returns a stored value if it exists. If it doesn't, it acquires
	// a lock and call the getter callback to retrieve a new value. Then it stores that
	// value in the cache and releases the lock.
	GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error)

	// GetOrSetWithLockEx returns a stored value if it exists. If it doesn't, it acquires
	// a lock and call the getter callback to retrieve a new value. Then it stores that
	// value in the cache, sets expiry ttl for the key and releases the lock.
	GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error)
}

// TODO rename to StoreSweeper ..? and SweepExpired() .. and what is this "Every" thing .. weird
// this is used by gcstorage .. check ..
// also, we'll rename gcstore to just gcloudcache
// type StoreCleaner interface {
// 	// CleanExpiredEvery cleans expired keys every d duration.
// 	// If onError is not nil, it will be called when an error occurs.
// 	CleanExpiredEvery(ctx context.Context, d time.Duration, onError func(err error))
// }

type ByteStoreGetter interface {
	ByteStore() Store[[]byte]
}

// type ByteStore interface {
// 	Store[[]byte]
// 	ByteSerializer
// }

// type Serializer[T any] interface {
// 	Serialize(value any) (T, error)
// 	Deserialize(data []byte) (T, error)
// }

// type ByteSerializer Serializer[[]byte]
