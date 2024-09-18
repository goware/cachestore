package redis

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/goware/cachestore"
	"github.com/redis/go-redis/v9"
)

const DefaultTTL = time.Second * 24 * 60 * 60 // 1 day in seconds

var _ cachestore.Store[any] = &RedisStore[any]{}

type RedisStore[V any] struct {
	options cachestore.StoreOptions
	client  *redis.Client
	random  io.Reader
}

func Backend(cfg *Config, opts ...cachestore.StoreOptions) cachestore.Backend {
	// TODO: perhaps in the future, we actually create a Backend struct type,
	// where we put the redis connection, this way we can reuse the same connection pool
	// instead of making new ones at the time of connect. This would move the dial function
	// from inside of NewWithBackend to this method here. We would also need to either
	// add the `pool *redis.Pool` on the *Config object above, or we can make a new one
	// called like RedisBackend struct { StoreOptions, Config, pool }

	options := cachestore.ApplyOptions(opts...)
	cfg.StoreOptions = options
	return cfg
}

func NewWithBackend[V any](backend cachestore.Backend, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	cfg, ok := backend.(*Config)
	if !ok {
		return nil, fmt.Errorf("cachestore/redis: invalid backend config supplied")
	}
	for _, opt := range opts {
		opt.Apply(&cfg.StoreOptions)
	}
	return New[V](cfg, cfg.StoreOptions)
}

func New[V any](cfg *Config, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	if !cfg.Enabled {
		return nil, errors.New("cachestore/redis: attempting to create store while config.Enabled is false")
	}

	if cfg.Host == "" {
		return nil, errors.New("cachestore/redis: missing \"host\" parameter")
	}
	if cfg.Port < 1 {
		cfg.Port = 6379
	}
	if cfg.KeyTTL == 0 {
		cfg.KeyTTL = DefaultTTL // default setting
	}

	// Create store and connect to backend
	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	store := &RedisStore[V]{
		options: cachestore.ApplyOptions(opts...),
		client:  newRedisClient(cfg, address),
		random:  rand.Reader,
	}

	err := store.client.Ping(context.Background()).Err()
	if err != nil {
		return nil, fmt.Errorf("cachestore/redis: unable to dial redis host %v: %w", address, err)
	}

	// Apply store options, where value set by options will take precedence over the default
	store.options = cachestore.ApplyOptions(opts...)
	if store.options.DefaultKeyExpiry == 0 && cfg.KeyTTL > 0 {
		store.options.DefaultKeyExpiry = cfg.KeyTTL
	}

	// Set default key expiry for a long time on redis always. This is how we ensure
	// the cache will always function as a LRU.
	if store.options.DefaultKeyExpiry == 0 {
		store.options.DefaultKeyExpiry = DefaultTTL
	}

	return store, nil
}

func newRedisClient(cfg *Config, address string) *redis.Client {
	var maxIdle, maxActive = cfg.MaxIdle, cfg.MaxActive
	if maxIdle <= 0 {
		maxIdle = 20
	}
	if maxActive <= 0 {
		maxActive = 50
	}

	return redis.NewClient(&redis.Options{
		Addr:         address,
		DB:           cfg.DBIndex,
		MinIdleConns: maxIdle,
		PoolSize:     maxActive,
	})
}

func (c *RedisStore[V]) Set(ctx context.Context, key string, value V) error {
	// call SetEx passing default keyTTL
	return c.SetEx(ctx, key, value, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if len(key) > cachestore.MaxKeyLength {
		return cachestore.ErrKeyLengthTooLong
	}
	if len(key) == 0 {
		return cachestore.ErrInvalidKey
	}

	// TODO: handle timeout here, and return error if we hit it via ctx

	data, err := serialize(value)
	if err != nil {
		return fmt.Errorf("unable to serialize object: %w", err)
	}

	if ttl > 0 {
		_, err = c.client.SetEx(ctx, key, data, ttl).Result()
	} else {
		_, err = c.client.Set(ctx, key, data, 0).Result()
	}
	if err != nil {
		return fmt.Errorf("unable to set key %s: %w", key, err)
	}
	return nil
}

func (c *RedisStore[V]) GetEx(ctx context.Context, key string) (V, time.Duration, bool, error) {
	var out V
	var ttl time.Duration

	_, err := c.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		getVal := pipe.Get(ctx, key)
		getTTL := pipe.TTL(ctx, key)

		_, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("exec: %w", err)
		}

		if errors.Is(err, redis.Nil) {
			return nil
		}

		ttl, err = getTTL.Result()
		if err != nil {
			return fmt.Errorf("TTL command failed: %w", err)
		}

		if ttl == -1 {
			return fmt.Errorf("key %s does not have ttl set", key)
		}

		data, err := getVal.Bytes()
		if data == nil {
			return fmt.Errorf("get bytes: %w", err)
		}

		out, err = deserialize[V](data)
		if err != nil {
			return fmt.Errorf("deserialize: %w", err)
		}

		return nil
	})

	if err != nil {
		return out, ttl, false, fmt.Errorf("GetEx: %w", err)
	}

	if ttl == 0 {
		return out, ttl, false, nil
	}

	return out, ttl, true, nil
}

func (c *RedisStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return c.BatchSetEx(ctx, keys, values, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	pipeline := c.client.Pipeline()

	// use pipelining to insert all keys. This ensures only one round-trip to
	// the server. We could use MSET but it doesn't support TTL so we'd need to
	// send one EXPIRE command per key anyway
	for i, key := range keys {
		data, err := serialize(values[i])
		if err != nil {
			return fmt.Errorf("unable to serialize object: %w", err)
		}

		if ttl > 0 {
			err = pipeline.SetEx(ctx, key, data, ttl).Err()
		} else {
			err = pipeline.Set(ctx, key, data, 0).Err()
		}
		if err != nil {
			return fmt.Errorf("failed writing key: %w", err)
		}
	}

	// send all commands
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error encountered while batch-inserting value: %w", err)
	}

	return nil
}

func (c *RedisStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V

	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil && err != redis.Nil {
		return out, false, fmt.Errorf("GET command failed: %w", err)
	}
	if data == nil {
		return out, false, nil
	}

	out, err = deserialize[V](data)
	if err != nil {
		return out, false, err
	}

	return out, true, nil
}

func (c *RedisStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	// execute MGET and convert result to []V
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}

	// we should always return the same number of values as keys requested,
	// in the same order
	if len(values) != len(keys) {
		return nil, nil, fmt.Errorf("cachestore/redis: failed assertion")
	}
	out := make([]V, len(values))
	oks := make([]bool, len(values))

	for i, value := range values {
		if value == nil {
			continue
		}
		v, _ := value.(string)

		out[i], err = deserialize[V]([]byte(v))
		if err != nil {
			return nil, nil, err
		}
		oks[i] = true
	}

	return out, oks, nil
}

func (c *RedisStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	value, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("EXISTS command failed: %w", err)
	}

	if value == 1 {
		return true, nil
	}
	return false, nil
}

func (c *RedisStore[V]) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func (c *RedisStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if len(keyPrefix) < 4 {
		return fmt.Errorf("cachestore/redis: DeletePrefix keyPrefix '%s' must be at least 4 characters long", keyPrefix)
	}

	keys := make([]string, 0, 1000)

	var results []string
	var cursor uint64 = 0
	var limit int64 = 2000
	var err error
	start := true

	for start || cursor != 0 {
		start = false
		results, cursor, err = c.client.Scan(ctx, cursor, fmt.Sprintf("%s*", keyPrefix), limit).Result()
		if err != nil {
			return fmt.Errorf("cachestore/redis: SCAN command returned unexpected result: %w", err)
		}
		keys = append(keys, results...)
	}

	if len(keys) == 0 {
		return nil
	}

	err = c.client.Unlink(ctx, keys...).Err()
	if err != nil {
		return fmt.Errorf("cachestore/redis: DeletePrefix UNLINK failed: %w", err)
	}

	return nil
}

func (c *RedisStore[V]) ClearAll(ctx context.Context) error {
	// With redis, we do not support ClearAll as its too destructive. For testing
	// use the memlru if you want to Clear All.
	return fmt.Errorf("cachestore/redis: unsupported")
}

func (c *RedisStore[V]) GetOrSetWithLock(
	ctx context.Context, key string, getter func(context.Context, string) (V, error),
) (V, error) {
	return c.GetOrSetWithLockEx(ctx, key, getter, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) GetOrSetWithLockEx(
	ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration,
) (V, error) {
	var out V

	mu, err := c.newMutex(ctx, key)
	if err != nil {
		return out, fmt.Errorf("cachestore/redis: creating mutex failed: %w", err)
	}
	defer mu.Unlock()

	for i := 0; ; i++ {
		// If there's a value in the cache, return it immediately
		out, found, err := c.Get(ctx, key)
		if err != nil {
			return out, err
		}
		if found {
			return out, nil
		}

		// Otherwise attempt to acquire a lock for writing
		acquired, err := mu.TryLock(ctx)
		if err != nil {
			return out, fmt.Errorf("cachestore/redis: try lock failed: %w", err)
		}
		if acquired {
			break
		}

		if err := mu.WaitForRetry(ctx, i); err != nil {
			return out, fmt.Errorf("cachestore/redis: timed out waiting for lock: %w", err)
		}
	}

	// We extend the lock in a goroutine for as long as GetOrSetWithLockEx runs.
	// If we're unable to extend the lock, the cancellation is propagated to the getter,
	// that is then expected to terminate.
	ctx, cancel := context.WithCancel(ctx) // TODO/NOTE: use WithCancelCause in the future to signal underlying err
	extendCtx, cancelExtending := context.WithCancel(ctx)
	defer cancelExtending()
	go func() {
		for {
			select {
			case <-extendCtx.Done():
				return
			case <-time.After(mu.lockExpiry / 2):
				if err := mu.Extend(extendCtx); err != nil {
					cancel()
				}
			}
		}
	}()

	// Retrieve a new value from the origin
	out, err = getter(ctx, key)
	if err != nil {
		return out, fmt.Errorf("getter function failed: %w", err)
	}

	// Store the retrieved value in the cache
	if err := c.SetEx(ctx, key, out, ttl); err != nil {
		return out, err
	}

	return out, nil
}

func (c *RedisStore[V]) RedisClient() *redis.Client {
	return c.client
}

func serialize[V any](value V) ([]byte, error) {
	// return the value directly if the type is a []byte or string,
	// otherwise assume its json and unmarshal it
	switch v := any(value).(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return json.Marshal(value)
	}
}

func deserialize[V any](data []byte) (V, error) {
	var out V

	switch any(out).(type) {
	case string:
		outv := reflect.ValueOf(&out).Elem()
		outv.SetString(string(data))
		return out, nil
	case []byte:
		outv := reflect.ValueOf(&out).Elem()
		outv.SetBytes(data)
		return out, nil
	default:
		err := json.Unmarshal(data, &out)
		return out, err
	}
}
