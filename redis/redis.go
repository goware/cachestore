package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/cachestore"
	"github.com/pkg/errors"
)

// const LongTime = time.Second * 24 * 60 * 60 * 30 // 1 month in seconds
const LongTime = time.Second * 24 * 60 * 60 // 1 day in seconds

var _ cachestore.Store = &RedisStore{}

type RedisStore struct {
	pool   *redis.Pool
	keyTTL time.Duration
}

func New(cfg *Config) (cachestore.Store, error) {
	if cfg.Host == "" {
		return nil, errors.New("missing \"host\" parameter")
	}
	if cfg.Port < 1 {
		cfg.Port = 6379
	}
	if cfg.KeyTTL == 0 {
		cfg.KeyTTL = LongTime // default setting
	}
	return createWithDialFunc(cfg, func() (redis.Conn, error) {
		address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

		c, err := redis.Dial("tcp", address)
		return c, errors.Wrapf(err, "unable to dial redis host %v", address) // errors.Wrap(nil, ...) returns nil
	})
}

func NewWithKeyTTL(cfg *Config, keyTTL time.Duration) (cachestore.Store, error) {
	cfg.KeyTTL = keyTTL
	return New(cfg)
}

func createWithDialFunc(cfg *Config, dial func() (redis.Conn, error)) (*RedisStore, error) {
	return &RedisStore{
		pool: newPool(cfg, dial),
	}, nil
}

// taken and adapted from https://medium.com/@gilcrest_65433/basic-redis-examples-with-go-a3348a12878e
func newPool(cfg *Config, dial func() (redis.Conn, error)) *redis.Pool {
	var maxIdle, maxActive = cfg.MaxIdle, cfg.MaxActive
	if maxIdle <= 0 {
		maxIdle = 4
	}
	if maxActive <= 0 {
		maxActive = 8
	}

	return &redis.Pool{
		// Maximum number of idle connections in the pool.
		MaxIdle: maxIdle,
		// max number of connections
		MaxActive: maxActive,

		Dial: dial,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return errors.Wrap(err, "PING failed")
		},
	}
}

func (c *RedisStore) Set(ctx context.Context, key string, value []byte) error {
	if len(key) > cachestore.MaxKeyLength {
		return cachestore.ErrKeyLengthTooLong
	}
	if len(key) == 0 {
		return cachestore.ErrInvalidKey
	}

	conn := c.pool.Get()
	// the redigo docs clearly states that connections read from the pool needs
	// to be closed by the application, but it feels a little odd closing
	// connections in pools
	defer conn.Close()

	// TODO: handle timeout here, and return error if we hit it via ctx

	_, err := conn.Do("SETEX", key, LongTime.Seconds(), value)
	return errors.Wrapf(err, "failed setting key %s", key)
}

func (c *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	conn := c.pool.Get()
	defer conn.Close()

	value, err := conn.Do("GET", key)
	if err != nil {
		return nil, errors.Wrap(err, "GET command failed")
	}

	if value == nil {
		return nil, nil
	}
	return redis.Bytes(value, nil)
}

func (c *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()

	value, err := redis.Bytes(conn.Do("EXISTS", key))
	if err != nil {
		return false, errors.Wrap(err, "EXISTS command failed")
	}

	if len(value) > 0 && value[0] == '1' {
		return true, nil
	}
	return false, nil
}

func (c *RedisStore) Delete(ctx context.Context, key string) error {
	conn := c.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (c *RedisStore) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()

	return conn.Do(cmd, args...)
}

func (c *RedisStore) Close() error {
	// redigo's pool.Close never returns an error, but perhaps it will in the
	// future so it makes sense to follow their API
	return c.pool.Close()
}
