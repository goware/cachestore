package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/goware/cachestore"
	"github.com/pkg/errors"
)

// const LongTime = time.Second * 24 * 60 * 60 * 30 // 1 month in seconds
const LongTime = time.Second * 24 * 60 * 60 // 1 day in seconds

var _ cachestore.Store[any] = &RedisStore[any]{}

type RedisStore[V any] struct {
	pool   *redis.Pool
	keyTTL float64
}

func New[V any](cfg *Config) (cachestore.Store[V], error) {
	if cfg.Host == "" {
		return nil, errors.New("missing \"host\" parameter")
	}
	if cfg.Port < 1 {
		cfg.Port = 6379
	}
	if cfg.KeyTTL == 0 {
		cfg.KeyTTL = LongTime // default setting
	}
	return createWithDialFunc[V](cfg, func() (redis.Conn, error) {
		address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

		c, err := redis.Dial("tcp", address, redis.DialDatabase(cfg.DBIndex))
		return c, errors.Wrapf(err, "unable to dial redis host %v", address) // errors.Wrap(nil, ...) returns nil
	})
}

func NewWithKeyTTL[V any](cfg *Config, keyTTL time.Duration) (cachestore.Store[V], error) {
	cfg.KeyTTL = keyTTL
	return New[V](cfg)
}

func createWithDialFunc[V any](cfg *Config, dial func() (redis.Conn, error)) (*RedisStore[V], error) {
	return &RedisStore[V]{
		pool:   newPool(cfg, dial),
		keyTTL: cfg.KeyTTL.Seconds(),
	}, nil
}

// taken and adapted from https://medium.com/@gilcrest_65433/basic-redis-examples-with-go-a3348a12878e
func newPool(cfg *Config, dial func() (redis.Conn, error)) *redis.Pool {
	var maxIdle, maxActive = cfg.MaxIdle, cfg.MaxActive
	if maxIdle <= 0 {
		maxIdle = 20
	}
	if maxActive <= 0 {
		maxActive = 50
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

func (c *RedisStore[V]) Set(ctx context.Context, key string, value V) error {
	// call SetEx passing default keyTTL
	return c.SetEx(ctx, key, value, time.Duration(c.keyTTL)*time.Second)
}

func (c *RedisStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
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

	data, err := serialize(value)
	if err != nil {
		return fmt.Errorf("unable to serialize object: %w", err)
	}

	_, err = conn.Do("SETEX", key, ttl.Seconds(), data)
	if err != nil {
		return fmt.Errorf("unable to set key %s: %w", key, err)
	}
	return nil
}

func (c *RedisStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return c.BatchSetEx(ctx, keys, values, LongTime)
}

func (c *RedisStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	if len(keys) != len(values) {
		return errors.New("keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("no keys are passed")
	}

	conn := c.pool.Get()
	defer conn.Close()

	// use pipelining to insert all keys. This ensures only one round-trip to
	// the server. We could use MSET but it doesn't support TTL so we'd need to
	// send one EXPIRE command per key anyway
	for i, key := range keys {
		data, err := serialize(values[i])
		if err != nil {
			return fmt.Errorf("unable to serialize object: %w", err)
		}

		err = conn.Send("SETEX", key, ttl.Seconds(), data)
		if err != nil {
			return errors.Wrap(err, "failed writing key")
		}
	}

	// send all commands
	err := conn.Flush()
	if err != nil {
		return errors.Wrap(err, "error encountered when sending commands to server")
	}

	// and wait for the reply
	_, err = conn.Receive()
	return errors.Wrap(err, "error encountered while batch-inserting value")
}

func (c *RedisStore[V]) Get(ctx context.Context, key string) (V, error) {
	conn := c.pool.Get()
	defer conn.Close()

	var out V

	value, err := conn.Do("GET", key)
	if err != nil {
		return out, errors.Wrap(err, "GET command failed")
	}

	if value == nil {
		return out, nil
	}

	data, err := redis.Bytes(value, nil)
	if err != nil {
		return out, err
	}

	out, err = deserialize[V](data)
	if err != nil {
		return out, err
	}

	return out, nil
}

func (c *RedisStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, error) {
	conn := c.pool.Get()
	defer conn.Close()

	// convert []string to []interface{} for redigo below
	ks := make([]interface{}, len(keys))
	for i, k := range keys {
		ks[i] = k
	}

	// execute MGET and convert result to []V
	values, err := redis.ByteSlices(conn.Do("MGET", ks...))
	if err != nil {
		return nil, err
	}

	out := make([]V, len(values))

	for i, value := range values {
		out[i], err = deserialize[V](value)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (c *RedisStore[V]) Exists(ctx context.Context, key string) (bool, error) {
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

func (c *RedisStore[V]) Delete(ctx context.Context, key string) error {
	conn := c.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (c *RedisStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	conn := c.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("SCAN", 0, "MATCH", fmt.Sprintf("%s*", keyPrefix)))
	if err != nil {
		return err
	}

	keys, ok := values[1].([]interface{})
	if !ok {
		return errors.New("SCAN command returned unexpected result")
	}

	cursor := fmt.Sprintf("%s", values[0])
	for cursor != "0" {
		values, err = redis.Values(conn.Do("SCAN", cursor, "MATCH", fmt.Sprintf("%s:*", keyPrefix)))
		if err != nil {
			return err
		}

		keys = append(keys, values[1].([]interface{})...)
		cursor = fmt.Sprintf("%s", values[0])
	}

	// prepare for a transaction
	err = conn.Send("MULTI")
	if err != nil {
		return err
	}

	for _, key := range keys {
		// UNLINK is non blocking, hence not using DEL
		// overall on big queries faster
		err = conn.Send("UNLINK", key)
		if err != nil {
			return err
		}
	}

	_, err = conn.Do("EXEC")

	return err
}

func (c *RedisStore[V]) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()

	return conn.Do(cmd, args...)
}

func (c *RedisStore[V]) Close() error {
	// redigo's pool.Close never returns an error, but perhaps it will in the
	// future so it makes sense to follow their API
	return c.pool.Close()
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
