package redis

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type mutex struct {
	client *redis.Client
	key    string
	val    string

	wasLocked bool

	waitCtx       context.Context
	waitCtxCancel context.CancelFunc
}

func (c *RedisStore[V]) newMutex(ctx context.Context, key string) (*mutex, error) {
	b := make([]byte, 20)
	if _, err := c.random.Read(b); err != nil {
		return nil, err
	}

	m := &mutex{
		client: c.client,
		key:    "cachestore/mutex:" + key,
		val:    base64.StdEncoding.EncodeToString(b),
	}
	// TODO: timeout from cfg
	m.waitCtx, m.waitCtxCancel = context.WithTimeout(ctx, 10*time.Second)
	return m, nil
}

func (m *mutex) TryLock(ctx context.Context) (bool, error) {
	// TODO: expiry from cfg
	acquired, err := m.client.SetNX(ctx, m.key, m.val, 10*time.Second).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	m.wasLocked = true
	return acquired, nil
}

func (m *mutex) WaitForRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.waitCtx.Done():
		return m.waitCtx.Err()
	case <-time.After(100 * time.Millisecond): // TODO: delay from cfg
		return nil
	}
}

var releaseLockScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
    	return redis.call("del", KEYS[1])
	else
    	return 0
	end
`)

func (m *mutex) Unlock() {
	m.waitCtxCancel()
	if m.wasLocked {
		releaseLockScript.Run(context.Background(), m.client, []string{m.key}, m.val).Result()
	}
}
