package redis

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type mutex struct {
	client *redis.Client
	key    string
	val    string
	rand   rand.Rand

	wasLocked bool

	waitCtx       context.Context
	waitCtxCancel context.CancelFunc

	lockExpiry    time.Duration
	retryTimeout  time.Duration
	minRetryDelay time.Duration
	maxRetryDelay time.Duration
}

func (c *RedisStore[V]) newMutex(ctx context.Context, key string) (*mutex, error) {
	b := make([]byte, 20)
	if _, err := c.random.Read(b); err != nil {
		return nil, err
	}

	m := &mutex{
		client:        c.client,
		key:           "cachestore/mutex:" + key,
		val:           base64.StdEncoding.EncodeToString(b),
		lockExpiry:    c.options.LockExpiry,
		retryTimeout:  c.options.LockRetryTimeout,
		minRetryDelay: c.options.LockMinRetryDelay,
		maxRetryDelay: c.options.LockMaxRetryDelay,
	}

	if m.lockExpiry == 0 {
		m.lockExpiry = 5 * time.Second
	}
	if m.retryTimeout == 0 {
		m.retryTimeout = 10 * time.Second
	}
	if m.maxRetryDelay == 0 {
		m.maxRetryDelay = 200 * time.Millisecond
	}

	m.waitCtx, m.waitCtxCancel = context.WithTimeout(ctx, m.retryTimeout)
	return m, nil
}

func (m *mutex) TryLock(ctx context.Context) (bool, error) {
	acquired, err := m.client.SetNX(ctx, m.key, m.val, m.lockExpiry).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if acquired {
		m.wasLocked = true
	}
	return acquired, nil
}

func (m *mutex) WaitForRetry(ctx context.Context, retryNumber int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.waitCtx.Done():
		return m.waitCtx.Err()
	case <-time.After(m.nextDelay(retryNumber)):
		return nil
	}
}

var extendLockScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (m *mutex) Extend(ctx context.Context) error {
	expired, err := extendLockScript.Run(ctx, m.client, []string{m.key}, m.val, m.lockExpiry.Milliseconds()).Bool()
	if err != nil {
		return err
	}
	if !expired {
		return fmt.Errorf("unable to extend lock")
	}
	return nil
}

func (m *mutex) nextDelay(retryNumber int) time.Duration {
	minDelay, maxDelay := int(m.minRetryDelay)*(retryNumber+1), int(m.maxRetryDelay)
	return time.Duration(rand.Intn(maxDelay-minDelay) + minDelay)
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
