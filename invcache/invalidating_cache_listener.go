package invcache

import (
	"context"
	"strings"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type CacheInvalidator[V any] struct {
	log    logger.Logger
	store  InvalidatingCache[V]
	pubsub pubsub.PubSub[CacheInvalidationMessage]
}

func NewCacheInvalidator[V any](log logger.Logger, store InvalidatingCache[V], ps pubsub.PubSub[CacheInvalidationMessage]) *CacheInvalidator[V] {
	return &CacheInvalidator[V]{
		log:    log,
		store:  store,
		pubsub: ps,
	}
}

func (ci *CacheInvalidator[V]) Listen(ctx context.Context) error {
	sub, err := ci.pubsub.Subscribe(ctx, DefaultChannelID)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-sub.ReadMessage():
			if !ok {
				return nil
			}

			// Skip if message.Origin == local instance ID
			if msg.Origin == ci.store.GetInstanceID() {
				continue
			}

			if len(msg.Keys) > 1 && hasWildcards(msg.Keys) {
				ci.log.Errorf("ClearAll() and DeletePrefix() do not support batch operations")
				continue
			}

			for _, key := range msg.Keys {
				ci.handleKey(ctx, key)
			}
		}
	}
}

func (ci *CacheInvalidator[V]) handleKey(ctx context.Context, key string) {
	switch {
	case key == "*":
		if err := ci.store.clearAll(ctx); err != nil {
			ci.log.Errorf("failed to delete all cache entries: %w", err)
		}

	case strings.HasSuffix(key, "*"):
		prefix := removeSuffix(key, "*")
		if err := ci.store.deletePrefix(ctx, prefix); err != nil {
			ci.log.Errorf("failed to delete cache entries with prefix %s: %w", prefix, err)
		}

	default:
		if err := ci.store.delete(ctx, key); err != nil {
			ci.log.Errorf("failed to delete cache entry for key %s: %w", key, err)
		}
	}
}

func hasWildcards(keys []string) bool {
	for _, k := range keys {
		if k == "*" || strings.HasSuffix(k, "*") {
			return true
		}
	}
	return false
}

func removeSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s[:len(s)-len(suffix)]
	}
	return s
}
