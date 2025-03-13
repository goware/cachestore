package invstore

import (
	"context"
	"strings"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type StoreInvalidator[V any] struct {
	log    logger.Logger
	store  InvalidatingStore[V]
	pubsub pubsub.PubSub[StoreInvalidationMessage]
}

func NewStoreInvalidator[V any](log logger.Logger, store InvalidatingStore[V], ps pubsub.PubSub[StoreInvalidationMessage]) *StoreInvalidator[V] {
	return &StoreInvalidator[V]{
		log:    log,
		store:  store,
		pubsub: ps,
	}
}

func (ci *StoreInvalidator[V]) Listen(ctx context.Context) error {
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
				ci.handleKey(ctx, key, msg.ContentHash)
			}
		}
	}
}

func (ci *StoreInvalidator[V]) handleKey(ctx context.Context, key string, msgHash string) {
	switch {
	case key == "*":
		if err := ci.store.clearAll(ctx); err != nil {
			ci.log.Errorf("failed to delete all store entries: %w", err)
		}

	case strings.HasSuffix(key, "*"):
		prefix := removeSuffix(key, "*")
		if err := ci.store.deletePrefix(ctx, prefix); err != nil {
			ci.log.Errorf("failed to delete store entries with prefix %s: %w", prefix, err)
		}

	default:
		if msgHash == "" {
			if err := ci.store.delete(ctx, key); err != nil {
				ci.log.Errorf("failed to delete store entry for key %s: %v", key, err)
			}
			return
		}

		val, ok, err := ci.store.Get(ctx, key)
		if err != nil {
			ci.log.Errorf("failed to get key %q: %v", key, err)
			return
		}
		if !ok {
			return
		}

		currentHash, err := ComputeHash(val)
		if err != nil {
			ci.log.Errorf("failed to compute hash for key %q: %v", key, err)
			return
		}

		if currentHash == msgHash {
			return
		}

		if err := ci.store.delete(ctx, key); err != nil {
			ci.log.Errorf("failed to delete store entry for key %s: %v", key, err)
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
