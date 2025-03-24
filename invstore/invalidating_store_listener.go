package invstore

import (
	"context"
	"strings"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type StoreInvalidator struct {
	log    logger.Logger
	store  LocalInvStore
	pubsub pubsub.PubSub[StoreInvalidationMessage]
}

func NewStoreInvalidator(log logger.Logger, store LocalInvStore, ps pubsub.PubSub[StoreInvalidationMessage]) *StoreInvalidator {
	return &StoreInvalidator{
		log:    log,
		store:  store,
		pubsub: ps,
	}
}

func (ci *StoreInvalidator) Listen(ctx context.Context) error {
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

			if len(msg.Entries) > 1 && hasWildcards(msg.Entries) {
				ci.log.Errorf("ClearAll() and DeletePrefix() do not support batch operations")
				continue
			}

			for _, entry := range msg.Entries {
				ci.handleKey(ctx, entry)
			}
		}
	}
}

func (ci *StoreInvalidator) handleKey(ctx context.Context, entry CacheInvalidationEntry) {
	switch {
	case entry.Key == "*":
		if err := ci.store.ClearAllLocal(ctx); err != nil {
			ci.log.Errorf("failed to delete all store entries: %w", err)
		}

	case strings.HasSuffix(entry.Key, "*"):
		prefix := removeSuffix(entry.Key, "*")
		if err := ci.store.DeletePrefixLocal(ctx, prefix); err != nil {
			ci.log.Errorf("failed to delete store entries with prefix %s: %w", prefix, err)
		}

	default:
		if entry.ContentHash == "" {
			if err := ci.store.DeleteLocal(ctx, entry.Key); err != nil {
				ci.log.Errorf("failed to delete store entry for key %s: %v", entry.Key, err)
			}
			return
		}

		val, ok, err := ci.store.GetAny(ctx, entry.Key)
		if err != nil {
			ci.log.Errorf("failed to get key %q: %v", entry.Key, err)
			return
		}
		if !ok {
			return
		}

		currentHash, err := ComputeHash(val)
		if err != nil {
			ci.log.Errorf("failed to compute hash for key %q: %v", entry.Key, err)
			return
		}

		if currentHash == entry.ContentHash {
			return
		}

		if err := ci.store.DeleteLocal(ctx, entry.Key); err != nil {
			ci.log.Errorf("failed to delete store entry for key %s: %v", entry.Key, err)
		}
	}
}

func hasWildcards(entries []CacheInvalidationEntry) bool {
	for _, e := range entries {
		if e.Key == "*" || strings.HasSuffix(e.Key, "*") {
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
