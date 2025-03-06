package invcache

import (
	"context"

	"github.com/goware/logger"
	"github.com/goware/pubsub"
)

type CacheInvalidator[V any] struct {
	log        logger.Logger
	store      InvalidatingCache[V]
	pubsub     pubsub.PubSub[CacheInvalidationMessage]
	instanceID string
}

func NewCacheInvalidator[V any](log logger.Logger, store InvalidatingCache[V], ps pubsub.PubSub[CacheInvalidationMessage], instanceID string) *CacheInvalidator[V] {
	return &CacheInvalidator[V]{
		log:        log,
		store:      store,
		pubsub:     ps,
		instanceID: instanceID,
	}
}

func (ci *CacheInvalidator[V]) Listen(ctx context.Context) error {
	sub, err := ci.pubsub.Subscribe(ctx, ChannelID)
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
			if msg.Origin == ci.instanceID {
				continue
			}

			if err := ci.store.delete(ctx, msg.Key); err != nil {
				ci.log.Errorf("failed to delete cache entry for key %s: %w", msg.Key, err)
			}
		}
	}
}
