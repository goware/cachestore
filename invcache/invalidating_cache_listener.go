package invcache

import (
	"context"

	"github.com/goware/pubsub"
)

type CacheInvalidator[V any] struct {
	store      InvalidatingCache[V]
	pubsub     pubsub.PubSub[CacheInvalidationMessage]
	instanceID string
}

func NewCacheInvalidator[V any](store InvalidatingCache[V], ps pubsub.PubSub[CacheInvalidationMessage], instanceID string) *CacheInvalidator[V] {
	return &CacheInvalidator[V]{
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

			ci.store.delete(ctx, msg.Key)
		}
	}
}
