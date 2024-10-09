package gcstorage

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/goware/cachestore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const DefaultTTL = time.Second * 24 * 60 * 60 // 1 day in seconds

const keySuffix = ".cachestore"

type cacheObject[T any] struct {
	Object    T         `json:"object"`
	ExpiresAt time.Time `json:"expires_at"`
}

var _ cachestore.Store[any] = &GCStorage[any]{}
var _ cachestore.StoreCleaner = &GCStorage[any]{}

type GCStorage[V any] struct {
	keyPrefix string

	defaultKeyExpiry time.Duration

	client       *storage.Client
	bucketHandle *storage.BucketHandle
}

func Backend(cfg *Config, opts ...cachestore.StoreOptions) cachestore.Backend {
	options := cachestore.ApplyOptions(opts...)
	cfg.StoreOptions = options
	return cfg
}

func NewWithBackend[V any](backend cachestore.Backend, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	cfg, ok := backend.(*Config)
	if !ok {
		return nil, fmt.Errorf("cachestore/gcstorage: invalid backend config supplied")
	}
	for _, opt := range opts {
		opt.Apply(&cfg.StoreOptions)
	}
	return New[V](cfg, cfg.StoreOptions)
}

// New creates a new GCStorage instance.
//
// The GCStorage instance is a cachestore.Store implementation that uses Google Cloud Storage as the backend. The object
// is serialized to JSON and stored in the bucket. The key is prefixed with the keyPrefix and the object is stored in the
// bucket with the keyPrefix + key + `.cachestore`. The object is stored with a custom time of the expiry time.
//
// Please note that: The Google Cloud Storage bucket must have proper Object Lifecycle Management rules to delete
// the objects after expiry automatically. In case the bucket does not have Object Lifecycle Management rules, the
// objects will not be deleted. In that case the objects will be deleted only if CleanExpiredEvery is working in the
// background.
//
// Required Lifecycle Management Rule for automatic deletion:
// 1. Delete object	0+ days since object's custom time
// 2. Name matches suffix '.cachestore'
func New[V any](cfg *Config, opts ...cachestore.StoreOptions) (cachestore.Store[V], error) {
	for _, opt := range opts {
		opt.Apply(&cfg.StoreOptions)
	}

	var gcOpts []option.ClientOption
	if cfg.Credentials != nil {
		gcOpts = append(gcOpts, option.WithCredentials(cfg.Credentials))
	}

	client, err := storage.NewClient(context.Background(), gcOpts...)
	if err != nil {
		return nil, fmt.Errorf("cachestore/gcstorage: new client returned error: %w", err)
	}

	return &GCStorage[V]{
		keyPrefix:        cfg.KeyPrefix,
		defaultKeyExpiry: cmp.Or(cfg.DefaultKeyExpiry, DefaultTTL),
		client:           client,
		bucketHandle:     client.Bucket(cfg.Bucket),
	}, nil
}

func (g *GCStorage[V]) Exists(ctx context.Context, key string) (bool, error) {
	attr, err := g.bucketHandle.Object(g.objectKey(key)).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("cachestore/gcstorage: get attrs returned error: %w", err)
	}

	if !attr.CustomTime.IsZero() && attr.CustomTime.Before(time.Now()) {
		return false, nil
	}
	return true, nil
}

func (g *GCStorage[V]) Set(ctx context.Context, key string, value V) error {
	return g.SetEx(ctx, key, value, g.defaultKeyExpiry)
}

func (g *GCStorage[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if len(key) > cachestore.MaxKeyLength {
		return fmt.Errorf("cachestore/gcstorage: %w", cachestore.ErrKeyLengthTooLong)
	}
	if len(key) == 0 {
		return fmt.Errorf("cachestore/gcstorage: %w", cachestore.ErrInvalidKey)
	}

	var expiresAt time.Time
	if ttl != 0 {
		expiresAt = time.Now().Add(ttl)
	}

	data, err := serialize(cacheObject[V]{
		Object:    value,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return err
	}

	obj := g.bucketHandle.Object(g.objectKey(key))
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.CustomTime = expiresAt

	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("cachestore/gcstorage: write returned error: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("cachestore/gcstorage: close returned error: %w", err)
	}
	return nil
}

func (g *GCStorage[V]) Get(ctx context.Context, key string) (V, bool, error) {
	value, _, exists, err := g.GetEx(ctx, key)
	return value, exists, err
}

func (g *GCStorage[V]) GetEx(ctx context.Context, key string) (V, *time.Duration, bool, error) {
	obj := g.bucketHandle.Object(g.objectKey(key))
	r, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return *new(V), nil, false, nil
		}
		return *new(V), nil, false, fmt.Errorf("cachestore/gcstorage: new reader error: %w", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return *new(V), nil, false, fmt.Errorf("cachestore/gcstorage: read all error: %w", err)
	}

	value, err := deserialize[cacheObject[V]](data)
	if err != nil {
		return *new(V), nil, false, fmt.Errorf("cachestore/gcstorage: deserialize error: %w", err)
	}

	if !value.ExpiresAt.IsZero() && value.ExpiresAt.Before(time.Now()) {
		return *new(V), nil, false, nil
	}

	ttl := time.Until(value.ExpiresAt)
	return value.Object, &ttl, true, nil
}

func (g *GCStorage[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return g.BatchSetEx(ctx, keys, values, g.defaultKeyExpiry)
}

func (g *GCStorage[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	for i, key := range keys {
		if err := g.SetEx(ctx, key, values[i], ttl); err != nil {
			return err
		}
	}
	return nil
}

func (g *GCStorage[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	out := make([]V, len(keys))
	exists := make([]bool, len(keys))
	for i, key := range keys {
		value, exist, err := g.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}

		out[i] = value
		exists[i] = exist
	}

	return out, exists, nil
}

func (g *GCStorage[V]) Delete(ctx context.Context, key string) error {
	return g.bucketHandle.Object(g.objectKey(key)).Delete(ctx)
}

func (g *GCStorage[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if len(keyPrefix) > cachestore.MaxKeyLength {
		return fmt.Errorf("cachestore/gcstorage: %w", cachestore.ErrInvalidKeyPrefix)
	}
	if len(keyPrefix) < 4 {
		return fmt.Errorf("cachestore/gcstorage: %w", cachestore.ErrInvalidKeyPrefix)
	}

	objIt := g.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: g.keyPrefix + keyPrefix,
	})

	for {
		objAttrs, err := objIt.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return fmt.Errorf("cachestore/gcstorage: it next error: %w", err)
		}

		if !strings.HasSuffix(objAttrs.Name, keySuffix) {
			continue
		}

		if err = g.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("cachestore/gcstorage: object delete error: %w", err)
		}
	}

	return nil
}

func (g *GCStorage[V]) ClearAll(ctx context.Context) error {
	objIt := g.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: g.keyPrefix,
	})

	for {
		objAttrs, err := objIt.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return fmt.Errorf("cachestore/gcstorage: it next error: %w", err)
		}

		if !strings.HasSuffix(objAttrs.Name, keySuffix) {
			continue
		}

		if err = g.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("cachestore/gcstorage: object delete error: %w", err)
		}
	}
	return nil
}

func (g *GCStorage[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return *new(V), cachestore.ErrNotSupported
}

func (g *GCStorage[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	return *new(V), cachestore.ErrNotSupported
}

func (g *GCStorage[V]) CleanExpiredEvery(ctx context.Context, d time.Duration, onError func(err error)) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			objIt := g.bucketHandle.Objects(ctx, &storage.Query{
				Prefix: g.keyPrefix,
			})

			for {
				objAttrs, err := objIt.Next()
				if err != nil {
					if errors.Is(err, iterator.Done) {
						break
					}
					return
				}

				if !strings.HasSuffix(objAttrs.Name, keySuffix) {
					continue
				}

				if objAttrs.CustomTime.IsZero() {
					continue
				}

				if objAttrs.CustomTime.Before(time.Now()) {
					if err = g.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
						if onError != nil {
							onError(fmt.Errorf("cachestore/gcstorage: delete error: %w", err))
						}
						continue
					}
				}
			}
		}
	}
}

func (g *GCStorage[V]) objectKey(key string) string {
	return fmt.Sprintf("%s%s%s", g.keyPrefix, key, keySuffix)
}

func serialize[V any](value V) ([]byte, error) {
	return json.Marshal(value)
}

func deserialize[V any](data []byte) (V, error) {
	var out V
	if err := json.Unmarshal(data, &out); err != nil {
		return *new(V), err
	}

	return out, nil

}
