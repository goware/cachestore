package gcstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/goware/cachestore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var ErrNotSupported = fmt.Errorf("not supported")

type cacheObject[T any] struct {
	Object    T         `json:"object"`
	ExpiresAt time.Time `json:"expires_at"`
}

var _ cachestore.Store[any] = &GCStorage[any]{}

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
		return nil, fmt.Errorf("cachestore/redis: invalid backend config supplied")
	}
	for _, opt := range opts {
		opt.Apply(&cfg.StoreOptions)
	}
	return New[V](cfg, cfg.StoreOptions)
}

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
		return nil, err
	}

	return &GCStorage[V]{
		keyPrefix:        cfg.KeyPrefix,
		defaultKeyExpiry: cfg.DefaultKeyExpiry,
		client:           client,
		bucketHandle:     client.Bucket(cfg.Bucket),
	}, nil
}

func (g *GCStorage[V]) Exists(ctx context.Context, key string) (bool, error) {
	attr, err := g.bucketHandle.Object(g.keyPrefix + key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		
		return false, err
	}


	if attr.Metadata["expires_at"] != "" {
		expiresAt, err := time.Parse(time.RFC3339, attr.Metadata["expires_at"])
		if err != nil {
			return false, err
		}
		if !expiresAt.IsZero() && expiresAt.Before(time.Now()) {
			return false, nil
		}
	}
	return true, nil
}

func (g *GCStorage[V]) Set(ctx context.Context, key string, value V) error {
	return g.SetEx(ctx, key, value, g.defaultKeyExpiry)
}

func (g *GCStorage[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
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

	obj := g.bucketHandle.Object(g.keyPrefix + key)
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.Metadata = map[string]string{
		"expires_at": expiresAt.Format(time.RFC3339),
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func (g *GCStorage[V]) Get(ctx context.Context, key string) (V, bool, error) {
	value, _, exists, err := g.GetEx(ctx, key)
	return value, exists, err
}

func (g *GCStorage[V]) GetEx(ctx context.Context, key string) (V, *time.Duration, bool, error) {
	var errReturnObj V

	obj := g.bucketHandle.Object(g.keyPrefix + key)
	r, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return errReturnObj, nil, false, nil
		}

		return errReturnObj, nil, false, err
	}

	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return errReturnObj, nil, false, err
	}

	value, err := deserialize[cacheObject[V]](data)
	if err != nil {
		return errReturnObj, nil, false, err
	}

	if value.ExpiresAt != (time.Time{}) && value.ExpiresAt.Before(time.Now()) {
		return errReturnObj, nil, false, nil
	}

	ttl := value.ExpiresAt.Sub(time.Now())
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
	var out []V
	var exists []bool
	for _, key := range keys {
		value, exist, err := g.Get(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, value)
		exists = append(exists, exist)
	}
	return out, exists, nil
}

func (g *GCStorage[V]) Delete(ctx context.Context, key string) error {
	obj := g.bucketHandle.Object(g.keyPrefix + key)
	return obj.Delete(ctx)
}

func (g *GCStorage[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	objIt := g.bucketHandle.Objects(ctx, &storage.Query{
		Prefix: g.keyPrefix + keyPrefix,
	})

	for {
		objAttrs, err := objIt.Next()
		if err != nil && errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return err
		}

		if err = g.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return err
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
		if err != nil && errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return err
		}

		if err = g.bucketHandle.Object(objAttrs.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (g *GCStorage[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	var out V
	return out, ErrNotSupported
}

func (g *GCStorage[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	var out V
	return out, ErrNotSupported
}

func serialize[V any](value V) ([]byte, error) {
	return json.Marshal(value)
}

func deserialize[V any](data []byte) (V, error) {
	var out V
	err := json.Unmarshal(data, &out)
	return out, err
}
