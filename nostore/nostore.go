package nostore

import (
	"context"

	"github.com/goware/cachestore"
)

var _ cachestore.Store = &NoStore{}

type NoStore struct{}

func New() (cachestore.Store, error) {
	return &NoStore{}, nil
}

func (s *NoStore) Set(ctx context.Context, key string, value []byte) error {
	return nil
}

func (s *NoStore) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (s *NoStore) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (s *NoStore) Delete(ctx context.Context, key string) error {
	return nil
}
