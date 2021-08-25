package memlru

import (
	"context"
	"fmt"
	"sync"

	"github.com/goware/cachestore"
	lru "github.com/hashicorp/golang-lru"
)

var _ cachestore.Storage = &MemStore{}

const defaultLRUSize = 512

type MemStore struct {
	values *lru.Cache
	mu     sync.RWMutex
}

func NewMemStore(lruSize int) (*MemStore, error) {
	if lruSize == 0 {
		lruSize = defaultLRUSize
	}

	values, err := lru.New(lruSize)
	if err != nil {
		return nil, err
	}

	return &MemStore{
		values: values,
	}, nil
}

func (m *MemStore) Set(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	m.values.Add(key, value)
	m.mu.Unlock()
	return nil
}

func (m *MemStore) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	v, ok := m.values.Get(key)
	m.mu.Unlock()

	if !ok {
		// key not found, respond with no data
		return nil, nil
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("memlru#Get: value of key %s is not a []byte", key)
	}

	return b, nil
}

func (m *MemStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	present := m.values.Remove(key)
	m.mu.Unlock()

	// NOTE/TODO: we do not check for presence, prob okay
	_ = present
	return nil
}
