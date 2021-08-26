package memlru

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/goware/cachestore"
	lru "github.com/hashicorp/golang-lru"
)

var _ cachestore.Store = &MemLRU{}

const defaultLRUSize = 512

type MemLRU struct {
	backend *lru.Cache
	mu      sync.RWMutex
}

func New() (cachestore.Store, error) {
	return NewWithSize(defaultLRUSize)
}

func NewWithSize(size int) (cachestore.Store, error) {
	if size < 1 {
		return nil, errors.New("size must be greater or equal to 1")
	}

	backend, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	return &MemLRU{backend: backend}, nil
}

func (m *MemLRU) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := m.backend.Peek(key)
	return exists, nil
}

func (m *MemLRU) Set(ctx context.Context, key string, value []byte) error {
	if len(key) > cachestore.MaxKeyLength {
		return cachestore.ErrKeyLengthTooLong
	}
	if len(key) == 0 {
		return cachestore.ErrInvalidKey
	}
	m.mu.Lock()
	m.backend.Add(key, value)
	m.mu.Unlock()
	return nil
}

func (m *MemLRU) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	v, ok := m.backend.Get(key)
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

func (m *MemLRU) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	present := m.backend.Remove(key)
	m.mu.Unlock()

	// NOTE/TODO: we do not check for presence, prob okay
	_ = present
	return nil
}
