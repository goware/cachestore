package cachestore

import (
	"context"
	"errors"
)

const MaxKeyLength = 80

var (
	ErrKeyLengthTooLong = errors.New("cachestore: key length is too long")
	ErrInvalidKey       = errors.New("cachestore: invalid key")
)

type Store interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, key string) error
}
