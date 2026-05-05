package memory

import (
	"context"
	"errors"
	"sync"

	"devcloud/internal/services/redshift/backend"
)

type ExecFunc func(ctx context.Context, statement string) (backend.Result, error)
type CatalogFunc func(ctx context.Context) (backend.CatalogSnapshot, error)

type Backend struct {
	mu      sync.Mutex
	closed  bool
	exec    ExecFunc
	catalog CatalogFunc
}

func New(exec ExecFunc, catalog CatalogFunc) *Backend {
	return &Backend{exec: exec, catalog: catalog}
}

func (b *Backend) Exec(ctx context.Context, statement string) (backend.Result, error) {
	if err := b.ready(); err != nil {
		return backend.Result{}, err
	}
	if b.exec == nil {
		return backend.Result{}, errors.New("memory redshift backend has no executor")
	}
	return b.exec(ctx, statement)
}

func (b *Backend) Begin(ctx context.Context) (backend.Transaction, error) {
	if err := b.ready(); err != nil {
		return nil, err
	}
	return &transaction{backend: b}, nil
}

func (b *Backend) Catalog(ctx context.Context) (backend.CatalogSnapshot, error) {
	if err := b.ready(); err != nil {
		return backend.CatalogSnapshot{}, err
	}
	if b.catalog == nil {
		return backend.CatalogSnapshot{}, nil
	}
	return b.catalog(ctx)
}

func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *Backend) ready() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("memory redshift backend is closed")
	}
	return nil
}

type transaction struct {
	backend *Backend
	closed  bool
}

func (tx *transaction) Exec(ctx context.Context, statement string) (backend.Result, error) {
	if tx.closed {
		return backend.Result{}, errors.New("memory redshift transaction is closed")
	}
	return tx.backend.Exec(ctx, statement)
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.closed {
		return errors.New("memory redshift transaction is closed")
	}
	tx.closed = true
	return nil
}

func (tx *transaction) Rollback(ctx context.Context) error {
	if tx.closed {
		return nil
	}
	tx.closed = true
	return nil
}
