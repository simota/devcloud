package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ID string

type Store interface {
	Put(ctx context.Context, raw io.Reader) (ID, error)
	Get(ctx context.Context, id ID) (io.ReadCloser, bool, error)
	Delete(ctx context.Context, id ID) error
}

type FileStore struct {
	root string
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func (s *FileStore) Put(ctx context.Context, raw io.Reader) (ID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return "", fmt.Errorf("create blob root: %w", err)
	}

	tmp, err := os.CreateTemp(s.root, "blob-*")
	if err != nil {
		return "", fmt.Errorf("create blob temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hash), raw); err != nil {
		tmp.Close()
		return "", fmt.Errorf("write blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close blob temp file: %w", err)
	}

	id := ID(hex.EncodeToString(hash.Sum(nil)))
	path := s.pathFor(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create blob shard: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return "", fmt.Errorf("commit blob: %w", err)
	}
	return id, nil
}

func (s *FileStore) Get(ctx context.Context, id ID) (io.ReadCloser, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	f, err := os.Open(s.pathFor(id))
	if err == nil {
		return f, true, nil
	}
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("open blob: %w", err)
}

func (s *FileStore) Delete(ctx context.Context, id ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(s.pathFor(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete blob: %w", err)
	}
	return nil
}

func (s *FileStore) pathFor(id ID) string {
	value := string(id)
	if len(value) < 4 {
		return filepath.Join(s.root, value+".blob")
	}
	return filepath.Join(s.root, value[:2], value[2:4], value+".blob")
}
