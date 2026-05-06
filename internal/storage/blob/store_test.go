package blob

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStorePutGetDelete(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())

	id, err := store.Put(ctx, strings.NewReader("raw message"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	rc, ok, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false")
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "raw message" {
		t.Fatalf("blob content = %q", string(got))
	}

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	_, ok, err = store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if ok {
		t.Fatal("Get() after delete ok = true")
	}
}

func TestFileStorePutUsesContentAddressedShardPath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)

	firstID, err := store.Put(ctx, strings.NewReader("same payload"))
	if err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	secondID, err := store.Put(ctx, strings.NewReader("same payload"))
	if err != nil {
		t.Fatalf("second Put() error = %v", err)
	}
	if firstID != secondID {
		t.Fatalf("ids differ for identical payload: first=%q second=%q", firstID, secondID)
	}

	path := store.pathFor(firstID)
	if filepath.Dir(filepath.Dir(path)) != filepath.Join(root, string(firstID)[:2]) {
		t.Fatalf("pathFor(%q) = %q", firstID, path)
	}
	rc, ok, err := store.Get(ctx, firstID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false")
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "same payload" {
		t.Fatalf("blob content = %q", string(got))
	}
}

func TestFileStoreMissingAndShortIDPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)

	if got, want := store.pathFor(ID("abc")), filepath.Join(root, "abc.blob"); got != want {
		t.Fatalf("short pathFor() = %q, want %q", got, want)
	}
	if _, ok, err := store.Get(ctx, ID("missing")); err != nil || ok {
		t.Fatalf("Get(missing) ok=%v err=%v", ok, err)
	}
	if err := store.Delete(ctx, ID("missing")); err != nil {
		t.Fatalf("Delete(missing) error = %v", err)
	}
}

func TestFileStoreHonorsCanceledContext(t *testing.T) {
	store := NewFileStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.Put(ctx, strings.NewReader("payload")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put() error = %v, want context.Canceled", err)
	}
	if _, _, err := store.Get(ctx, ID("missing")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get() error = %v, want context.Canceled", err)
	}
	if err := store.Delete(ctx, ID("missing")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want context.Canceled", err)
	}
}

func TestFileStorePutReaderErrorRemovesTempFile(t *testing.T) {
	root := t.TempDir()
	store := NewFileStore(root)
	expected := errors.New("reader failed")

	if _, err := store.Put(context.Background(), failingReader{err: expected}); err == nil || !strings.Contains(err.Error(), "write blob") || !errors.Is(err, expected) {
		t.Fatalf("Put() error = %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, "blob-*"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files were not removed: %v", matches)
	}
}

type failingReader struct {
	err error
}

func (r failingReader) Read(p []byte) (int, error) {
	return 0, r.err
}
