package blob

import (
	"context"
	"io"
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
