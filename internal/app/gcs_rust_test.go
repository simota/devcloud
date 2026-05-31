package app

import (
	"path/filepath"
	"testing"
)

func TestGCSRustEngineDisabledByDefault(t *testing.T) {
	t.Setenv("DEVCLOUD_GCS_ENGINE", "")
	t.Setenv("DEVCLOUD_GCS_RUST_BIN", "")

	if bin, ok := gcsRustEngine(); ok || bin != "" {
		t.Fatalf("gcsRustEngine() = %q, %v; want disabled", bin, ok)
	}
}

func TestGCSRustEngineUsesDefaultBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_GCS_ENGINE", "rust")
	t.Setenv("DEVCLOUD_GCS_RUST_BIN", "")

	bin, ok := gcsRustEngine()
	if !ok {
		t.Fatal("gcsRustEngine() disabled, want enabled")
	}
	if want := filepath.Join("rust", "target", "debug", "devcloud-gcs"); bin != want {
		t.Fatalf("gcsRustEngine() bin = %q, want %q", bin, want)
	}
}

func TestGCSRustEngineUsesCustomBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_GCS_ENGINE", "rust")
	t.Setenv("DEVCLOUD_GCS_RUST_BIN", "/tmp/devcloud-gcs")

	bin, ok := gcsRustEngine()
	if !ok {
		t.Fatal("gcsRustEngine() disabled, want enabled")
	}
	if bin != "/tmp/devcloud-gcs" {
		t.Fatalf("gcsRustEngine() bin = %q", bin)
	}
}
