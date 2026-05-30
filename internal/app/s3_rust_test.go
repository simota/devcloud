package app

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"devcloud/internal/events"
)

func TestS3RustEngineDisabledByDefault(t *testing.T) {
	t.Setenv("DEVCLOUD_S3_ENGINE", "")
	t.Setenv("DEVCLOUD_S3_RUST_BIN", "")

	if bin, ok := s3RustEngine(); ok || bin != "" {
		t.Fatalf("s3RustEngine() = %q, %v; want disabled", bin, ok)
	}
}

func TestS3RustEngineUsesDefaultBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_S3_ENGINE", "rust")
	t.Setenv("DEVCLOUD_S3_RUST_BIN", "")

	bin, ok := s3RustEngine()
	if !ok {
		t.Fatal("s3RustEngine() disabled, want enabled")
	}
	if want := filepath.Join("rust", "target", "debug", "devcloud-s3"); bin != want {
		t.Fatalf("s3RustEngine() bin = %q, want %q", bin, want)
	}
}

func TestS3RustEngineUsesCustomBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_S3_ENGINE", "rust")
	t.Setenv("DEVCLOUD_S3_RUST_BIN", "/tmp/devcloud-s3")

	bin, ok := s3RustEngine()
	if !ok {
		t.Fatal("s3RustEngine() disabled, want enabled")
	}
	if bin != "/tmp/devcloud-s3" {
		t.Fatalf("s3RustEngine() bin = %q", bin)
	}
}

func TestS3RustEnvPassesAuthAndRegion(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.S3Port = 4567
	cfg.Storage.Path = ".devcloud"
	cfg.Auth.S3.Mode = "strict"
	cfg.Auth.S3.AccessKeyID = "access"
	cfg.Auth.S3.SecretAccessKey = "secret"
	cfg.Services.S3.Region = "ap-northeast-1"

	env := strings.Join(s3RustEnv(cfg), "\n")
	for _, want := range []string{
		"DEVCLOUD_S3_ADDR=127.0.0.1:4567",
		"DEVCLOUD_S3_STORAGE=.devcloud/s3/buckets",
		"DEVCLOUD_S3_AUTH_MODE=strict",
		"DEVCLOUD_S3_ACCESS_KEY_ID=access",
		"DEVCLOUD_S3_SECRET_ACCESS_KEY=secret",
		"DEVCLOUD_S3_REGION=ap-northeast-1",
	} {
		if !strings.Contains(env, want) {
			t.Fatalf("s3RustEnv() missing %q in:\n%s", want, env)
		}
	}
}

func TestForwardS3RustOutputPublishesDashboardEvents(t *testing.T) {
	bus := events.NewBus()
	ch, cancel := bus.Subscribe(1, []string{"s3"})
	defer cancel()

	var passthrough bytes.Buffer
	forwardS3RustOutput(strings.NewReader(
		"plain log\n"+
			`DEVCLOUD_EVENT {"type":"s3.object.put","service":"s3","payload":{"bucket":"b","key":"k","etag":"abc","contentLength":3}}`+"\n",
	), bus, &passthrough)

	if got := strings.TrimSpace(passthrough.String()); got != "plain log" {
		t.Fatalf("passthrough = %q, want plain log", got)
	}
	select {
	case event := <-ch:
		if event.Type != "s3.object.put" {
			t.Fatalf("event type = %q", event.Type)
		}
		if event.Payload["bucket"] != "b" || event.Payload["key"] != "k" {
			t.Fatalf("event payload = %#v", event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dashboard event")
	}
}
