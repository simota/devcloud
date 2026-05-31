package app

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRedisRustEngineDisabledByDefault(t *testing.T) {
	t.Setenv("DEVCLOUD_REDIS_ENGINE", "")
	t.Setenv("DEVCLOUD_REDIS_RUST_BIN", "")

	if bin, ok := redisRustEngine(); ok || bin != "" {
		t.Fatalf("redisRustEngine() = %q, %v; want disabled", bin, ok)
	}
}

func TestRedisRustEngineUsesDefaultBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_REDIS_ENGINE", "rust")
	t.Setenv("DEVCLOUD_REDIS_RUST_BIN", "")

	bin, ok := redisRustEngine()
	if !ok {
		t.Fatal("redisRustEngine() disabled, want enabled")
	}
	if want := filepath.Join("rust", "target", "debug", "devcloud-redis"); bin != want {
		t.Fatalf("redisRustEngine() bin = %q, want %q", bin, want)
	}
}

func TestRedisRustEngineUsesCustomBinary(t *testing.T) {
	t.Setenv("DEVCLOUD_REDIS_ENGINE", "rust")
	t.Setenv("DEVCLOUD_REDIS_RUST_BIN", "/tmp/devcloud-redis")

	bin, ok := redisRustEngine()
	if !ok {
		t.Fatal("redisRustEngine() disabled, want enabled")
	}
	if bin != "/tmp/devcloud-redis" {
		t.Fatalf("redisRustEngine() bin = %q", bin)
	}
}

func TestRedisRustEnvPassesManagedRedisConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.RedisPort = 6380
	cfg.Storage.Path = ".devcloud"
	cfg.Services.Redis.BinaryPath = "/usr/local/bin/redis-server"
	cfg.Services.Redis.MaxMemoryMB = 128
	cfg.Services.Redis.AppendOnly = true
	cfg.Auth.Redis.Mode = "strict"
	cfg.Auth.Redis.Password = "secret"

	env := strings.Join(redisRustEnv(cfg), "\n")
	for _, want := range []string{
		"DEVCLOUD_REDIS_ADDR=127.0.0.1:6380",
		"DEVCLOUD_REDIS_DATA_DIR=.devcloud/redis",
		"DEVCLOUD_REDIS_BINARY=/usr/local/bin/redis-server",
		"DEVCLOUD_REDIS_MAX_MEMORY_MB=128",
		"DEVCLOUD_REDIS_APPEND_ONLY=true",
		"DEVCLOUD_REDIS_AUTH_MODE=strict",
		"DEVCLOUD_REDIS_PASSWORD=secret",
	} {
		if !strings.Contains(env, want) {
			t.Fatalf("redisRustEnv() missing %q in:\n%s", want, env)
		}
	}
}
