package app

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoverManagedRedisCommandReturnsActionableError(t *testing.T) {
	originalLookPath := managedRedisLookPath
	managedRedisLookPath = func(name string) (string, error) {
		return "", exec.ErrNotFound
	}
	t.Cleanup(func() {
		managedRedisLookPath = originalLookPath
	})

	_, err := discoverManagedRedisCommand("")
	if err == nil {
		t.Fatal("discoverManagedRedisCommand() error = nil")
	}
	for _, want := range []string{"redis-server", "PATH", "brew install redis", "apt install redis-server", "mode=external", "externalUrl"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("discoverManagedRedisCommand() error missing %q: %v", want, err)
		}
	}
}

func TestStartManagedRedisValidatesConfigBeforeCommandDiscovery(t *testing.T) {
	originalLookPath := managedRedisLookPath
	lookPathCalled := false
	managedRedisLookPath = func(name string) (string, error) {
		lookPathCalled = true
		return "", exec.ErrNotFound
	}
	t.Cleanup(func() {
		managedRedisLookPath = originalLookPath
	})

	_, err := startManagedRedisProcess(context.Background(), managedRedisConfig{
		DataDir: filepath.Join(t.TempDir(), "redis"),
		Host:    "127.0.0.1",
		Port:    70000,
	})
	if err == nil {
		t.Fatal("startManagedRedisProcess() error = nil")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Fatalf("startManagedRedisProcess() validation error = %v, want port", err)
	}
	if lookPathCalled {
		t.Fatal("startManagedRedisProcess() discovered redis-server before validating config")
	}
}

func TestValidateManagedRedisConfigRejectsRequiredEmptyFields(t *testing.T) {
	base := managedRedisConfig{
		DataDir:  filepath.Join(t.TempDir(), "redis"),
		Host:     "127.0.0.1",
		Port:     6379,
		AuthMode: "relaxed",
	}
	tests := []struct {
		name string
		cfg  managedRedisConfig
		want string
	}{
		{
			name: "data dir",
			cfg:  func() managedRedisConfig { cfg := base; cfg.DataDir = " "; return cfg }(),
			want: "data directory",
		},
		{
			name: "host",
			cfg:  func() managedRedisConfig { cfg := base; cfg.Host = " "; return cfg }(),
			want: "host",
		},
		{
			name: "strict password",
			cfg:  func() managedRedisConfig { cfg := base; cfg.AuthMode = "strict"; return cfg }(),
			want: "password",
		},
		{
			name: "unknown auth",
			cfg:  func() managedRedisConfig { cfg := base; cfg.AuthMode = "unknown"; return cfg }(),
			want: "auth mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateManagedRedisConfig(tt.cfg)
			if err == nil {
				t.Fatal("validateManagedRedisConfig() error = nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateManagedRedisConfig() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestManagedRedisArgsUseExpectedRedisServerFlags(t *testing.T) {
	args := managedRedisArgs(managedRedisConfig{
		DataDir:     filepath.Join(t.TempDir(), "redis"),
		Host:        "127.0.0.1",
		Port:        6380,
		MaxMemoryMB: 128,
		AppendOnly:  true,
		AuthMode:    "strict",
		Password:    "secret",
	})
	joined := strings.Join(args, " ")
	for _, want := range []string{"--bind 127.0.0.1", "--port 6380", "--dir ", "--save 60 1", "--appendonly yes", "--maxmemory 128mb", "--requirepass secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("managedRedisArgs() missing %q: %s", want, joined)
		}
	}
}

func TestManagedRedisArgsOmitRequirePassInRelaxedMode(t *testing.T) {
	args := managedRedisArgs(managedRedisConfig{
		DataDir: filepath.Join(t.TempDir(), "redis"),
		Host:    "127.0.0.1",
		Port:    6379,
	})
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--requirepass") {
		t.Fatalf("managedRedisArgs() included requirepass in relaxed mode: %s", joined)
	}
	if !strings.Contains(joined, "--appendonly no") || !strings.Contains(joined, "--maxmemory 256mb") {
		t.Fatalf("managedRedisArgs() missing relaxed defaults: %s", joined)
	}
}

func TestManagedRedisCloseTerminatesChildProcess(t *testing.T) {
	cmd := exec.Command("sh", "-c", "trap 'exit 0' TERM; while true; do sleep 1; done")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child process: %v", err)
	}
	redis := &managedRedis{cmd: cmd, addr: "127.0.0.1:6379"}
	start := time.Now()
	if err := redis.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close() took %s, expected graceful SIGTERM before kill timeout", elapsed)
	}
}
