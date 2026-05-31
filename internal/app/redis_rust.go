package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// redisRustEngine reports whether managed Redis should be owned by the Rust
// runner instead of the Go managed-redis child-process wrapper.
//
// Opt-in, development-only:
//
//	DEVCLOUD_REDIS_ENGINE=rust       select the Rust Redis runner
//	DEVCLOUD_REDIS_RUST_BIN=<path>   path to the binary
//	                                 (default: ./rust/target/debug/devcloud-redis)
func redisRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_REDIS_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_REDIS_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-redis")
	}
	return bin, true
}

type rustRedisLifecycle struct {
	cmd  *exec.Cmd
	addr string
}

func startRedisRust(ctx context.Context, cfg Config, binPath string) (managedRedisLifecycle, error) {
	addr := loopbackAddr(cfg.Server.RedisPort)
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = redisRustEnv(cfg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start rust redis runner %q: %w", binPath, err)
	}
	redis := &rustRedisLifecycle{cmd: cmd, addr: addr}
	if err := waitForManagedRedisTCP(ctx, "127.0.0.1", cfg.Server.RedisPort); err != nil {
		_ = redis.Close()
		return nil, fmt.Errorf("wait for rust redis startup: %w", err)
	}
	return redis, nil
}

func redisRustEnv(cfg Config) []string {
	return append(os.Environ(),
		"DEVCLOUD_REDIS_ADDR="+loopbackAddr(cfg.Server.RedisPort),
		"DEVCLOUD_REDIS_DATA_DIR="+redisDataDir(cfg),
		"DEVCLOUD_REDIS_BINARY="+cfg.Services.Redis.BinaryPath,
		"DEVCLOUD_REDIS_MAX_MEMORY_MB="+strconv.Itoa(cfg.Services.Redis.MaxMemoryMB),
		"DEVCLOUD_REDIS_APPEND_ONLY="+strconv.FormatBool(cfg.Services.Redis.AppendOnly),
		"DEVCLOUD_REDIS_AUTH_MODE="+cfg.Auth.Redis.Mode,
		"DEVCLOUD_REDIS_PASSWORD="+cfg.Auth.Redis.Password,
	)
}

func (r *rustRedisLifecycle) Addr() string {
	return r.addr
}

func (r *rustRedisLifecycle) Close() error {
	if r == nil || r.cmd == nil || r.cmd.Process == nil {
		return nil
	}
	if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- r.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil && !managedRedisExpectedTermination(err) {
			return err
		}
		return nil
	case <-time.After(5 * time.Second):
		if err := r.cmd.Process.Kill(); err != nil {
			return err
		}
		err := <-done
		if err != nil && !managedRedisExpectedTermination(err) {
			return err
		}
		return nil
	}
}
