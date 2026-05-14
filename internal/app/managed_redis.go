package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type managedRedisLifecycle interface {
	Addr() string
	Close() error
}

type managedRedis struct {
	cmd       *exec.Cmd
	addr      string
	closeOnce sync.Once
	closeErr  error
}

type managedRedisConfig struct {
	DataDir     string
	Host        string
	Port        int
	BinaryPath  string
	MaxMemoryMB int
	AppendOnly  bool
	AuthMode    string
	Password    string
}

var (
	errManagedRedisServerMissing = errors.New("managed redis-server binary missing")

	managedRedisLookPath = exec.LookPath
	managedRedisCommand  = exec.CommandContext
	managedRedisWait     = waitForManagedRedisTCP
	startManagedRedis    = startManagedRedisProcess
)

func startManagedRedisFromConfig(ctx context.Context, cfg Config) (managedRedisLifecycle, error) {
	redisCfg := managedRedisConfig{
		DataDir:     redisDataDir(cfg),
		Host:        "127.0.0.1",
		Port:        cfg.Server.RedisPort,
		BinaryPath:  cfg.Services.Redis.BinaryPath,
		MaxMemoryMB: cfg.Services.Redis.MaxMemoryMB,
		AppendOnly:  cfg.Services.Redis.AppendOnly,
		AuthMode:    cfg.Auth.Redis.Mode,
		Password:    cfg.Auth.Redis.Password,
	}
	return startManagedRedis(ctx, redisCfg)
}

func startManagedRedisProcess(ctx context.Context, cfg managedRedisConfig) (managedRedisLifecycle, error) {
	if err := validateManagedRedisConfig(cfg); err != nil {
		return nil, err
	}
	cfg = normalizeManagedRedisConfig(cfg)
	redisPath, err := discoverManagedRedisCommand(cfg.BinaryPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create managed redis data directory: %w", err)
	}

	args := managedRedisArgs(cfg)
	cmd := managedRedisCommand(ctx, redisPath, args...)
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start managed redis-server process: %w", err)
	}
	redis := &managedRedis{cmd: cmd, addr: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))}
	if err := managedRedisWait(ctx, cfg.Host, cfg.Port); err != nil {
		_ = redis.Close()
		return nil, fmt.Errorf("wait for managed redis startup: %w", err)
	}
	return redis, nil
}

func normalizeManagedRedisConfig(cfg managedRedisConfig) managedRedisConfig {
	cfg.AuthMode = strings.ToLower(strings.TrimSpace(cfg.AuthMode))
	if cfg.AuthMode == "" {
		cfg.AuthMode = "relaxed"
	}
	if cfg.MaxMemoryMB <= 0 {
		cfg.MaxMemoryMB = 256
	}
	return cfg
}

func validateManagedRedisConfig(cfg managedRedisConfig) error {
	if strings.TrimSpace(cfg.DataDir) == "" {
		return errors.New("managed redis data directory is required")
	}
	if strings.TrimSpace(cfg.Host) == "" {
		return errors.New("managed redis host is required")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("managed redis port must be between 1 and 65535: %d", cfg.Port)
	}
	authMode := strings.ToLower(strings.TrimSpace(cfg.AuthMode))
	if authMode == "" {
		authMode = "relaxed"
	}
	switch authMode {
	case "relaxed":
	case "strict":
		if cfg.Password == "" {
			return errors.New("managed redis strict auth requires password")
		}
	default:
		return fmt.Errorf("unsupported managed redis auth mode: %s", cfg.AuthMode)
	}
	return nil
}

func discoverManagedRedisCommand(binaryPath string) (string, error) {
	if strings.TrimSpace(binaryPath) != "" {
		return binaryPath, nil
	}
	path, err := managedRedisLookPath("redis-server")
	if err == nil {
		return path, nil
	}
	return "", fmt.Errorf("%w: managed redis requires %q on PATH; install with brew install redis or apt install redis-server, or set services.redis.mode=external with externalUrl: %v", errManagedRedisServerMissing, "redis-server", err)
}

func managedRedisArgs(cfg managedRedisConfig) []string {
	cfg = normalizeManagedRedisConfig(cfg)
	args := []string{
		"--bind", cfg.Host,
		"--port", strconv.Itoa(cfg.Port),
		"--dir", cfg.DataDir,
		"--save", "60 1",
		"--appendonly", redisBool(cfg.AppendOnly),
		"--maxmemory", strconv.Itoa(cfg.MaxMemoryMB) + "mb",
	}
	if cfg.AuthMode == "strict" {
		args = append(args, "--requirepass", cfg.Password)
	}
	return args
}

func redisBool(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func waitForManagedRedisTCP(ctx context.Context, host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *managedRedis) Addr() string {
	return r.addr
}

func (r *managedRedis) Close() error {
	if r == nil || r.cmd == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.cmd.Process != nil {
			if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
				r.closeErr = err
			}
		}
		done := make(chan error, 1)
		go func() {
			done <- r.cmd.Wait()
		}()
		select {
		case err := <-done:
			if err != nil && !managedRedisExpectedTermination(err) && r.closeErr == nil {
				r.closeErr = err
			}
		case <-time.After(5 * time.Second):
			if r.cmd.Process != nil {
				if err := r.cmd.Process.Kill(); err != nil && r.closeErr == nil {
					r.closeErr = err
				}
			}
			if err := <-done; err != nil && !managedRedisExpectedTermination(err) && r.closeErr == nil {
				r.closeErr = err
			}
		}
	})
	return r.closeErr
}

func managedRedisExpectedTermination(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGTERM
}
