package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// aasRustEngine reports whether the Application Auto Scaling service should be
// served by the Rust reimplementation (strangler-fig increment #2) instead of
// the in-process Go server, and returns the binary path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_AAS_ENGINE=rust       select the Rust server
//	DEVCLOUD_AAS_RUST_BIN=<path>   path to the binary
//	                               (default: ./rust/target/debug/devcloud-applicationautoscaling)
func aasRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_AAS_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_AAS_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-applicationautoscaling")
	}
	return bin, true
}

// runAASRust launches the Rust Application Auto Scaling server as a subprocess
// bound to the same loopback port the Go server would have used, pointed at the
// same storage path. The Rust server writes a byte-compatible state.json, so the
// state survives switching engines back and forth.
//
// On context cancellation it is sent SIGTERM (graceful shutdown) and SIGKILLed
// after a short grace period if it does not exit.
func runAASRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_AAS_ADDR="+loopbackAddr(cfg.Server.AppAutoScalingPort),
		"DEVCLOUD_AAS_STORAGE="+filepath.Join(cfg.Storage.Path, "applicationautoscaling"),
		"DEVCLOUD_AAS_REGION="+cfg.Services.AppAutoScaling.Region,
		"DEVCLOUD_AAS_ACCOUNT_ID="+cfg.Auth.AppAutoScaling.AccountID,
		"DEVCLOUD_AAS_AUTH_MODE="+cfg.Auth.AppAutoScaling.Mode,
		"DEVCLOUD_AAS_ACCESS_KEY="+cfg.Auth.AppAutoScaling.AccessKeyID,
		"DEVCLOUD_AAS_SECRET_KEY="+cfg.Auth.AppAutoScaling.SecretAccessKey,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust application-autoscaling server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust application-autoscaling server exited: %w", err)
		}
		return fmt.Errorf("run rust application-autoscaling server: %w", err)
	}
	return nil
}
