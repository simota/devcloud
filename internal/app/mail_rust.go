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

// mailRustEngine reports whether the SMTP service should be served by the Rust
// reimplementation (strangler-fig increment #1) instead of the in-process Go
// server, and returns the binary path to launch.
//
// This is an opt-in, development-only seam gated entirely on environment
// variables, so the default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_MAIL_ENGINE=rust         select the Rust SMTP server
//	DEVCLOUD_MAIL_RUST_BIN=<path>     path to the `devcloud-mail` binary
//	                                  (default: ./rust/target/debug/devcloud-mail)
func mailRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_MAIL_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_MAIL_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-mail")
	}
	return bin, true
}

// runMailRust launches the Rust SMTP server as a subprocess bound to the same
// loopback port the Go server would have used, pointed at the same storage
// paths. The Rust FileStore writes a byte-compatible on-disk format, so the
// dashboard (which reads mail straight from the filesystem) is unaffected.
//
// The subprocess shares stdout/stderr with the daemon. On context cancellation
// it is sent SIGTERM (the Rust binary shuts its accept loop down gracefully) and
// SIGKILLed after a short grace period if it does not exit.
func runMailRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_MAIL_ADDR="+loopbackAddr(cfg.Server.SMTPPort),
		"DEVCLOUD_MAIL_STORAGE="+filepath.Join(cfg.Storage.Path, "mail"),
		"DEVCLOUD_MAIL_BLOBS="+filepath.Join(cfg.Storage.Path, "blobs"),
		"DEVCLOUD_MAIL_MAX_BYTES="+strconv.FormatInt(cfg.Services.Mail.MaxMessageBytes, 10),
		"DEVCLOUD_MAIL_AUTH_MODE="+cfg.Auth.SMTP.Mode,
		"DEVCLOUD_MAIL_USERNAME="+cfg.Auth.SMTP.Username,
		"DEVCLOUD_MAIL_PASSWORD="+cfg.Auth.SMTP.Password,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Graceful shutdown: SIGTERM on cancel, then SIGKILL after the grace period.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust mail server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	// A SIGTERM/SIGKILL triggered by context cancellation is a clean shutdown,
	// not a service failure — surface context.Canceled so Daemon.Run treats it
	// like the Go servers' ctx-cancel return.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust mail server exited: %w", err)
		}
		return fmt.Errorf("run rust mail server: %w", err)
	}
	return nil
}
