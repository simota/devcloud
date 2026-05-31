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

// gcsRustEngine reports whether the GCS JSON API should be served by the Rust
// reimplementation instead of the in-process Go server, and returns the binary
// path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_GCS_ENGINE=rust       select the Rust GCS server
//	DEVCLOUD_GCS_RUST_BIN=<path>   path to the binary
//	                              (default: ./rust/target/debug/devcloud-gcs)
func gcsRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_GCS_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_GCS_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-gcs")
	}
	return bin, true
}

// runGCSRust launches the Rust GCS JSON API server as a subprocess bound to the
// same loopback port the Go server would have used. It points at the same shared
// S3 bucket storage root so GCS and S3 continue to see the same object data.
func runGCSRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_GCS_ADDR="+loopbackAddr(cfg.Server.GCSPort),
		"DEVCLOUD_GCS_PROJECT="+defaultString(cfg.Services.GCS.Project, cfg.Auth.GCS.Project),
		"DEVCLOUD_GCS_LOCATION="+cfg.Services.GCS.Location,
		"DEVCLOUD_GCS_AUTH_MODE="+cfg.Auth.GCS.Mode,
		"DEVCLOUD_GCS_BEARER_TOKEN="+cfg.Auth.GCS.BearerToken,
		"DEVCLOUD_GCS_STORAGE="+filepath.Join(cfg.Storage.Path, "s3", "buckets"),
		"DEVCLOUD_GCS_UPLOAD_SESSIONS="+filepath.Join(cfg.Storage.Path, "gcs", "upload_sessions"),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust gcs server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust gcs server exited: %w", err)
		}
		return fmt.Errorf("run rust gcs server: %w", err)
	}
	return nil
}
