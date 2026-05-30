package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"devcloud/internal/events"
)

const s3RustEventPrefix = "DEVCLOUD_EVENT "

// s3RustEngine reports whether the S3 service should be served by the Rust
// reimplementation (strangler-fig increment #7) instead of the in-process Go
// server, and returns the binary path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_S3_ENGINE=rust       select the Rust S3 server
//	DEVCLOUD_S3_RUST_BIN=<path>   path to the binary
//	                              (default: ./rust/target/debug/devcloud-s3)
//
// The Rust server serves the S3 HTTP parity surface used by the S3 acceptance
// gate.
func s3RustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_S3_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_S3_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-s3")
	}
	return bin, true
}

// runS3Rust launches the Rust S3 server as a subprocess bound to the same
// loopback port the Go server would have used, pointed at the same bucket
// storage root. The Rust server writes the same bucket/object layout, so state
// survives switching engines back and forth.
//
// On context cancellation it is sent SIGTERM (graceful) and SIGKILLed after a
// short grace period if it does not exit.
func runS3Rust(ctx context.Context, cfg Config, binPath string, publisher events.Publisher) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = s3RustEnv(cfg)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("capture rust s3 stdout: %w", err)
	}
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust s3 server %q: %w", binPath, err)
	}
	stdoutDone := make(chan struct{})
	go func() {
		defer close(stdoutDone)
		forwardS3RustOutput(stdout, publisher, os.Stdout)
	}()

	err = cmd.Wait()
	<-stdoutDone
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust s3 server exited: %w", err)
		}
		return fmt.Errorf("run rust s3 server: %w", err)
	}
	return nil
}

func s3RustEnv(cfg Config) []string {
	return append(os.Environ(),
		"DEVCLOUD_S3_ADDR="+loopbackAddr(cfg.Server.S3Port),
		"DEVCLOUD_S3_STORAGE="+filepath.Join(cfg.Storage.Path, "s3", "buckets"),
		"DEVCLOUD_S3_AUTH_MODE="+cfg.Auth.S3.Mode,
		"DEVCLOUD_S3_ACCESS_KEY_ID="+cfg.Auth.S3.AccessKeyID,
		"DEVCLOUD_S3_SECRET_ACCESS_KEY="+cfg.Auth.S3.SecretAccessKey,
		"DEVCLOUD_S3_REGION="+cfg.Services.S3.Region,
	)
}

func forwardS3RustOutput(r io.Reader, publisher events.Publisher, passthrough io.Writer) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, s3RustEventPrefix) {
			publishS3RustEvent(strings.TrimPrefix(line, s3RustEventPrefix), publisher)
			continue
		}
		if passthrough != nil {
			fmt.Fprintln(passthrough, line)
		}
	}
}

func publishS3RustEvent(raw string, publisher events.Publisher) {
	var event events.Event
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return
	}
	if event.Type == "" || event.Service == "" {
		return
	}
	events.Emit(publisher, event)
}
