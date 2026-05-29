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

// sqsRustEngine reports whether the SQS service should be served by the Rust
// reimplementation (strangler-fig increment #3) instead of the in-process Go
// server, and returns the binary path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_SQS_ENGINE=rust       select the Rust SQS server
//	DEVCLOUD_SQS_RUST_BIN=<path>   path to the binary
//	                               (default: ./rust/target/debug/devcloud-sqs)
//
// The Rust server speaks the AWS JSON 1.0 protocol (modern SDK default). The
// legacy Query/XML protocol is not served by the Rust engine; clients needing
// it must use the Go engine.
func sqsRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_SQS_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_SQS_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-sqs")
	}
	return bin, true
}

// runSQSRust launches the Rust SQS server as a subprocess bound to the same
// loopback port the Go server would have used, pointed at the same storage path.
// The Rust server writes a byte-compatible state.json, so state survives
// switching engines back and forth.
//
// On context cancellation it is sent SIGTERM (graceful) and SIGKILLed after a
// short grace period if it does not exit.
func runSQSRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_SQS_ADDR="+loopbackAddr(cfg.Server.SQSPort),
		"DEVCLOUD_SQS_STORAGE="+filepath.Join(cfg.Storage.Path, "sqs"),
		"DEVCLOUD_SQS_REGION="+cfg.Services.SQS.Region,
		"DEVCLOUD_SQS_ACCOUNT_ID="+cfg.Auth.SQS.AccountID,
		"DEVCLOUD_SQS_QUEUE_URL_HOST="+cfg.Services.SQS.QueueURLHost,
		"DEVCLOUD_SQS_AUTH_MODE="+cfg.Auth.SQS.Mode,
		"DEVCLOUD_SQS_ACCESS_KEY="+cfg.Auth.SQS.AccessKeyID,
		"DEVCLOUD_SQS_SECRET_KEY="+cfg.Auth.SQS.SecretAccessKey,
		"DEVCLOUD_SQS_MAX_QUEUES="+strconv.Itoa(cfg.Services.SQS.MaxQueues),
		"DEVCLOUD_SQS_MAX_MESSAGE_BYTES="+strconv.FormatInt(cfg.Services.SQS.MaxMessageBytes, 10),
		"DEVCLOUD_SQS_MAX_RECEIVE_BATCH_SIZE="+strconv.Itoa(cfg.Services.SQS.MaxReceiveBatchSize),
		"DEVCLOUD_SQS_DEFAULT_VISIBILITY_TIMEOUT="+strconv.Itoa(cfg.Services.SQS.DefaultVisibilityTimeoutSeconds),
		"DEVCLOUD_SQS_DEFAULT_DELAY="+strconv.Itoa(cfg.Services.SQS.DefaultDelaySeconds),
		"DEVCLOUD_SQS_DEFAULT_RETENTION="+strconv.Itoa(cfg.Services.SQS.DefaultMessageRetentionSeconds),
		"DEVCLOUD_SQS_DEFAULT_WAIT_TIME="+strconv.Itoa(cfg.Services.SQS.DefaultReceiveWaitTimeSeconds),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust sqs server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust sqs server exited: %w", err)
		}
		return fmt.Errorf("run rust sqs server: %w", err)
	}
	return nil
}
