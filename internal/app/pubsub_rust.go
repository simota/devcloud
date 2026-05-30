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

// pubSubRustEngine reports whether the Pub/Sub **REST** protocol should be served
// by the Rust reimplementation (strangler-fig increment #5) instead of the
// in-process Go REST server, and returns the binary path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_PUBSUB_ENGINE=rust       select the Rust Pub/Sub REST server
//	DEVCLOUD_PUBSUB_RUST_BIN=<path>   path to the binary
//	                                  (default: ./rust/target/debug/devcloud-pubsub)
//
// Only the REST protocol is ported; the gRPC server stays on the Go engine and
// continues to run in-process. Both engines share the same byte-compatible
// resources.json / pubsub.json on disk.
func pubSubRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_PUBSUB_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_PUBSUB_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-pubsub")
	}
	return bin, true
}

// runPubSubRESTRust launches the Rust Pub/Sub REST server as a subprocess bound
// to the same loopback REST port the Go server would have used, pointed at the
// same storage dirs. The Rust server writes byte-compatible resources.json /
// pubsub.json, so state survives switching engines back and forth.
//
// On context cancellation it is sent SIGTERM (graceful) and SIGKILLed after a
// short grace period if it does not exit.
func runPubSubRESTRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_PUBSUB_REST_ADDR="+loopbackAddr(cfg.Server.PubSubRESTPort),
		"DEVCLOUD_PUBSUB_PROJECT="+defaultString(cfg.Services.PubSub.Project, cfg.Auth.PubSub.ProjectID),
		"DEVCLOUD_PUBSUB_AUTH_MODE="+cfg.Auth.PubSub.Mode,
		"DEVCLOUD_PUBSUB_BEARER_TOKEN="+cfg.Auth.PubSub.BearerToken,
		"DEVCLOUD_PUBSUB_STORAGE="+pubsubDataDir(cfg),
		"DEVCLOUD_PUBSUB_MESSAGE_STORAGE="+pubsubMessageDataDir(cfg),
		"DEVCLOUD_PUBSUB_DEFAULT_ACK_DEADLINE="+strconv.Itoa(cfg.Services.PubSub.DefaultAckDeadlineSeconds),
		"DEVCLOUD_PUBSUB_MESSAGE_RETENTION="+strconv.Itoa(cfg.Services.PubSub.MessageRetentionSeconds),
		"DEVCLOUD_PUBSUB_MAX_ACK_DEADLINE="+strconv.Itoa(cfg.Services.PubSub.MaxAckDeadlineSeconds),
		"DEVCLOUD_PUBSUB_MAX_PULL_MESSAGES="+strconv.Itoa(cfg.Services.PubSub.MaxPullMessages),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust pubsub REST server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust pubsub REST server exited: %w", err)
		}
		return fmt.Errorf("run rust pubsub REST server: %w", err)
	}
	return nil
}
