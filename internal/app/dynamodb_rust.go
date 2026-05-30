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

// dynamoDBRustEngine reports whether the DynamoDB service should be served by the
// Rust reimplementation (strangler-fig increment #4) instead of the in-process
// Go server, and returns the binary path to launch.
//
// Opt-in, development-only, gated entirely on environment variables so the
// default behavior and the YAML config are unchanged:
//
//	DEVCLOUD_DYNAMODB_ENGINE=rust       select the Rust DynamoDB server
//	DEVCLOUD_DYNAMODB_RUST_BIN=<path>   path to the binary
//	                                    (default: ./rust/target/debug/devcloud-dynamodb)
//
// The Rust server speaks the AWS JSON 1.0 protocol (the only protocol the Go
// server speaks too), dispatched by X-Amz-Target.
func dynamoDBRustEngine() (binPath string, enabled bool) {
	if os.Getenv("DEVCLOUD_DYNAMODB_ENGINE") != "rust" {
		return "", false
	}
	bin := os.Getenv("DEVCLOUD_DYNAMODB_RUST_BIN")
	if bin == "" {
		bin = filepath.Join("rust", "target", "debug", "devcloud-dynamodb")
	}
	return bin, true
}

// runDynamoDBRust launches the Rust DynamoDB server as a subprocess bound to the
// same loopback port the Go server would have used, pointed at the same storage
// path. The Rust server writes a byte-compatible state.json, so state survives
// switching engines back and forth.
//
// On context cancellation it is sent SIGTERM (graceful) and SIGKILLed after a
// short grace period if it does not exit.
func runDynamoDBRust(ctx context.Context, cfg Config, binPath string) error {
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DEVCLOUD_DYNAMODB_ADDR="+loopbackAddr(cfg.Server.DynamoDBPort),
		"DEVCLOUD_DYNAMODB_STORAGE="+filepath.Join(cfg.Storage.Path, "dynamodb"),
		"DEVCLOUD_DYNAMODB_REGION="+cfg.Services.DynamoDB.Region,
		"DEVCLOUD_DYNAMODB_AUTH_MODE="+cfg.Auth.DynamoDB.Mode,
		"DEVCLOUD_DYNAMODB_ACCESS_KEY="+cfg.Auth.DynamoDB.AccessKeyID,
		"DEVCLOUD_DYNAMODB_SECRET_KEY="+cfg.Auth.DynamoDB.SecretAccessKey,
		"DEVCLOUD_DYNAMODB_MAX_ITEM_BYTES="+strconv.FormatInt(cfg.Services.DynamoDB.MaxItemBytes, 10),
		"DEVCLOUD_DYNAMODB_MAX_TABLES="+strconv.Itoa(cfg.Services.DynamoDB.MaxTables),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start rust dynamodb server %q: %w", binPath, err)
	}

	err := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("rust dynamodb server exited: %w", err)
		}
		return fmt.Errorf("run rust dynamodb server: %w", err)
	}
	return nil
}
