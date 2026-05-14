package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestServerIntegrationWithRedisServer(t *testing.T) {
	redisPath, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server binary not in PATH")
	}
	port := freeRedisIntegrationPort(t)
	dataDir := filepath.Join(t.TempDir(), "redis")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, redisPath,
		"--bind", "127.0.0.1",
		"--port", fmt.Sprint(port),
		"--dir", dataDir,
		"--save", "",
		"--appendonly", "no",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitForRedisIntegrationPing(t, addr)

	server := NewServer(Config{
		Mode:     ModeManaged,
		Addr:     addr,
		AuthMode: AuthModeRelaxed,
		DataDir:  dataDir,
	})
	serverCtx, stopServer := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(serverCtx)
	}()
	t.Cleanup(func() {
		stopServer()
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Server.Run() error = %v", err)
		}
	})

	waitForRedisIntegrationServer(t, server)

	result, err := server.Exec(context.Background(), "SET", []string{"devcloud:integration:string", "value"})
	if err != nil {
		t.Fatalf("Exec(SET) error = %v", err)
	}
	if result.Rows == nil {
		t.Fatal("Exec(SET) rows = nil, want empty or populated slice")
	}
	result, err = server.Exec(context.Background(), "GET", []string{"devcloud:integration:string"})
	if err != nil {
		t.Fatalf("Exec(GET) error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0] != "value" {
		t.Fatalf("Exec(GET) rows = %#v, want value", result.Rows)
	}
	if ok, err := server.ExpireKey(context.Background(), "devcloud:integration:string", 60); err != nil || !ok {
		t.Fatalf("ExpireKey() = %t, %v; want true, nil", ok, err)
	}
	detail, err := server.KeyDetail(context.Background(), "devcloud:integration:string")
	if err != nil {
		t.Fatalf("KeyDetail() error = %v", err)
	}
	if detail.Type != "string" || len(detail.Preview) != 1 || detail.Preview[0] != "value" || detail.TTLSeconds <= 0 {
		t.Fatalf("KeyDetail() = %#v, want string preview with positive TTL", detail)
	}
}

func freeRedisIntegrationPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback bind unavailable: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForRedisIntegrationPing(t *testing.T, addr string) {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	defer client.Close()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		lastErr = client.Ping(ctx).Err()
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for redis-server at %s: %v", addr, lastErr)
}

func waitForRedisIntegrationServer(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := server.Status(context.Background())
		if err == nil && status.ServerVersion != "" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for redis service client")
}
