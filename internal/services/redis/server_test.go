package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestValidateConfigAcceptsManagedDefaults(t *testing.T) {
	cfg := Config{Mode: ModeManaged, Addr: "127.0.0.1:6379", AuthMode: AuthModeRelaxed}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
}

func TestValidateConfigInfersExternalMode(t *testing.T) {
	cfg := Config{ExternalURL: "redis://127.0.0.1:6379/1", AuthMode: AuthModeStrict, Password: "secret"}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	if got := NewServer(cfg).Config().Mode; got != ModeExternal {
		t.Fatalf("NewServer().Config().Mode = %q, want %q", got, ModeExternal)
	}
}

func TestValidateConfigRejectsUnsafeOrIncompleteConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "managed missing addr", cfg: Config{Mode: ModeManaged}, want: "address"},
		{name: "managed bad addr", cfg: Config{Mode: ModeManaged, Addr: "127.0.0.1"}, want: "host:port"},
		{name: "external missing url", cfg: Config{Mode: ModeExternal}, want: "externalUrl"},
		{name: "external bad scheme", cfg: Config{Mode: ModeExternal, ExternalURL: "http://127.0.0.1:6379"}, want: "scheme"},
		{name: "strict missing password", cfg: Config{Mode: ModeManaged, Addr: "127.0.0.1:6379", AuthMode: AuthModeStrict}, want: "password"},
		{name: "bad auth mode", cfg: Config{Mode: ModeManaged, Addr: "127.0.0.1:6379", AuthMode: "unknown"}, want: "auth mode"},
		{name: "bad mode", cfg: Config{Mode: "memory", Addr: "127.0.0.1:6379"}, want: "mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.cfg)
			if err == nil {
				t.Fatal("validateConfig() error = nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateConfig() error = %v, want %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("validateConfig() leaked password: %v", err)
			}
		})
	}
}

func TestValidateConfigDoesNotLeakExternalURLPassword(t *testing.T) {
	cfg := Config{
		Mode:        ModeExternal,
		ExternalURL: "redis://:super-secret\x7f@127.0.0.1:6379/0",
		AuthMode:    AuthModeRelaxed,
	}

	err := validateConfig(cfg)
	if err == nil {
		t.Fatal("validateConfig() error = nil")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("validateConfig() leaked password: %v", err)
	}
}

func TestNewClientDoesNotLeakExternalURLPassword(t *testing.T) {
	server := NewServer(Config{
		Mode:        ModeExternal,
		ExternalURL: "redis://:super-secret@127.0.0.1:not-a-port/0",
		AuthMode:    AuthModeRelaxed,
	})

	_, err := server.newClient()
	if err == nil {
		t.Fatal("newClient() error = nil")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("newClient() leaked password: %v", err)
	}
}

func TestCommandAllowedClassifiesAllowlist(t *testing.T) {
	tests := []struct {
		command string
		class   CommandClass
	}{
		{command: "get", class: CommandClassRead},
		{command: "HGETALL", class: CommandClassRead},
		{command: "scan", class: CommandClassRead},
		{command: "set", class: CommandClassMutation},
		{command: "ZADD", class: CommandClassMutation},
	}
	for _, tt := range tests {
		class, ok := CommandAllowed(tt.command)
		if !ok || class != tt.class {
			t.Fatalf("CommandAllowed(%q) = %q, %t; want %q, true", tt.command, class, ok, tt.class)
		}
	}
}

func TestCommandAllowedRejectsDangerousCommands(t *testing.T) {
	for _, command := range []string{"CONFIG SET", "FLUSHALL", "SCRIPT", "EVAL", "DEBUG", "SHUTDOWN", "CLUSTER", "REPLICAOF", "MODULE"} {
		if class, ok := CommandAllowed(command); ok {
			t.Fatalf("CommandAllowed(%q) = %q, true; want rejected", command, class)
		}
	}
}

func TestRedisPreviewAndResultRowsAreStableNonNilSlices(t *testing.T) {
	preview := mapPreview(map[string]string{
		"beta":  "2",
		"alpha": "1",
	})
	if got, want := strings.Join(preview, ","), "alpha: 1,beta: 2"; got != want {
		t.Fatalf("mapPreview() = %q, want %q", got, want)
	}

	rows := redisResultRows([]string(nil))
	if rows == nil {
		t.Fatal("redisResultRows([]string(nil)) = nil, want empty slice")
	}
	if len(rows) != 0 {
		t.Fatalf("redisResultRows([]string(nil)) length = %d, want 0", len(rows))
	}
}

func TestServerRunReturnsContextCancellationAfterValidation(t *testing.T) {
	server := NewServer(Config{Mode: ModeManaged, Addr: "127.0.0.1:6379"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Run(ctx)
	}()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}
