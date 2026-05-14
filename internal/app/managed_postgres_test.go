package app

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiscoverManagedPostgresCommandReturnsActionableError(t *testing.T) {
	originalLookPath := managedPostgresLookPath
	managedPostgresLookPath = func(name string) (string, error) {
		return "", exec.ErrNotFound
	}
	t.Cleanup(func() {
		managedPostgresLookPath = originalLookPath
	})

	_, err := discoverManagedPostgresCommand("initdb")
	if err == nil {
		t.Fatal("discoverManagedPostgresCommand() error = nil")
	}
	for _, want := range []string{"initdb", "PATH", "mode=external", "externalDsn"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("discoverManagedPostgresCommand() error missing %q: %v", want, err)
		}
	}
}

func TestStartManagedPostgresValidatesConfigBeforeCommandDiscovery(t *testing.T) {
	originalLookPath := managedPostgresLookPath
	lookPathCalled := false
	managedPostgresLookPath = func(name string) (string, error) {
		lookPathCalled = true
		return "", exec.ErrNotFound
	}
	t.Cleanup(func() {
		managedPostgresLookPath = originalLookPath
	})

	_, err := startManagedPostgres(context.Background(), managedPostgresConfig{
		DataDir:  filepath.Join(t.TempDir(), "postgres"),
		Host:     "127.0.0.1",
		Port:     70000,
		Database: "dev",
		User:     "dev",
		Password: "secret",
	})
	if err == nil {
		t.Fatal("startManagedPostgres() error = nil")
	}
	if !strings.Contains(err.Error(), "port") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("startManagedPostgres() validation error = %v", err)
	}
	if lookPathCalled {
		t.Fatal("startManagedPostgres() discovered commands before validating config")
	}
}

func TestValidateManagedPostgresConfigRejectsRequiredEmptyFields(t *testing.T) {
	base := managedPostgresConfig{
		DataDir:  filepath.Join(t.TempDir(), "postgres"),
		Host:     "127.0.0.1",
		Port:     15439,
		Database: "dev",
		User:     "dev",
		Password: "secret",
	}
	tests := []struct {
		name string
		cfg  managedPostgresConfig
		want string
	}{
		{
			name: "data dir",
			cfg:  func() managedPostgresConfig { cfg := base; cfg.DataDir = " "; return cfg }(),
			want: "data directory",
		},
		{
			name: "host",
			cfg:  func() managedPostgresConfig { cfg := base; cfg.Host = " "; return cfg }(),
			want: "host",
		},
		{
			name: "database",
			cfg:  func() managedPostgresConfig { cfg := base; cfg.Database = " "; return cfg }(),
			want: "database",
		},
		{
			name: "user",
			cfg:  func() managedPostgresConfig { cfg := base; cfg.User = " "; return cfg }(),
			want: "user",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateManagedPostgresConfig(tt.cfg)
			if err == nil {
				t.Fatal("validateManagedPostgresConfig() error = nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateManagedPostgresConfig() error = %v, want %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("validateManagedPostgresConfig() leaked password: %v", err)
			}
		})
	}
}

func TestEnsureManagedPostgresDataDirInitializesOnce(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "postgres")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	originalCommand := managedPostgresCommand
	managedPostgresCommand = fakeManagedPostgresCommand(t, logPath)
	t.Cleanup(func() {
		managedPostgresCommand = originalCommand
	})

	cfg := managedPostgresConfig{DataDir: dataDir, User: "dev", Password: "secret"}
	if err := ensureManagedPostgresDataDir(context.Background(), "initdb", cfg); err != nil {
		t.Fatalf("ensureManagedPostgresDataDir() error = %v", err)
	}
	if err := ensureManagedPostgresDataDir(context.Background(), "initdb", cfg); err != nil {
		t.Fatalf("ensureManagedPostgresDataDir() second error = %v", err)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if got := strings.Count(string(contents), "initdb"); got != 1 {
		t.Fatalf("initdb command count = %d, want 1; log=%s", got, contents)
	}
	for _, want := range []string{"--auth-host=scram-sha-256", "--auth-local=trust", "--pwfile"} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("initdb command missing %q: %s", want, contents)
		}
	}
	if !strings.Contains(string(contents), "-U "+managedPostgresBootstrapUser) {
		t.Fatalf("initdb command should use bootstrap user: %s", contents)
	}
	if strings.Contains(string(contents), "secret") {
		t.Fatalf("initdb command log leaked password: %s", contents)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "PG_VERSION")); err != nil {
		t.Fatalf("PG_VERSION was not created: %v", err)
	}
}

func TestEnsureManagedPostgresDatabaseCreatesMissingDatabase(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "postgres")
	socketDir := filepath.Join(t.TempDir(), "socket")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	originalCommand := managedPostgresCommand
	managedPostgresCommand = fakeManagedPostgresCommand(t, logPath)
	t.Cleanup(func() {
		managedPostgresCommand = originalCommand
	})

	cfg := managedPostgresConfig{
		DataDir:   dataDir,
		SocketDir: socketDir,
		Host:      "127.0.0.1",
		Port:      15439,
		Database:  `dev"warehouse`,
		User:      "dev",
		Password:  "secret",
	}
	if err := ensureManagedPostgresDatabase(context.Background(), "psql", cfg); err != nil {
		t.Fatalf("ensureManagedPostgresDatabase() error = %v", err)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	log := string(contents)
	for _, want := range []string{
		"select 1 from pg_database where datname = 'dev\"warehouse'",
		"-h " + socketDir,
		"-p 15439",
		"-U " + managedPostgresBootstrapUser,
		"-v ON_ERROR_STOP=1",
		"-f -",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("psql command log missing %q: %s", want, log)
		}
	}
	if strings.Contains(log, "secret") {
		t.Fatalf("psql command log leaked password: %s", log)
	}
}

func TestEnsureManagedPostgresUserDoesNotPutPasswordInCommandArgs(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "postgres")
	socketDir := filepath.Join(t.TempDir(), "socket")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	originalCommand := managedPostgresCommand
	managedPostgresCommand = fakeManagedPostgresCommand(t, logPath)
	t.Cleanup(func() {
		managedPostgresCommand = originalCommand
	})

	cfg := managedPostgresConfig{
		DataDir:   dataDir,
		SocketDir: socketDir,
		Host:      "127.0.0.1",
		Port:      15439,
		Database:  "dev",
		User:      `analyst"user`,
		Password:  "secret",
	}
	if err := ensureManagedPostgresUser(context.Background(), "psql", cfg); err != nil {
		t.Fatalf("ensureManagedPostgresUser() error = %v", err)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	log := string(contents)
	for _, want := range []string{"psql", "-h " + socketDir, "-p 15439", "-U " + managedPostgresBootstrapUser, "-v ON_ERROR_STOP=1", "-f -"} {
		if !strings.Contains(log, want) {
			t.Fatalf("psql command log missing %q: %s", want, log)
		}
	}
	if strings.Contains(log, "secret") {
		t.Fatalf("psql command log leaked password: %s", log)
	}
}

func TestManagedPostgresCommandEnvReplacesExistingPassword(t *testing.T) {
	env := managedPostgresCommandEnv([]string{
		"PATH=/usr/bin",
		"PGPASSWORD=old-secret",
		"PGDATABASE=postgres",
	}, managedPostgresConfig{Password: "new-secret"})

	joined := strings.Join(env, "\n")
	if strings.Contains(joined, "old-secret") {
		t.Fatalf("managedPostgresCommandEnv() leaked previous password: %s", joined)
	}
	if strings.Count(joined, "PGPASSWORD=") != 1 {
		t.Fatalf("managedPostgresCommandEnv() should set exactly one PGPASSWORD: %s", joined)
	}
	if !strings.Contains(joined, "PGPASSWORD=new-secret") {
		t.Fatalf("managedPostgresCommandEnv() did not set configured password: %s", joined)
	}
	if !strings.Contains(joined, "PATH=/usr/bin") || !strings.Contains(joined, "PGDATABASE=postgres") {
		t.Fatalf("managedPostgresCommandEnv() dropped unrelated environment: %s", joined)
	}
}

func TestManagedPostgresDSNRedactionPreservesConnectionShape(t *testing.T) {
	dsn := managedPostgresDSN(managedPostgresConfig{
		Host:     "127.0.0.1",
		Port:     15439,
		Database: "dev",
		User:     "dev",
		Password: "secret",
	})
	if !strings.Contains(dsn, "secret") {
		t.Fatalf("managedPostgresDSN() should include password for connection: %s", dsn)
	}
	redacted := redactedManagedPostgresDSN(dsn)
	if strings.Contains(redacted, "secret") {
		t.Fatalf("redacted DSN leaked password: %s", redacted)
	}
	for _, want := range []string{"postgres://dev:redacted@", "127.0.0.1:15439", "/dev", "sslmode=disable"} {
		if !strings.Contains(redacted, want) {
			t.Fatalf("redacted DSN missing %q: %s", want, redacted)
		}
	}
}

func TestStartManagedPostgresClosesProcessWhenStartupWaitFails(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "commands.log")
	originalLookPath := managedPostgresLookPath
	originalCommand := managedPostgresCommand
	originalWait := managedPostgresWait
	managedPostgresLookPath = func(name string) (string, error) {
		return name, nil
	}
	managedPostgresCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name == "postgres" {
			commandArgs := []string{"-test.run=TestManagedPostgresHelperProcess", "--", "postgres-sleep"}
			cmd := exec.CommandContext(ctx, os.Args[0], commandArgs...)
			cmd.Env = append(os.Environ(),
				"GO_WANT_MANAGED_POSTGRES_HELPER_PROCESS=1",
				"MANAGED_POSTGRES_HELPER_LOG="+logPath,
			)
			return cmd
		}
		return fakeManagedPostgresCommand(t, logPath)(ctx, name, args...)
	}
	managedPostgresWait = func(ctx context.Context, host string, port int) error {
		return errors.New("startup probe failed")
	}
	t.Cleanup(func() {
		managedPostgresLookPath = originalLookPath
		managedPostgresCommand = originalCommand
		managedPostgresWait = originalWait
	})

	_, err := startManagedPostgres(context.Background(), managedPostgresConfig{
		DataDir:  filepath.Join(t.TempDir(), "postgres"),
		Host:     "127.0.0.1",
		Port:     15439,
		Database: "dev",
		User:     "dev",
		Password: "secret",
	})
	if err == nil {
		t.Fatal("startManagedPostgres() error = nil")
	}
	if !strings.Contains(err.Error(), "startup probe failed") {
		t.Fatalf("startManagedPostgres() error = %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("startManagedPostgres() startup error leaked password: %v", err)
	}
}

func TestStartManagedPostgresUsesGracefulCancelForServerProcess(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "commands.log")
	originalLookPath := managedPostgresLookPath
	originalCommand := managedPostgresCommand
	originalWait := managedPostgresWait
	managedPostgresLookPath = func(name string) (string, error) {
		return name, nil
	}
	managedPostgresCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		if name == "postgres" {
			commandArgs := []string{"-test.run=TestManagedPostgresHelperProcess", "--", "postgres-sleep"}
			cmd := exec.CommandContext(ctx, os.Args[0], commandArgs...)
			cmd.Env = append(os.Environ(),
				"GO_WANT_MANAGED_POSTGRES_HELPER_PROCESS=1",
				"MANAGED_POSTGRES_HELPER_LOG="+logPath,
			)
			return cmd
		}
		return fakeManagedPostgresCommand(t, logPath)(ctx, name, args...)
	}
	managedPostgresWait = func(ctx context.Context, host string, port int) error {
		return nil
	}
	t.Cleanup(func() {
		managedPostgresLookPath = originalLookPath
		managedPostgresCommand = originalCommand
		managedPostgresWait = originalWait
	})

	ctx, cancel := context.WithCancel(context.Background())
	pg, err := startManagedPostgres(ctx, managedPostgresConfig{
		DataDir:  filepath.Join(t.TempDir(), "postgres"),
		Host:     "127.0.0.1",
		Port:     15439,
		Database: "dev",
		User:     "dev",
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("startManagedPostgres() error = %v", err)
	}
	cancel()
	if err := pg.Close(); err != nil {
		t.Fatalf("Close() after context cancellation error = %v", err)
	}
}

func TestRedactManagedPostgresSecretRedactsShortPassword(t *testing.T) {
	output := "initdb failed for password dev"
	redacted := redactManagedPostgresSecret(output, "dev")
	if strings.Contains(redacted, "dev") {
		t.Fatalf("redacted output leaked short password: %s", redacted)
	}
	if !strings.Contains(redacted, "redacted") {
		t.Fatalf("redacted output missing marker: %s", redacted)
	}
}

func TestManagedPostgresCloseIsIdempotent(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=TestManagedPostgresHelperProcess", "--", "postgres-sleep")
	cmd.Env = append(os.Environ(),
		"GO_WANT_MANAGED_POSTGRES_HELPER_PROCESS=1",
		"MANAGED_POSTGRES_HELPER_LOG="+filepath.Join(t.TempDir(), "commands.log"),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}

	pg := &managedPostgres{cmd: cmd}
	if err := pg.Close(); err != nil {
		t.Fatalf("Close() first error = %v", err)
	}
	if err := pg.Close(); err != nil {
		t.Fatalf("Close() second error = %v", err)
	}
}

func fakeManagedPostgresCommand(t *testing.T, logPath string) func(context.Context, string, ...string) *exec.Cmd {
	t.Helper()
	return func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commandArgs := append([]string{"-test.run=TestManagedPostgresHelperProcess", "--", name}, args...)
		cmd := exec.CommandContext(ctx, os.Args[0], commandArgs...)
		cmd.Env = append(os.Environ(),
			"GO_WANT_MANAGED_POSTGRES_HELPER_PROCESS=1",
			"MANAGED_POSTGRES_HELPER_LOG="+logPath,
		)
		return cmd
	}
}

func TestManagedPostgresHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_MANAGED_POSTGRES_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	separator := -1
	for i, arg := range args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 || separator+1 >= len(args) {
		os.Exit(2)
	}
	command := strings.Join(args[separator+1:], " ")
	name := args[separator+1]
	if err := appendHelperLog(os.Getenv("MANAGED_POSTGRES_HELPER_LOG"), command+"\n"); err != nil {
		os.Exit(2)
	}
	if name == "postgres-sleep" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	if name != "initdb" {
		os.Exit(0)
	}
	dataDir := ""
	for i := separator + 2; i < len(args)-1; i++ {
		if args[i] == "-D" {
			dataDir = args[i+1]
			break
		}
	}
	if dataDir == "" {
		os.Exit(2)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o600); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func appendHelperLog(path string, value string) error {
	if path == "" {
		return errors.New("missing helper log path")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(value)
	return err
}
