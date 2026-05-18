package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type managedPostgresLifecycle interface {
	DSN() string
	Close() error
}

type managedPostgres struct {
	cmd       *exec.Cmd
	dsn       string
	socketDir string
	closeOnce sync.Once
	closeErr  error
}

type managedPostgresConfig struct {
	DataDir   string
	SocketDir string
	Host      string
	Port      int
	Database  string
	User      string
	Password  string
}

const managedPostgresBootstrapUser = "devcloud"

var (
	managedPostgresLookPath      = exec.LookPath
	managedPostgresCommand       = exec.CommandContext
	managedPostgresWait          = waitForManagedPostgresTCP
	startManagedRedshiftPostgres = startManagedRedshiftPostgresProcess
)

func startManagedRedshiftPostgresProcess(ctx context.Context, cfg Config) (managedPostgresLifecycle, error) {
	port, err := managedRedshiftPostgresPort(cfg.Server.RedshiftPort)
	if err != nil {
		return nil, err
	}
	pgCfg := managedPostgresConfig{
		DataDir:  filepath.Join(redshiftDataDir(cfg), "postgres"),
		Host:     "127.0.0.1",
		Port:     port,
		Database: cfg.Services.Redshift.Database,
		User:     cfg.Auth.Redshift.User,
		Password: cfg.Auth.Redshift.Password,
	}
	return startManagedPostgres(ctx, pgCfg)
}

// managedRedshiftPostgresPort returns the TCP port the in-process managed
// PostgreSQL backend should bind. The deterministic default is
// RedshiftPort+10000 so test reproducibility is preserved; if that would
// exceed the TCP ceiling (RedshiftPort > 55535, common when callers free a
// port via macOS's high-numbered ephemeral range) we fall back to an
// OS-assigned ephemeral port. Tests can override the strategy by stubbing
// managedRedshiftPostgresEphemeralPort.
func managedRedshiftPostgresPort(redshiftPort int) (int, error) {
	port := redshiftPort + 10000
	if port >= 1 && port <= 65535 {
		return port, nil
	}
	return managedRedshiftPostgresEphemeralPort()
}

var managedRedshiftPostgresEphemeralPort = func() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate managed redshift postgres port: %w", err)
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("allocate managed redshift postgres port: unexpected addr type %T", listener.Addr())
	}
	return addr.Port, nil
}

func startManagedPostgres(ctx context.Context, cfg managedPostgresConfig) (*managedPostgres, error) {
	if err := validateManagedPostgresConfig(cfg); err != nil {
		return nil, err
	}
	cfg = normalizeManagedPostgresConfig(cfg)
	initdbPath, err := discoverManagedPostgresCommand("initdb")
	if err != nil {
		return nil, err
	}
	postgresPath, err := discoverManagedPostgresCommand("postgres")
	if err != nil {
		return nil, err
	}
	psqlPath, err := discoverManagedPostgresCommand("psql")
	if err != nil {
		return nil, err
	}
	if err := ensureManagedPostgresDataDir(ctx, initdbPath, cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.SocketDir, 0o700); err != nil {
		return nil, fmt.Errorf("create managed redshift postgres socket directory: %w", err)
	}

	cmd := managedPostgresCommand(ctx, postgresPath,
		"-D", cfg.DataDir,
		"-h", cfg.Host,
		"-p", strconv.Itoa(cfg.Port),
		"-k", cfg.SocketDir,
	)
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(os.Interrupt)
	}
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start managed redshift postgres process: %w", err)
	}
	pg := &managedPostgres{cmd: cmd, dsn: managedPostgresDSN(cfg), socketDir: cfg.SocketDir}
	if err := managedPostgresWait(ctx, cfg.Host, cfg.Port); err != nil {
		_ = pg.Close()
		return nil, fmt.Errorf("wait for managed redshift postgres startup: %w", err)
	}
	if err := ensureManagedPostgresUser(ctx, psqlPath, cfg); err != nil {
		_ = pg.Close()
		return nil, err
	}
	if err := ensureManagedPostgresDatabase(ctx, psqlPath, cfg); err != nil {
		_ = pg.Close()
		return nil, err
	}
	return pg, nil
}

func normalizeManagedPostgresConfig(cfg managedPostgresConfig) managedPostgresConfig {
	if strings.TrimSpace(cfg.SocketDir) == "" {
		cfg.SocketDir = filepath.Join(os.TempDir(), "devcloud-redshift-postgres-"+strconv.Itoa(cfg.Port))
	}
	return cfg
}

func validateManagedPostgresConfig(cfg managedPostgresConfig) error {
	if strings.TrimSpace(cfg.DataDir) == "" {
		return errors.New("managed redshift postgres data directory is required")
	}
	if strings.TrimSpace(cfg.Host) == "" {
		return errors.New("managed redshift postgres host is required")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("managed redshift postgres port must be between 1 and 65535: %d", cfg.Port)
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return errors.New("managed redshift postgres database name is required")
	}
	if strings.TrimSpace(cfg.User) == "" {
		return errors.New("managed redshift postgres user is required")
	}
	return nil
}

func discoverManagedPostgresCommand(name string) (string, error) {
	path, err := managedPostgresLookPath(name)
	if err == nil {
		return path, nil
	}
	return "", fmt.Errorf("redshift managed postgres requires %q on PATH; install PostgreSQL client/server binaries or use services.redshift.backend.mode=external with externalDsn: %w", name, err)
}

func ensureManagedPostgresDataDir(ctx context.Context, initdbPath string, cfg managedPostgresConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("create managed redshift postgres data directory: %w", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "PG_VERSION")); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect managed redshift postgres data directory: %w", err)
	}

	pwFile, err := writeManagedPostgresPasswordFile(cfg)
	if err != nil {
		return err
	}
	defer os.Remove(pwFile)

	cmd := managedPostgresCommand(ctx, initdbPath,
		"-D", cfg.DataDir,
		"-U", managedPostgresBootstrapUser,
		"--auth-host=scram-sha-256",
		"--auth-local=trust",
		"--pwfile", pwFile,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("initialize managed redshift postgres data directory with initdb: %w: %s", err, redactManagedPostgresSecret(string(output), cfg.Password))
	}
	return nil
}

func writeManagedPostgresPasswordFile(cfg managedPostgresConfig) (string, error) {
	file, err := os.CreateTemp(filepath.Dir(cfg.DataDir), ".devcloud-postgres-pw-*")
	if err != nil {
		return "", fmt.Errorf("create managed redshift postgres password file: %w", err)
	}
	path := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(path)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("secure managed redshift postgres password file: %w", err)
	}
	if _, err := file.WriteString(cfg.Password + "\n"); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write managed redshift postgres password file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close managed redshift postgres password file: %w", err)
	}
	cleanup = false
	return path, nil
}

func ensureManagedPostgresUser(ctx context.Context, psqlPath string, cfg managedPostgresConfig) error {
	script := fmt.Sprintf(`
do $devcloud$
begin
	if exists (select 1 from pg_roles where rolname = %s) then
		execute format('alter role %%I with login password %%L', %s, %s);
	else
		execute format('create role %%I with login password %%L', %s, %s);
	end if;
end
$devcloud$;
`, postgresStringLiteral(cfg.User), postgresStringLiteral(cfg.User), postgresStringLiteral(cfg.Password), postgresStringLiteral(cfg.User), postgresStringLiteral(cfg.Password))
	if err := runManagedPostgresPSQLScript(ctx, psqlPath, cfg, "postgres", script); err != nil {
		return fmt.Errorf("ensure managed redshift postgres user: %w", err)
	}
	return nil
}

func ensureManagedPostgresDatabase(ctx context.Context, psqlPath string, cfg managedPostgresConfig) error {
	existsSQL := "select 1 from pg_database where datname = " + postgresStringLiteral(cfg.Database)
	cmd := managedPostgresCommand(ctx, psqlPath,
		"-h", cfg.SocketDir,
		"-p", strconv.Itoa(cfg.Port),
		"-U", managedPostgresBootstrapUser,
		"-d", "postgres",
		"-tAc", existsSQL,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect managed redshift postgres database: %w: %s", err, redactManagedPostgresSecret(string(output), cfg.Password))
	}
	if strings.TrimSpace(string(output)) == "1" {
		return nil
	}

	createSQL := "create database " + postgresIdentifier(cfg.Database) + " owner " + postgresIdentifier(cfg.User) + ";"
	if err := runManagedPostgresPSQLScript(ctx, psqlPath, cfg, "postgres", createSQL); err != nil {
		return fmt.Errorf("create managed redshift postgres database: %w", err)
	}
	return nil
}

func runManagedPostgresPSQLScript(ctx context.Context, psqlPath string, cfg managedPostgresConfig, database string, script string) error {
	cmd := managedPostgresCommand(ctx, psqlPath,
		"-h", cfg.SocketDir,
		"-p", strconv.Itoa(cfg.Port),
		"-U", managedPostgresBootstrapUser,
		"-d", database,
		"-v", "ON_ERROR_STOP=1",
		"-f", "-",
	)
	cmd.Stdin = strings.NewReader(script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("run psql script: %w: %s", err, redactManagedPostgresSecret(string(output), cfg.Password))
	}
	return nil
}

func managedPostgresCommandEnv(base []string, cfg managedPostgresConfig) []string {
	if base == nil {
		base = os.Environ()
	}
	env := make([]string, 0, len(base)+1)
	for _, item := range base {
		if !strings.HasPrefix(item, "PGPASSWORD=") {
			env = append(env, item)
		}
	}
	return append(env, "PGPASSWORD="+cfg.Password)
}

func postgresStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func postgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func managedPostgresDSN(cfg managedPostgresConfig) string {
	values := url.Values{}
	values.Set("sslmode", "disable")
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:     "/" + cfg.Database,
		RawQuery: values.Encode(),
	}
	return u.String()
}

func redactedManagedPostgresDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	username := u.User.Username()
	if username == "" {
		u.User = url.UserPassword("redacted", "redacted")
	} else {
		u.User = url.UserPassword(username, "redacted")
	}
	return u.String()
}

func redactPostgresConnectionError(err error, dsn string) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if strings.TrimSpace(dsn) == "" {
		return message
	}
	redactedDSN := redactedManagedPostgresDSN(dsn)
	message = strings.ReplaceAll(message, dsn, redactedDSN)
	u, parseErr := url.Parse(dsn)
	if parseErr == nil && u.User != nil {
		if password, ok := u.User.Password(); ok && password != "" {
			message = strings.ReplaceAll(message, password, "redacted")
		}
	}
	return message
}

func redactManagedPostgresSecret(value string, secret string) string {
	if secret == "" {
		return value
	}
	return strings.ReplaceAll(value, secret, "redacted")
}

func waitForManagedPostgresTCP(ctx context.Context, host string, port int) error {
	deadline := time.Now().Add(15 * time.Second)
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return lastErr
}

func (p *managedPostgres) DSN() string {
	if p == nil {
		return ""
	}
	return p.dsn
}

func (p *managedPostgres) Close() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
			_ = p.cmd.Process.Kill()
		}
		done := make(chan error, 1)
		go func() { done <- p.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "interrupt") {
				p.closeErr = err
			}
		case <-time.After(5 * time.Second):
			if err := p.cmd.Process.Kill(); err != nil {
				p.closeErr = err
				return
			}
			p.closeErr = <-done
		}
		if p.socketDir != "" {
			if err := os.RemoveAll(p.socketDir); err != nil && p.closeErr == nil {
				p.closeErr = err
			}
		}
	})
	return p.closeErr
}
