package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const fakeRedshiftPostgresDriverName = "devcloud_app_redshift_postgres_backend_test"
const failingRedshiftPostgresDriverName = "devcloud_app_redshift_postgres_backend_failure_test"

func init() {
	sql.Register(fakeRedshiftPostgresDriverName, fakeRedshiftPostgresDriver{})
	sql.Register(failingRedshiftPostgresDriverName, failingRedshiftPostgresDriver{})
}

func TestDaemonDoesNotExposeS3DashboardAPIWhenS3Disabled(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Services.Mail.Enabled = false
	cfg.Services.S3.Enabled = false
	cfg.Services.GCS.Enabled = false
	cfg.Services.DynamoDB.Enabled = false
	cfg.Services.BigQuery.Enabled = false
	cfg.Services.SQS.Enabled = false
	cfg.Services.PubSub.Enabled = false
	cfg.Services.Redshift.Enabled = false
	cfg.Server.DashboardPort = freeTCPPort(t)
	cfg.Server.S3Port = freeTCPPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewDaemon(cfg).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("daemon returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("daemon did not stop")
		}
	})

	baseURL := "http://" + loopbackAddr(cfg.Server.DashboardPort)
	waitForHTTP(t, baseURL+"/api/s3/status")

	statusBody := getBody(t, baseURL+"/api/s3/status")
	if !strings.Contains(statusBody, `"status":"disabled"`) || !strings.Contains(statusBody, `"running":false`) {
		t.Fatalf("S3 status should be disabled, got %s", statusBody)
	}
	servicesBody := getBody(t, baseURL+"/api/dashboard/services")
	for _, want := range []string{`"id":"mail"`, `"status":"disabled"`, `"id":"s3"`} {
		if !strings.Contains(servicesBody, want) {
			t.Fatalf("dashboard services missing %q: %s", want, servicesBody)
		}
	}

	resp, err := http.Get(baseURL + "/api/s3/buckets")
	if err != nil {
		t.Fatalf("GET S3 buckets: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("S3 buckets status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestDaemonStartsPubSubEndpointsAndDashboardRegistry(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Services.Mail.Enabled = false
	cfg.Services.S3.Enabled = false
	cfg.Services.GCS.Enabled = false
	cfg.Services.DynamoDB.Enabled = false
	cfg.Services.BigQuery.Enabled = false
	cfg.Services.SQS.Enabled = false
	cfg.Services.Redshift.Enabled = false
	cfg.Services.PubSub.Enabled = true
	cfg.Services.PubSub.EnableREST = true
	cfg.Server.DashboardPort = freeTCPPort(t)
	cfg.Server.PubSubGRPCPort = freeTCPPort(t)
	cfg.Server.PubSubRESTPort = freeTCPPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewDaemon(cfg).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("daemon returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("daemon did not stop")
		}
	})

	pubSubRESTURL := "http://" + loopbackAddr(cfg.Server.PubSubRESTPort)
	dashboardURL := "http://" + loopbackAddr(cfg.Server.DashboardPort)
	waitForHTTP(t, pubSubRESTURL+"/healthz")
	waitForPubSubGRPC(t, loopbackAddr(cfg.Server.PubSubGRPCPort), cfg.Auth.PubSub.ProjectID)
	waitForHTTP(t, dashboardURL+"/api/dashboard/services")

	restHealth := getBody(t, pubSubRESTURL+"/healthz")
	if !strings.Contains(restHealth, `"service":"pubsub"`) || !strings.Contains(restHealth, `"status":"running"`) {
		t.Fatalf("Pub/Sub REST health = %s", restHealth)
	}
	servicesBody := getBody(t, dashboardURL+"/api/dashboard/services")
	for _, want := range []string{`"id":"pubsub"`, `"status":"running"`} {
		if !strings.Contains(servicesBody, want) {
			t.Fatalf("dashboard services missing %q: %s", want, servicesBody)
		}
	}
}

func waitForPubSubGRPC(t *testing.T, addr string, project string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err == nil {
			client := pubsubpb.NewPublisherClient(conn)
			_, lastErr = client.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + project})
			conn.Close()
			cancel()
			if lastErr == nil {
				return
			}
		} else {
			lastErr = err
			cancel()
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Pub/Sub gRPC %s: %v", addr, lastErr)
}

func TestDaemonEnabledServerCountIncludesPubSubRedshiftAndDashboard(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Mail.Enabled = true
	cfg.Services.S3.Enabled = true
	cfg.Services.GCS.Enabled = true
	cfg.Services.DynamoDB.Enabled = true
	cfg.Services.BigQuery.Enabled = true
	cfg.Services.SQS.Enabled = true
	cfg.Services.PubSub.Enabled = true
	cfg.Services.Redshift.Enabled = true

	if got := NewDaemon(cfg).enabledServerCount(); got != 9 {
		t.Fatalf("enabledServerCount() = %d, want 9", got)
	}

	cfg.Services.PubSub.Enabled = false
	if got := NewDaemon(cfg).enabledServerCount(); got != 8 {
		t.Fatalf("enabledServerCount() without Pub/Sub = %d, want 8", got)
	}

	cfg.Services.Redshift.Enabled = false
	if got := NewDaemon(cfg).enabledServerCount(); got != 7 {
		t.Fatalf("enabledServerCount() without Pub/Sub and Redshift = %d, want 7", got)
	}
}

func TestRedshiftSQLBackendUsesManagedPostgresByDefault(t *testing.T) {
	managed := &fakeManagedPostgresLifecycle{dsn: "postgres://dev:secret@127.0.0.1:15439/dev?sslmode=disable"}
	originalStart := startManagedRedshiftPostgres
	startManagedRedshiftPostgres = func(ctx context.Context, cfg Config) (managedPostgresLifecycle, error) {
		return managed, nil
	}
	t.Cleanup(func() {
		startManagedRedshiftPostgres = originalStart
	})

	backend, err := redshiftSQLBackendWithDriver(context.Background(), DefaultConfig(), fakeRedshiftPostgresDriverName)
	if err != nil {
		t.Fatalf("redshiftSQLBackendWithDriver() error = %v", err)
	}
	if backend == nil {
		t.Fatal("redshiftSQLBackendWithDriver() = nil")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !managed.closed {
		t.Fatal("managed lifecycle was not closed")
	}
}

func TestRedshiftSQLBackendUsesMemoryFallbackWhenExplicit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "memory"

	backend, err := redshiftSQLBackend(context.Background(), cfg)
	if err != nil {
		t.Fatalf("redshiftSQLBackend() error = %v", err)
	}
	if backend != nil {
		t.Fatalf("redshiftSQLBackend() = %#v, want nil memory fallback", backend)
	}
}

func TestRedshiftSQLBackendOpensPostgresExternalDSN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "postgres"
	cfg.Services.Redshift.Backend.Mode = "external"
	cfg.Services.Redshift.Backend.ExternalDSN = "postgres://dev:secret@127.0.0.1:5432/dev?sslmode=disable"
	cfg.Services.Redshift.Backend.Managed = false

	backend, err := redshiftSQLBackendWithDriver(context.Background(), cfg, fakeRedshiftPostgresDriverName)
	if err != nil {
		t.Fatalf("redshiftSQLBackendWithDriver() error = %v", err)
	}
	if backend == nil {
		t.Fatal("redshiftSQLBackendWithDriver() = nil")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRedshiftSQLBackendInfersExternalModeWhenDSNConfigured(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "postgres"
	cfg.Services.Redshift.Backend.Mode = ""
	cfg.Services.Redshift.Backend.ExternalDSN = "postgres://dev:secret@127.0.0.1:5432/dev?sslmode=disable"
	cfg.Services.Redshift.Backend.Managed = false

	originalStart := startManagedRedshiftPostgres
	startManagedRedshiftPostgres = func(ctx context.Context, cfg Config) (managedPostgresLifecycle, error) {
		t.Fatal("external DSN mode should not start managed PostgreSQL")
		return nil, nil
	}
	t.Cleanup(func() {
		startManagedRedshiftPostgres = originalStart
	})

	backend, err := redshiftSQLBackendWithDriver(context.Background(), cfg, fakeRedshiftPostgresDriverName)
	if err != nil {
		t.Fatalf("redshiftSQLBackendWithDriver() error = %v", err)
	}
	if backend == nil {
		t.Fatal("redshiftSQLBackendWithDriver() = nil")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := redshiftBackendMode(cfg.Services.Redshift.Backend); got != "external" {
		t.Fatalf("redshiftBackendMode() = %q, want external", got)
	}
}

func TestRedshiftSQLBackendOpensManagedPostgresAndClosesLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "postgres"
	cfg.Services.Redshift.Backend.Mode = "managed"
	cfg.Services.Redshift.Backend.Managed = true

	managed := &fakeManagedPostgresLifecycle{dsn: "postgres://dev:secret@127.0.0.1:15439/dev?sslmode=disable"}
	originalStart := startManagedRedshiftPostgres
	startManagedRedshiftPostgres = func(ctx context.Context, cfg Config) (managedPostgresLifecycle, error) {
		return managed, nil
	}
	t.Cleanup(func() {
		startManagedRedshiftPostgres = originalStart
	})

	backend, err := redshiftSQLBackendWithDriver(context.Background(), cfg, fakeRedshiftPostgresDriverName)
	if err != nil {
		t.Fatalf("redshiftSQLBackendWithDriver() error = %v", err)
	}
	if backend == nil {
		t.Fatal("redshiftSQLBackendWithDriver() = nil")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !managed.closed {
		t.Fatal("managed lifecycle was not closed")
	}
}

func TestRedshiftBackendModeDefaultsManagedWithoutExternalDSN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Mode = ""
	cfg.Services.Redshift.Backend.ExternalDSN = ""

	if got := redshiftBackendMode(cfg.Services.Redshift.Backend); got != "managed" {
		t.Fatalf("redshiftBackendMode() = %q, want managed", got)
	}
}

func TestRedshiftBackendModeReportsMemoryFallbackWhenExplicit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "memory"
	cfg.Services.Redshift.Backend.Mode = ""

	if got := redshiftBackendMode(cfg.Services.Redshift.Backend); got != "memory" {
		t.Fatalf("redshiftBackendMode() = %q, want memory", got)
	}
}

func TestRedshiftSQLBackendManagedOpenFailureRedactsDSN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "postgres"
	cfg.Services.Redshift.Backend.Mode = "managed"
	cfg.Services.Redshift.Backend.Managed = true

	managed := &fakeManagedPostgresLifecycle{dsn: "postgres://dev:secret@127.0.0.1:15439/dev?sslmode=disable"}
	originalStart := startManagedRedshiftPostgres
	startManagedRedshiftPostgres = func(ctx context.Context, cfg Config) (managedPostgresLifecycle, error) {
		return managed, nil
	}
	t.Cleanup(func() {
		startManagedRedshiftPostgres = originalStart
	})

	_, err := redshiftSQLBackendWithDriver(context.Background(), cfg, "missing-redshift-postgres-driver")
	if err == nil {
		t.Fatal("redshiftSQLBackendWithDriver() error = nil")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("managed backend open error leaked password: %v", err)
	}
	if !strings.Contains(err.Error(), "redacted") {
		t.Fatalf("managed backend open error should include redacted DSN: %v", err)
	}
	if !managed.closed {
		t.Fatal("managed lifecycle was not closed after backend open failure")
	}
}

func TestRedshiftSQLBackendExternalOpenFailureRedactsDSN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Redshift.Backend.Kind = "postgres"
	cfg.Services.Redshift.Backend.Mode = "external"
	cfg.Services.Redshift.Backend.ExternalDSN = "postgres://dev:secret@127.0.0.1:5432/dev?sslmode=disable"
	cfg.Services.Redshift.Backend.Managed = false

	_, err := redshiftSQLBackendWithDriver(context.Background(), cfg, failingRedshiftPostgresDriverName)
	if err == nil {
		t.Fatal("redshiftSQLBackendWithDriver() error = nil")
	}
	for _, leaked := range []string{"secret", cfg.Services.Redshift.Backend.ExternalDSN} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("external backend open error leaked %q: %v", leaked, err)
		}
	}
	if !strings.Contains(err.Error(), "postgres://dev:redacted@127.0.0.1:5432/dev?sslmode=disable") {
		t.Fatalf("external backend open error should include redacted DSN: %v", err)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("cannot bind loopback TCP port in this environment: %v", err)
		}
		t.Fatalf("listen on free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func getBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(body)
}

type fakeRedshiftPostgresDriver struct{}

func (fakeRedshiftPostgresDriver) Open(name string) (driver.Conn, error) {
	return fakeRedshiftPostgresConn{}, nil
}

type fakeRedshiftPostgresConn struct{}

func (fakeRedshiftPostgresConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in fake redshift postgres driver")
}

func (fakeRedshiftPostgresConn) Close() error {
	return nil
}

func (fakeRedshiftPostgresConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not implemented in fake redshift postgres driver")
}

func (fakeRedshiftPostgresConn) Ping(ctx context.Context) error {
	return ctx.Err()
}

type failingRedshiftPostgresDriver struct{}

func (failingRedshiftPostgresDriver) Open(name string) (driver.Conn, error) {
	return failingRedshiftPostgresConn{name: name}, nil
}

type failingRedshiftPostgresConn struct {
	name string
}

func (failingRedshiftPostgresConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in failing redshift postgres driver")
}

func (failingRedshiftPostgresConn) Close() error {
	return nil
}

func (failingRedshiftPostgresConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not implemented in failing redshift postgres driver")
}

func (c failingRedshiftPostgresConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("could not connect to " + c.name)
}

type fakeManagedPostgresLifecycle struct {
	dsn    string
	closed bool
}

func (l *fakeManagedPostgresLifecycle) DSN() string {
	return l.dsn
}

func (l *fakeManagedPostgresLifecycle) Close() error {
	l.closed = true
	return nil
}
