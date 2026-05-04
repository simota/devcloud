package app

import (
	"context"
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

func TestDaemonEnabledServerCountIncludesPubSubAndDashboard(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Mail.Enabled = true
	cfg.Services.S3.Enabled = true
	cfg.Services.GCS.Enabled = true
	cfg.Services.DynamoDB.Enabled = true
	cfg.Services.BigQuery.Enabled = true
	cfg.Services.SQS.Enabled = true
	cfg.Services.PubSub.Enabled = true

	if got := NewDaemon(cfg).enabledServerCount(); got != 8 {
		t.Fatalf("enabledServerCount() = %d, want 8", got)
	}

	cfg.Services.PubSub.Enabled = false
	if got := NewDaemon(cfg).enabledServerCount(); got != 7 {
		t.Fatalf("enabledServerCount() without Pub/Sub = %d, want 7", got)
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
