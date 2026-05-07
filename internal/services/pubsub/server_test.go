package pubsub

import (
	"context"
	"net"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRESTReadiness(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["service"] != "pubsub" || body["status"] != "running" || body["protocol"] != "rest" {
		t.Fatalf("body = %#v", body)
	}
}

func TestServerDefaultsPubSubListenerAddresses(t *testing.T) {
	server := NewServer(Config{})

	if server.config.GRPCAddr != "127.0.0.1:8085" {
		t.Fatalf("GRPCAddr = %q, want %q", server.config.GRPCAddr, "127.0.0.1:8085")
	}
	if server.config.RESTAddr != "127.0.0.1:8086" {
		t.Fatalf("RESTAddr = %q, want %q", server.config.RESTAddr, "127.0.0.1:8086")
	}
	if server.config.EnablePush {
		t.Fatalf("EnablePush default = true, want false")
	}
}

func TestGRPCReadiness(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["service"] != "pubsub" || body["status"] != "running" || body["protocol"] != "grpc" {
		t.Fatalf("body = %#v", body)
	}
}

func TestListenerCountMatchesRESTConfiguration(t *testing.T) {
	restDisabled := NewServer(Config{Project: "devcloud", RESTEnabled: false})
	if got := restDisabled.listenerCount(); got != 1 {
		t.Fatalf("listenerCount() with REST disabled = %d, want 1", got)
	}

	restEnabled := NewServer(Config{Project: "devcloud", RESTEnabled: true})
	if got := restEnabled.listenerCount(); got != 2 {
		t.Fatalf("listenerCount() with REST enabled = %d, want 2", got)
	}
}

func TestGRPCReadinessReportsStoreLoadFailureSafely(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	if err := os.WriteFile(storagePath, []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub store: %v", err)
	}
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "pubsub resource store unavailable") {
		t.Fatalf("body = %s", body)
	}
	if strings.Contains(body, storagePath) {
		t.Fatalf("readiness leaked storage path: %s", body)
	}
}

func TestGRPCHealthDoesNotRequireStoreReadiness(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	if err := os.WriteFile(storagePath, []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub store: %v", err)
	}
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestGRPCReadinessRejectsUnsupportedMethod(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)

	server.grpcRoutes().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET" {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func newPubSubGRPCTestClients(t *testing.T, config Config) (*Server, context.Context, pubsubpb.PublisherClient, pubsubpb.SubscriberClient) {
	t.Helper()
	server := NewServer(config)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := server.newGRPCServer()
	t.Cleanup(grpcServer.Stop)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Errorf("grpc serve: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return server, ctx, pubsubpb.NewPublisherClient(conn), pubsubpb.NewSubscriberClient(conn)
}

func performPubSubRequest(server *Server, method string, path string, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	server.ServeHTTP(rec, req)
	return rec
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
