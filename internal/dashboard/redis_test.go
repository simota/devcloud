package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	redissvc "devcloud/internal/services/redis"
)

func TestRedisStatusWhenEnabledDoesNotLeakPassword(t *testing.T) {
	server := NewServer(Config{RedisEndpoint: "redis://127.0.0.1:16379", RedisStoragePath: ".devcloud/test/redis"}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{
		Mode:     redissvc.ModeManaged,
		Addr:     "127.0.0.1:16379",
		AuthMode: redissvc.AuthModeStrict,
		Password: "secret",
		DataDir:  ".devcloud/test/redis",
	}))

	rec := performRequest(server.routes(), http.MethodGet, "/api/redis/status")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); containsAny(body, []string{"secret", "password"}) {
		t.Fatalf("redis status leaked sensitive data: %s", body)
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if response["service"] != "redis" || response["running"] != true || response["mode"] != redissvc.ModeManaged {
		t.Fatalf("status response = %#v", response)
	}
}

func TestRedisKeysReturnsNonNullArrayForEmptyKeyspace(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{Mode: redissvc.ModeManaged, Addr: "127.0.0.1:16379"}))

	rec := performRequest(server.routes(), http.MethodGet, "/api/redis/keys?cursor=0&count=100")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response redisKeysResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode keys: %v", err)
	}
	if response.Keys == nil {
		t.Fatal("keys = nil, want empty slice")
	}
	if len(response.Keys) != 0 || response.NextCursor != 0 {
		t.Fatalf("keys response = %#v, want empty keyspace", response)
	}
}

func TestRedisKeyDetailReturnsNonNullPreviewForMissingKey(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{Mode: redissvc.ModeManaged, Addr: "127.0.0.1:16379"}))

	rec := performRequest(server.routes(), http.MethodGet, "/api/redis/keys/missing")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response redisKeyDetailResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode key detail: %v", err)
	}
	if response.Preview == nil {
		t.Fatal("preview = nil, want empty slice")
	}
	if response.Type != "none" || response.TTLSeconds != -2 {
		t.Fatalf("key detail = %#v", response)
	}
}

func TestRedisKeyDetailAcceptsEscapedSlashInKey(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{Mode: redissvc.ModeManaged, Addr: "127.0.0.1:16379"}))

	rec := performRequest(server.routes(), http.MethodGet, "/api/redis/keys/session%2Fuser%3A123")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response redisKeyDetailResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode key detail: %v", err)
	}
	if response.Key != "session/user:123" {
		t.Fatalf("key = %q, want escaped slash decoded", response.Key)
	}
	if response.Preview == nil {
		t.Fatal("preview = nil, want empty slice")
	}
}

func TestRedisCommandRejectsDangerousCommands(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{Mode: redissvc.ModeManaged, Addr: "127.0.0.1:16379"}))

	rec := performRequestWithBody(server.routes(), http.MethodPost, "/api/redis/command", `{"command":"FLUSHALL","args":[]}`)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

func TestRedisCommandResponseRowsAreNonNull(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedis(redissvc.NewServer(redissvc.Config{Mode: redissvc.ModeManaged, Addr: "127.0.0.1:16379"}))

	rec := performRequestWithBody(server.routes(), http.MethodPost, "/api/redis/command", `{"command":"GET","args":["key"]}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response redisCommandResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode command response: %v", err)
	}
	if response.Rows == nil {
		t.Fatal("rows = nil, want empty slice")
	}
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
