package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	redissvc "devcloud/internal/services/redis"
)

type redisKeySummary struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

type redisKeysResponse struct {
	Cursor     uint64            `json:"cursor"`
	NextCursor uint64            `json:"nextCursor"`
	Keys       []redisKeySummary `json:"keys"`
}

type redisKeyDetailResponse struct {
	Key        string   `json:"key"`
	Type       string   `json:"type"`
	TTLSeconds int64    `json:"ttlSeconds"`
	Preview    []string `json:"preview"`
}

type redisCommandRequest struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

type redisCommandResponse struct {
	Command string   `json:"command"`
	Class   string   `json:"class"`
	Rows    []string `json:"rows"`
}

func (s *Server) handleRedisStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	mode := "managed"
	address := strings.TrimPrefix(defaultString(s.config.RedisEndpoint, "redis://127.0.0.1:6379"), "redis://")
	if s.redis != nil {
		cfg := s.redis.Config()
		snapshot, err := s.redis.Status(r.Context())
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis status unavailable"})
			return
		}
		status = "running"
		running = true
		mode = defaultString(cfg.Mode, mode)
		address = defaultString(snapshot.Address, address)
		writeJSON(w, map[string]any{
			"service":          "redis",
			"status":           status,
			"running":          running,
			"mode":             mode,
			"address":          address,
			"serverVersion":    snapshot.ServerVersion,
			"connectedClients": snapshot.ConnectedClients,
			"usedMemoryHuman":  snapshot.UsedMemoryHuman,
			"db0Keys":          snapshot.DB0Keys,
			"storagePath":      defaultString(s.config.RedisStoragePath, ".devcloud/data/redis"),
		})
		return
	}
	writeJSON(w, map[string]any{
		"service":          "redis",
		"status":           status,
		"running":          running,
		"mode":             mode,
		"address":          address,
		"serverVersion":    "",
		"connectedClients": 0,
		"usedMemoryHuman":  "",
		"db0Keys":          0,
		"storagePath":      defaultString(s.config.RedisStoragePath, ".devcloud/data/redis"),
	})
}

func (s *Server) handleRedisKeys(w http.ResponseWriter, r *http.Request) {
	if s.redis == nil {
		http.Error(w, "redis service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		cursor, ok := redisCursorFromRequest(w, r)
		if !ok {
			return
		}
		count := redisCountFromRequest(r)
		match := r.URL.Query().Get("match")
		snapshot, err := s.redis.Keys(r.Context(), cursor, match, count)
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis keys unavailable"})
			return
		}
		writeJSON(w, redisKeysResponseFromSnapshot(snapshot))
	case http.MethodDelete:
		if r.URL.Query().Get("confirm") != "FLUSHDB" {
			http.Error(w, "confirm=FLUSHDB is required", http.StatusBadRequest)
			return
		}
		result, err := s.redis.FlushDB(r.Context())
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis flush failed"})
			return
		}
		writeJSON(w, map[string]any{"result": result})
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleRedisKey(w http.ResponseWriter, r *http.Request) {
	if s.redis == nil {
		http.Error(w, "redis service is disabled", http.StatusServiceUnavailable)
		return
	}
	key, action, err := redisKeyPath(r.URL.EscapedPath())
	if err != nil {
		http.Error(w, "invalid redis key path", http.StatusBadRequest)
		return
	}
	switch {
	case action == "" && r.Method == http.MethodGet:
		detail, err := s.redis.KeyDetail(r.Context(), key)
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis key unavailable"})
			return
		}
		writeJSON(w, redisKeyDetailResponseFromSnapshot(detail))
	case action == "" && r.Method == http.MethodDelete:
		deleted, err := s.redis.DeleteKey(r.Context(), key)
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis delete failed"})
			return
		}
		writeJSON(w, map[string]any{"deleted": deleted})
	case action == "expire" && r.Method == http.MethodPost:
		ttl, ok := redisTTLFromRequest(w, r)
		if !ok {
			return
		}
		updated, err := s.redis.ExpireKey(r.Context(), key, ttl)
		if err != nil {
			writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis expire failed"})
			return
		}
		writeJSON(w, map[string]any{"updated": updated})
	default:
		methodNotAllowed(w, "GET, DELETE, POST")
	}
}

func redisKeyPath(escapedPath string) (key string, action string, err error) {
	const prefix = "/api/redis/keys/"
	suffix := strings.TrimPrefix(escapedPath, prefix)
	if suffix == escapedPath || suffix == "" {
		return "", "", errors.New("missing redis key")
	}
	keyPart := strings.TrimSuffix(suffix, "/")
	if strings.HasSuffix(suffix, "/expire") {
		keyPart = strings.TrimSuffix(suffix, "/expire")
		action = "expire"
	}
	key, err = url.PathUnescape(keyPart)
	if err != nil {
		return "", "", err
	}
	if key == "" {
		return "", "", errors.New("missing redis key")
	}
	return key, action, nil
}

func (s *Server) handleRedisCommand(w http.ResponseWriter, r *http.Request) {
	if s.redis == nil {
		http.Error(w, "redis service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request redisCommandRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid redis command request", http.StatusBadRequest)
		return
	}
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	class, ok := redissvc.CommandAllowed(command)
	if !ok {
		writeJSONStatus(w, http.StatusForbidden, map[string]any{"error": "redis command is not allowlisted"})
		return
	}
	result, err := s.redis.Exec(r.Context(), command, request.Args)
	if errors.Is(err, redissvc.ErrCommandNotAllowed) {
		writeJSONStatus(w, http.StatusForbidden, map[string]any{"error": "redis command is not allowlisted"})
		return
	}
	if err != nil {
		writeJSONStatus(w, http.StatusBadGateway, map[string]any{"error": "redis command failed"})
		return
	}
	writeJSON(w, redisCommandResponse{
		Command: command,
		Class:   string(class),
		Rows:    result.Rows,
	})
}

func redisCursorFromRequest(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	raw := r.URL.Query().Get("cursor")
	if raw == "" {
		return 0, true
	}
	cursor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		http.Error(w, "cursor must be a non-negative integer", http.StatusBadRequest)
		return 0, false
	}
	return cursor, true
}

func redisCountFromRequest(r *http.Request) int64 {
	raw := r.URL.Query().Get("count")
	if raw == "" {
		return 100
	}
	count, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || count <= 0 {
		return 100
	}
	if count > 1000 {
		return 1000
	}
	return count
}

func redisTTLFromRequest(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := r.URL.Query().Get("ttlSeconds")
	if raw == "" {
		var request struct {
			TTLSeconds int64 `json:"ttlSeconds"`
		}
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "ttlSeconds is required", http.StatusBadRequest)
			return 0, false
		}
		raw = strconv.FormatInt(request.TTLSeconds, 10)
	}
	ttl, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ttl <= 0 {
		http.Error(w, "ttlSeconds must be a positive integer", http.StatusBadRequest)
		return 0, false
	}
	return ttl, true
}

func redisKeysResponseFromSnapshot(snapshot redissvc.KeysSnapshot) redisKeysResponse {
	keys := make([]redisKeySummary, 0, len(snapshot.Keys))
	for _, key := range snapshot.Keys {
		keys = append(keys, redisKeySummary{
			Key:        key.Key,
			Type:       key.Type,
			TTLSeconds: key.TTLSeconds,
		})
	}
	return redisKeysResponse{
		Cursor:     snapshot.Cursor,
		NextCursor: snapshot.NextCursor,
		Keys:       keys,
	}
}

func redisKeyDetailResponseFromSnapshot(snapshot redissvc.KeyDetail) redisKeyDetailResponse {
	preview := snapshot.Preview
	if preview == nil {
		preview = []string{}
	}
	return redisKeyDetailResponse{
		Key:        snapshot.Key,
		Type:       snapshot.Type,
		TTLSeconds: snapshot.TTLSeconds,
		Preview:    preview,
	}
}
