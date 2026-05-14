package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Server struct {
	config Config
	mu     sync.Mutex
	client *goredis.Client
}

func NewServer(cfg Config) *Server {
	return &Server{config: cfg.normalized()}
}

func (s *Server) Run(ctx context.Context) error {
	if err := validateConfig(s.config); err != nil {
		return err
	}
	client, err := s.newClient()
	if err != nil {
		return err
	}
	defer client.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("connect redis: %w", err)
	}
	s.mu.Lock()
	s.client = client
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.client == client {
			s.client = nil
		}
		s.mu.Unlock()
	}()

	<-ctx.Done()
	return ctx.Err()
}

func (s *Server) Config() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

type Status struct {
	Mode             string
	Address          string
	ServerVersion    string
	ConnectedClients int
	UsedMemoryHuman  string
	DB0Keys          int64
}

type KeySummary struct {
	Key        string
	Type       string
	TTLSeconds int64
}

type KeysSnapshot struct {
	Cursor     uint64
	NextCursor uint64
	Keys       []KeySummary
}

type KeyDetail struct {
	Key        string
	Type       string
	TTLSeconds int64
	Preview    []string
}

type CommandResult struct {
	Command string
	Class   CommandClass
	Rows    []string
}

func (s *Server) Status(ctx context.Context) (Status, error) {
	cfg := s.Config()
	status := Status{Mode: cfg.Mode, Address: cfg.redisAddress()}
	client := s.currentClient()
	if client == nil {
		return status, nil
	}
	info, err := client.Info(ctx, "server", "clients", "memory", "keyspace").Result()
	if err != nil {
		return status, fmt.Errorf("read redis status: %w", err)
	}
	values := parseRedisInfo(info)
	status.ServerVersion = values["redis_version"]
	status.UsedMemoryHuman = values["used_memory_human"]
	status.ConnectedClients, _ = strconv.Atoi(values["connected_clients"])
	status.DB0Keys = parseDB0KeyCount(values["db0"])
	return status, nil
}

func (s *Server) Keys(ctx context.Context, cursor uint64, match string, count int64) (KeysSnapshot, error) {
	if strings.TrimSpace(match) == "" {
		match = "*"
	}
	if count <= 0 {
		count = 100
	}
	snapshot := KeysSnapshot{Cursor: cursor, Keys: []KeySummary{}}
	client := s.currentClient()
	if client == nil {
		return snapshot, nil
	}
	keys, nextCursor, err := client.Scan(ctx, cursor, match, count).Result()
	if err != nil {
		return snapshot, fmt.Errorf("scan redis keys: %w", err)
	}
	snapshot.NextCursor = nextCursor
	for _, key := range keys {
		typ, _ := client.Type(ctx, key).Result()
		ttl, _ := client.TTL(ctx, key).Result()
		snapshot.Keys = append(snapshot.Keys, KeySummary{
			Key:        key,
			Type:       typ,
			TTLSeconds: ttlSeconds(ttl),
		})
	}
	return snapshot, nil
}

func (s *Server) KeyDetail(ctx context.Context, key string) (KeyDetail, error) {
	detail := KeyDetail{Key: key, Type: "none", TTLSeconds: -2, Preview: []string{}}
	client := s.currentClient()
	if client == nil {
		return detail, nil
	}
	typ, err := client.Type(ctx, key).Result()
	if err != nil {
		return detail, fmt.Errorf("read redis key type: %w", err)
	}
	ttl, err := client.TTL(ctx, key).Result()
	if err != nil {
		return detail, fmt.Errorf("read redis key ttl: %w", err)
	}
	detail.Type = typ
	detail.TTLSeconds = ttlSeconds(ttl)
	switch typ {
	case "string":
		value, err := client.Get(ctx, key).Result()
		if errors.Is(err, goredis.Nil) {
			return detail, nil
		}
		if err != nil {
			return detail, fmt.Errorf("read redis string key: %w", err)
		}
		detail.Preview = []string{value}
	case "list":
		values, err := client.LRange(ctx, key, 0, 49).Result()
		if err != nil {
			return detail, fmt.Errorf("read redis list key: %w", err)
		}
		detail.Preview = values
	case "hash":
		values, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return detail, fmt.Errorf("read redis hash key: %w", err)
		}
		detail.Preview = mapPreview(values)
	case "set":
		values, err := client.SMembers(ctx, key).Result()
		if err != nil {
			return detail, fmt.Errorf("read redis set key: %w", err)
		}
		detail.Preview = values
	case "zset":
		values, err := client.ZRangeWithScores(ctx, key, 0, 49).Result()
		if err != nil {
			return detail, fmt.Errorf("read redis zset key: %w", err)
		}
		detail.Preview = zsetPreview(values)
	}
	return detail, nil
}

func (s *Server) Exec(ctx context.Context, command string, args []string) (CommandResult, error) {
	name := strings.ToUpper(strings.TrimSpace(command))
	class, ok := CommandAllowed(name)
	result := CommandResult{Command: name, Class: class, Rows: []string{}}
	if !ok {
		return result, ErrCommandNotAllowed
	}
	client := s.currentClient()
	if client == nil {
		return result, nil
	}
	parts := make([]any, 0, 1+len(args))
	parts = append(parts, name)
	for _, arg := range args {
		parts = append(parts, arg)
	}
	value, err := client.Do(ctx, parts...).Result()
	if errors.Is(err, goredis.Nil) {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("execute redis command %s: %w", name, err)
	}
	result.Rows = redisResultRows(value)
	return result, nil
}

func (s *Server) DeleteKey(ctx context.Context, key string) (int64, error) {
	client := s.currentClient()
	if client == nil {
		return 0, nil
	}
	deleted, err := client.Del(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("delete redis key: %w", err)
	}
	return deleted, nil
}

func (s *Server) ExpireKey(ctx context.Context, key string, ttlSeconds int64) (bool, error) {
	client := s.currentClient()
	if client == nil {
		return false, nil
	}
	updated, err := client.Expire(ctx, key, time.Duration(ttlSeconds)*time.Second).Result()
	if err != nil {
		return false, fmt.Errorf("expire redis key: %w", err)
	}
	return updated, nil
}

func (s *Server) FlushDB(ctx context.Context) (string, error) {
	client := s.currentClient()
	if client == nil {
		return "OK", nil
	}
	result, err := client.FlushDB(ctx).Result()
	if err != nil {
		return "", fmt.Errorf("flush redis db: %w", err)
	}
	return result, nil
}

func (s *Server) newClient() (*goredis.Client, error) {
	cfg := s.Config()
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	var opts *goredis.Options
	var err error
	if cfg.Mode == ModeExternal {
		opts, err = goredis.ParseURL(cfg.ExternalURL)
		if err != nil {
			return nil, errors.New("parse redis externalUrl")
		}
	} else {
		opts = &goredis.Options{Addr: cfg.Addr}
	}
	if cfg.AuthMode == AuthModeStrict && opts.Password == "" {
		opts.Password = cfg.Password
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	return goredis.NewClient(opts), nil
}

func (s *Server) currentClient() *goredis.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (cfg Config) redisAddress() string {
	if cfg.Mode == ModeExternal && cfg.ExternalURL != "" {
		if parsed, err := goredis.ParseURL(cfg.ExternalURL); err == nil {
			return parsed.Addr
		}
	}
	return cfg.Addr
}

var ErrCommandNotAllowed = errors.New("redis command is not allowlisted")

func ttlSeconds(ttl time.Duration) int64 {
	if ttl < 0 {
		return int64(ttl / time.Second)
	}
	return int64(ttl.Seconds())
}

func parseRedisInfo(info string) map[string]string {
	values := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[key] = value
		}
	}
	return values
}

func parseDB0KeyCount(value string) int64 {
	for _, part := range strings.Split(value, ",") {
		key, raw, ok := strings.Cut(part, "=")
		if !ok || key != "keys" {
			continue
		}
		count, _ := strconv.ParseInt(raw, 10, 64)
		return count
	}
	return 0
}

func mapPreview(values map[string]string) []string {
	if len(values) == 0 {
		return []string{}
	}
	preview := make([]string, 0, len(values))
	for key, value := range values {
		preview = append(preview, key+": "+value)
	}
	sort.Strings(preview)
	return preview
}

func zsetPreview(values []goredis.Z) []string {
	if len(values) == 0 {
		return []string{}
	}
	preview := make([]string, 0, len(values))
	for _, value := range values {
		preview = append(preview, fmt.Sprintf("%v: %g", value.Member, value.Score))
	}
	return preview
}

func redisResultRows(value any) []string {
	switch typed := value.(type) {
	case nil:
		return []string{}
	case string:
		return []string{typed}
	case []string:
		if typed == nil {
			return []string{}
		}
		return typed
	case int64:
		return []string{strconv.FormatInt(typed, 10)}
	case bool:
		return []string{strconv.FormatBool(typed)}
	case []any:
		rows := make([]string, 0, len(typed))
		for _, item := range typed {
			rows = append(rows, fmt.Sprint(item))
		}
		return rows
	default:
		return []string{fmt.Sprint(typed)}
	}
}
