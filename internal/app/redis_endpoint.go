package app

import (
	"net/url"
	"strings"
)

func redisEndpointForDisplay(cfg Config) string {
	fallback := "redis://" + loopbackAddr(cfg.Server.RedisPort)
	if redisMode(cfg.Services.Redis) != "external" {
		return fallback
	}
	raw := strings.TrimSpace(cfg.Services.Redis.ExternalURL)
	if raw == "" {
		return fallback
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fallback
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
