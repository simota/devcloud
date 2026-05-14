package redis

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

const (
	ModeManaged  = "managed"
	ModeExternal = "external"

	AuthModeRelaxed = "relaxed"
	AuthModeStrict  = "strict"
)

type Config struct {
	Mode        string
	Addr        string
	ExternalURL string
	AuthMode    string
	Password    string
	DataDir     string
}

func (cfg Config) normalized() Config {
	cfg.Mode = normalizeMode(cfg.Mode, cfg.ExternalURL)
	cfg.AuthMode = strings.ToLower(strings.TrimSpace(cfg.AuthMode))
	if cfg.AuthMode == "" {
		cfg.AuthMode = AuthModeRelaxed
	}
	cfg.Addr = strings.TrimSpace(cfg.Addr)
	cfg.ExternalURL = strings.TrimSpace(cfg.ExternalURL)
	return cfg
}

func normalizeMode(mode string, externalURL string) string {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	if normalized != "" {
		return normalized
	}
	if strings.TrimSpace(externalURL) != "" {
		return ModeExternal
	}
	return ModeManaged
}

func validateConfig(cfg Config) error {
	cfg = cfg.normalized()
	switch cfg.Mode {
	case ModeManaged:
		if strings.TrimSpace(cfg.Addr) == "" {
			return errors.New("redis address is required for managed mode")
		}
		if _, _, err := net.SplitHostPort(cfg.Addr); err != nil {
			return fmt.Errorf("redis address must be host:port: %w", err)
		}
	case ModeExternal:
		if cfg.ExternalURL == "" {
			return errors.New("redis externalUrl is required for external mode")
		}
		parsed, err := url.Parse(cfg.ExternalURL)
		if err != nil {
			return errors.New("redis externalUrl is invalid")
		}
		if parsed.Scheme != "redis" && parsed.Scheme != "rediss" {
			return fmt.Errorf("redis externalUrl scheme must be redis or rediss: %s", parsed.Scheme)
		}
		if parsed.Host == "" {
			return errors.New("redis externalUrl host is required")
		}
	default:
		return fmt.Errorf("unsupported redis mode: %s", cfg.Mode)
	}
	switch cfg.AuthMode {
	case AuthModeRelaxed:
	case AuthModeStrict:
		if cfg.Password == "" {
			return errors.New("redis strict auth requires password")
		}
	default:
		return fmt.Errorf("unsupported redis auth mode: %s", cfg.AuthMode)
	}
	return nil
}
