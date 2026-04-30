package app

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Project  string
	Server   ServerConfig
	Auth     AuthConfig
	Storage  StorageConfig
	Services ServicesConfig
}

type ServerConfig struct {
	SMTPPort      int
	DashboardPort int
}

type AuthConfig struct {
	SMTP SMTPAuthConfig
}

type SMTPAuthConfig struct {
	Mode string
}

type StorageConfig struct {
	Path string
}

type ServicesConfig struct {
	Mail MailServiceConfig
}

type MailServiceConfig struct {
	Enabled         bool
	MaxMessageBytes int64
}

func DefaultConfig() Config {
	return Config{
		Project: "dev",
		Server: ServerConfig{
			SMTPPort:      1025,
			DashboardPort: 8025,
		},
		Auth: AuthConfig{
			SMTP: SMTPAuthConfig{Mode: "off"},
		},
		Storage: StorageConfig{Path: ".devcloud/data"},
		Services: ServicesConfig{
			Mail: MailServiceConfig{
				Enabled:         true,
				MaxMessageBytes: 10 * 1024 * 1024,
			},
		},
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var section []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		raw := scanner.Text()
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		indent := leadingSpaces(raw) / 2
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return Config{}, fmt.Errorf("parse config line %q: missing ':'", raw)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		if value == "" {
			if indent > len(section) {
				return Config{}, fmt.Errorf("parse config line %q: unexpected indentation", raw)
			}
			section = append(section[:indent], key)
			continue
		}

		path := append(append([]string(nil), section[:minInt(indent, len(section))]...), key)
		if err := applyConfigValue(&cfg, path, value); err != nil {
			return Config{}, err
		}
	}
	if err := scanner.Err(); err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return cfg, nil
}

func InitWorkspace(cfg Config) error {
	if err := validateStoragePath(cfg.Storage.Path); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "blobs"), 0o755); err != nil {
		return fmt.Errorf("create blob storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "mail"), 0o755); err != nil {
		return fmt.Errorf("create mail storage: %w", err)
	}
	if err := ensureFile(filepath.Join(cfg.Storage.Path, "mail", "index.json"), []byte("{}\n")); err != nil {
		return fmt.Errorf("create mail index: %w", err)
	}
	if err := os.MkdirAll(".devcloud/logs", 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	return ensureFile(".devcloud/config.yaml", []byte(defaultConfigYAML(cfg)))
}

func ResetWorkspace(cfg Config) error {
	if err := validateStoragePath(cfg.Storage.Path); err != nil {
		return err
	}
	if err := os.RemoveAll(cfg.Storage.Path); err != nil {
		return fmt.Errorf("remove storage: %w", err)
	}
	return InitWorkspace(cfg)
}

func validateStoragePath(path string) error {
	clean := filepath.Clean(path)
	if clean == ".devcloud" || strings.HasPrefix(clean, ".devcloud"+string(filepath.Separator)) {
		return nil
	}
	return fmt.Errorf("storage.path must be under .devcloud: %s", path)
}

func defaultConfigYAML(cfg Config) string {
	return fmt.Sprintf(`project: %s

server:
  smtpPort: %d
  dashboardPort: %d

auth:
  smtp:
    mode: %s

storage:
  path: %s

services:
  mail:
    enabled: %t
    maxMessageBytes: %d
`, cfg.Project, cfg.Server.SMTPPort, cfg.Server.DashboardPort, cfg.Auth.SMTP.Mode, cfg.Storage.Path, cfg.Services.Mail.Enabled, cfg.Services.Mail.MaxMessageBytes)
}

func ensureFile(path string, data []byte) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func applyConfigValue(cfg *Config, path []string, value string) error {
	switch strings.Join(path, ".") {
	case "project":
		cfg.Project = value
	case "server.smtpPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.smtpPort: %w", err)
		}
		cfg.Server.SMTPPort = port
	case "server.dashboardPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.dashboardPort: %w", err)
		}
		cfg.Server.DashboardPort = port
	case "auth.smtp.mode":
		cfg.Auth.SMTP.Mode = value
	case "storage.path":
		cfg.Storage.Path = value
	case "services.mail.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.mail.enabled: %w", err)
		}
		cfg.Services.Mail.Enabled = enabled
	case "services.mail.maxMessageBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.mail.maxMessageBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.mail.maxMessageBytes: must be positive")
		}
		cfg.Services.Mail.MaxMessageBytes = maxBytes
	default:
		return nil
	}
	return nil
}

func leadingSpaces(value string) int {
	for i, r := range value {
		if r != ' ' {
			return i
		}
	}
	return len(value)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
