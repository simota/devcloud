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
	S3Port        int
	GCSPort       int
	DynamoDBPort  int
}

type AuthConfig struct {
	SMTP     SMTPAuthConfig
	S3       S3AuthConfig
	GCS      GCSAuthConfig
	DynamoDB DynamoDBAuthConfig
}

type SMTPAuthConfig struct {
	Mode string
}

type S3AuthConfig struct {
	Mode            string
	AccessKeyID     string
	SecretAccessKey string
}

type GCSAuthConfig struct {
	Mode        string
	Project     string
	BearerToken string
}

type DynamoDBAuthConfig struct {
	Mode            string
	AccessKeyID     string
	SecretAccessKey string
}

type StorageConfig struct {
	Path string
}

type ServicesConfig struct {
	Mail     MailServiceConfig
	S3       S3ServiceConfig
	GCS      GCSServiceConfig
	DynamoDB DynamoDBServiceConfig
}

type MailServiceConfig struct {
	Enabled         bool
	MaxMessageBytes int64
}

type S3ServiceConfig struct {
	Enabled          bool
	Region           string
	PathStyle        bool
	VirtualHostStyle bool
	MaxObjectBytes   int64
	Multipart        S3MultipartConfig
}

type GCSServiceConfig struct {
	Enabled  bool
	Project  string
	Location string
}

type DynamoDBServiceConfig struct {
	Enabled      bool
	Region       string
	BillingMode  string
	MaxItemBytes int64
	MaxTables    int
	Streams      DynamoDBStreamsConfig
	TTL          DynamoDBTTLConfig
}

type DynamoDBStreamsConfig struct {
	Enabled bool
}

type DynamoDBTTLConfig struct {
	SchedulerIntervalSeconds int
}

type S3MultipartConfig struct {
	MinPartBytes int64
}

func DefaultConfig() Config {
	return Config{
		Project: "dev",
		Server: ServerConfig{
			SMTPPort:      1025,
			DashboardPort: 8025,
			S3Port:        4566,
			GCSPort:       4443,
			DynamoDBPort:  8000,
		},
		Auth: AuthConfig{
			SMTP: SMTPAuthConfig{Mode: "off"},
			S3: S3AuthConfig{
				Mode:            "relaxed",
				AccessKeyID:     "dev",
				SecretAccessKey: "dev",
			},
			GCS: GCSAuthConfig{
				Mode:    "relaxed",
				Project: "devcloud",
			},
			DynamoDB: DynamoDBAuthConfig{
				Mode:            "relaxed",
				AccessKeyID:     "dev",
				SecretAccessKey: "dev",
			},
		},
		Storage: StorageConfig{Path: ".devcloud/data"},
		Services: ServicesConfig{
			Mail: MailServiceConfig{
				Enabled:         true,
				MaxMessageBytes: 10 * 1024 * 1024,
			},
			S3: S3ServiceConfig{
				Enabled:          true,
				Region:           "us-east-1",
				PathStyle:        true,
				VirtualHostStyle: false,
				MaxObjectBytes:   5 * 1024 * 1024 * 1024,
				Multipart: S3MultipartConfig{
					MinPartBytes: 5 * 1024 * 1024,
				},
			},
			GCS: GCSServiceConfig{
				Enabled:  true,
				Project:  "devcloud",
				Location: "US",
			},
			DynamoDB: DynamoDBServiceConfig{
				Enabled:      true,
				Region:       "us-east-1",
				BillingMode:  "PAY_PER_REQUEST",
				MaxItemBytes: 400000,
				MaxTables:    256,
				Streams: DynamoDBStreamsConfig{
					Enabled: false,
				},
				TTL: DynamoDBTTLConfig{
					SchedulerIntervalSeconds: 60,
				},
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
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "s3", "buckets"), 0o755); err != nil {
		return fmt.Errorf("create s3 bucket storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "s3", "multipart"), 0o755); err != nil {
		return fmt.Errorf("create s3 multipart storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "gcs", "upload_sessions"), 0o755); err != nil {
		return fmt.Errorf("create gcs upload session storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "dynamodb"), 0o755); err != nil {
		return fmt.Errorf("create dynamodb storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "kv"), 0o755); err != nil {
		return fmt.Errorf("create kv storage: %w", err)
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
  s3Port: %d
  gcsPort: %d
  dynamodbPort: %d

auth:
  smtp:
    mode: %s
  s3:
    mode: %s
    accessKeyId: %s
    secretAccessKey: %s
  gcs:
    mode: %s
    project: %s
  dynamodb:
    mode: %s
    accessKeyId: %s
    secretAccessKey: %s

storage:
  path: %s

services:
  mail:
    enabled: %t
    maxMessageBytes: %d
  s3:
    enabled: %t
    region: %s
    pathStyle: %t
    virtualHostStyle: %t
    maxObjectBytes: %d
    multipart:
      minPartBytes: %d
  gcs:
    enabled: %t
    project: %s
    location: %s
  dynamodb:
    enabled: %t
    region: %s
    billingMode: %s
    maxItemBytes: %d
    maxTables: %d
    streams:
      enabled: %t
    ttl:
      schedulerIntervalSeconds: %d
`, cfg.Project, cfg.Server.SMTPPort, cfg.Server.DashboardPort, cfg.Server.S3Port, cfg.Server.GCSPort, cfg.Server.DynamoDBPort, cfg.Auth.SMTP.Mode, cfg.Auth.S3.Mode, cfg.Auth.S3.AccessKeyID, cfg.Auth.S3.SecretAccessKey, cfg.Auth.GCS.Mode, cfg.Auth.GCS.Project, cfg.Auth.DynamoDB.Mode, cfg.Auth.DynamoDB.AccessKeyID, cfg.Auth.DynamoDB.SecretAccessKey, cfg.Storage.Path, cfg.Services.Mail.Enabled, cfg.Services.Mail.MaxMessageBytes, cfg.Services.S3.Enabled, cfg.Services.S3.Region, cfg.Services.S3.PathStyle, cfg.Services.S3.VirtualHostStyle, cfg.Services.S3.MaxObjectBytes, cfg.Services.S3.Multipart.MinPartBytes, cfg.Services.GCS.Enabled, cfg.Services.GCS.Project, cfg.Services.GCS.Location, cfg.Services.DynamoDB.Enabled, cfg.Services.DynamoDB.Region, cfg.Services.DynamoDB.BillingMode, cfg.Services.DynamoDB.MaxItemBytes, cfg.Services.DynamoDB.MaxTables, cfg.Services.DynamoDB.Streams.Enabled, cfg.Services.DynamoDB.TTL.SchedulerIntervalSeconds)
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
	case "server.s3Port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.s3Port: %w", err)
		}
		cfg.Server.S3Port = port
	case "server.gcsPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.gcsPort: %w", err)
		}
		cfg.Server.GCSPort = port
	case "server.dynamodbPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.dynamodbPort: %w", err)
		}
		cfg.Server.DynamoDBPort = port
	case "auth.smtp.mode":
		cfg.Auth.SMTP.Mode = value
	case "auth.s3.mode":
		cfg.Auth.S3.Mode = value
	case "auth.s3.accessKeyId":
		cfg.Auth.S3.AccessKeyID = value
	case "auth.s3.secretAccessKey":
		cfg.Auth.S3.SecretAccessKey = value
	case "auth.gcs.mode":
		cfg.Auth.GCS.Mode = value
	case "auth.gcs.project":
		cfg.Auth.GCS.Project = value
	case "auth.gcs.bearerToken":
		cfg.Auth.GCS.BearerToken = value
	case "auth.dynamodb.mode":
		cfg.Auth.DynamoDB.Mode = value
	case "auth.dynamodb.accessKeyId":
		cfg.Auth.DynamoDB.AccessKeyID = value
	case "auth.dynamodb.secretAccessKey":
		cfg.Auth.DynamoDB.SecretAccessKey = value
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
	case "services.s3.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.s3.enabled: %w", err)
		}
		cfg.Services.S3.Enabled = enabled
	case "services.s3.region":
		cfg.Services.S3.Region = value
	case "services.s3.pathStyle":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.s3.pathStyle: %w", err)
		}
		cfg.Services.S3.PathStyle = enabled
	case "services.s3.virtualHostStyle":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.s3.virtualHostStyle: %w", err)
		}
		cfg.Services.S3.VirtualHostStyle = enabled
	case "services.s3.maxObjectBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.s3.maxObjectBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.s3.maxObjectBytes: must be positive")
		}
		cfg.Services.S3.MaxObjectBytes = maxBytes
	case "services.s3.multipart.minPartBytes":
		minBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.s3.multipart.minPartBytes: %w", err)
		}
		if minBytes <= 0 {
			return fmt.Errorf("parse services.s3.multipart.minPartBytes: must be positive")
		}
		cfg.Services.S3.Multipart.MinPartBytes = minBytes
	case "services.gcs.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.gcs.enabled: %w", err)
		}
		cfg.Services.GCS.Enabled = enabled
	case "services.gcs.project":
		cfg.Services.GCS.Project = value
	case "services.gcs.location":
		cfg.Services.GCS.Location = value
	case "services.dynamodb.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.dynamodb.enabled: %w", err)
		}
		cfg.Services.DynamoDB.Enabled = enabled
	case "services.dynamodb.region":
		cfg.Services.DynamoDB.Region = value
	case "services.dynamodb.billingMode":
		cfg.Services.DynamoDB.BillingMode = value
	case "services.dynamodb.maxItemBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.dynamodb.maxItemBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.dynamodb.maxItemBytes: must be positive")
		}
		cfg.Services.DynamoDB.MaxItemBytes = maxBytes
	case "services.dynamodb.maxTables":
		maxTables, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.dynamodb.maxTables: %w", err)
		}
		if maxTables <= 0 {
			return fmt.Errorf("parse services.dynamodb.maxTables: must be positive")
		}
		cfg.Services.DynamoDB.MaxTables = maxTables
	case "services.dynamodb.streams.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.dynamodb.streams.enabled: %w", err)
		}
		cfg.Services.DynamoDB.Streams.Enabled = enabled
	case "services.dynamodb.ttl.schedulerIntervalSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.dynamodb.ttl.schedulerIntervalSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.dynamodb.ttl.schedulerIntervalSeconds: must be positive")
		}
		cfg.Services.DynamoDB.TTL.SchedulerIntervalSeconds = seconds
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
