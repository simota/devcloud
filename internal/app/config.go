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
	SMTPPort        int
	DashboardPort   int
	S3Port          int
	GCSPort         int
	DynamoDBPort    int
	BigQueryPort    int
	RedshiftPort    int
	RedshiftAPIPort int
	RedisPort       int
	SQSPort         int
	PubSubGRPCPort  int
	PubSubRESTPort  int
}

type AuthConfig struct {
	SMTP     SMTPAuthConfig
	S3       S3AuthConfig
	GCS      GCSAuthConfig
	DynamoDB DynamoDBAuthConfig
	BigQuery BigQueryAuthConfig
	Redshift RedshiftAuthConfig
	Redis    RedisAuthConfig
	SQS      SQSAuthConfig
	PubSub   PubSubAuthConfig
}

type SMTPAuthConfig struct {
	Mode     string
	Username string
	Password string
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

type BigQueryAuthConfig struct {
	Mode        string
	Project     string
	BearerToken string
}

type RedshiftAuthConfig struct {
	Mode            string
	User            string
	Password        string
	AccessKeyID     string
	SecretAccessKey string
	AccountID       string
}

type RedisAuthConfig struct {
	Mode     string
	Password string
}

type SQSAuthConfig struct {
	Mode            string
	AccessKeyID     string
	SecretAccessKey string
	AccountID       string
}

type PubSubAuthConfig struct {
	Mode        string
	ProjectID   string
	BearerToken string
}

type StorageConfig struct {
	Path string
}

type ServicesConfig struct {
	Mail     MailServiceConfig
	S3       S3ServiceConfig
	GCS      GCSServiceConfig
	DynamoDB DynamoDBServiceConfig
	BigQuery BigQueryServiceConfig
	Redshift RedshiftServiceConfig
	Redis    RedisServiceConfig
	SQS      SQSServiceConfig
	PubSub   PubSubServiceConfig
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

type BigQueryServiceConfig struct {
	Enabled         bool
	Project         string
	Location        string
	MaxRowsPerTable int64
	MaxRequestBytes int64
	Query           BigQueryQueryConfig
}

type BigQueryQueryConfig struct {
	MaxResultRows       int
	MaxExecutionSeconds int
	DefaultUseLegacySQL bool
}

type RedshiftServiceConfig struct {
	Enabled           bool
	Region            string
	ClusterIdentifier string
	Database          string
	DataDir           string
	NodeType          string
	NumberOfNodes     int
	MaxStatementBytes int64
	Backend           RedshiftBackendConfig
	DataAPI           RedshiftDataAPIConfig
	SQL               RedshiftSQLConfig
	CopyUnload        RedshiftCopyUnloadConfig
}

type RedshiftBackendConfig struct {
	Kind        string
	Mode        string
	ExternalDSN string
	Managed     bool
}

type RedshiftDataAPIConfig struct {
	Enabled                   bool
	MaxResultBytes            int64
	MaxResultRows             int
	StatementRetentionSeconds int
	SessionRetentionSeconds   int
}

type RedshiftSQLConfig struct {
	EnableExtendedProtocol bool
	MaxResultRows          int
	DefaultSearchPath      string
}

type RedshiftCopyUnloadConfig struct {
	EnableLocalS3    bool
	MaxInputRowBytes int64
}

type RedisServiceConfig struct {
	Enabled     bool
	Mode        string
	BinaryPath  string
	ExternalURL string
	DataDir     string
	MaxMemoryMB int
	AppendOnly  bool
}

type SQSServiceConfig struct {
	Enabled                         bool
	Region                          string
	QueueURLHost                    string
	MaxQueues                       int
	MaxMessageBytes                 int64
	MaxReceiveBatchSize             int
	DefaultVisibilityTimeoutSeconds int
	DefaultDelaySeconds             int
	DefaultMessageRetentionSeconds  int
	DefaultReceiveWaitTimeSeconds   int
	SchedulerIntervalSeconds        int
}

type PubSubServiceConfig struct {
	Enabled                   bool
	Project                   string
	DataDir                   string
	MessageDataDir            string
	DefaultAckDeadlineSeconds int
	MessageRetentionSeconds   int
	MaxAckDeadlineSeconds     int
	MaxPullMessages           int
	PullWaitTimeoutSeconds    int
	EnableREST                bool
	EnableStreamingPull       bool
	EnablePush                bool
}

type S3MultipartConfig struct {
	MinPartBytes int64
}

func DefaultConfig() Config {
	return Config{
		Project: "dev",
		Server: ServerConfig{
			SMTPPort:        1025,
			DashboardPort:   8025,
			S3Port:          4566,
			GCSPort:         4443,
			DynamoDBPort:    8000,
			BigQueryPort:    9050,
			RedshiftPort:    5439,
			RedshiftAPIPort: 9099,
			RedisPort:       6379,
			SQSPort:         9324,
			PubSubGRPCPort:  8085,
			PubSubRESTPort:  8086,
		},
		Auth: AuthConfig{
			SMTP: SMTPAuthConfig{Mode: "relaxed", Username: "dev", Password: "dev"},
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
			BigQuery: BigQueryAuthConfig{
				Mode:        "relaxed",
				Project:     "devcloud",
				BearerToken: "dev",
			},
			Redshift: RedshiftAuthConfig{
				Mode:            "relaxed",
				User:            "dev",
				Password:        "dev",
				AccessKeyID:     "dev",
				SecretAccessKey: "dev",
				AccountID:       "000000000000",
			},
			Redis: RedisAuthConfig{
				Mode: "relaxed",
			},
			SQS: SQSAuthConfig{
				Mode:            "relaxed",
				AccessKeyID:     "dev",
				SecretAccessKey: "dev",
				AccountID:       "000000000000",
			},
			PubSub: PubSubAuthConfig{
				Mode:        "relaxed",
				ProjectID:   "devcloud",
				BearerToken: "dev",
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
			BigQuery: BigQueryServiceConfig{
				Enabled:         true,
				Project:         "devcloud",
				Location:        "US",
				MaxRowsPerTable: 1000000,
				MaxRequestBytes: 10 * 1024 * 1024,
				Query: BigQueryQueryConfig{
					MaxResultRows:       10000,
					MaxExecutionSeconds: 30,
					DefaultUseLegacySQL: false,
				},
			},
			Redshift: RedshiftServiceConfig{
				Enabled:           true,
				Region:            "us-east-1",
				ClusterIdentifier: "devcloud",
				Database:          "dev",
				DataDir:           "redshift",
				NodeType:          "dc2.large",
				NumberOfNodes:     1,
				MaxStatementBytes: 16 * 1024 * 1024,
				Backend: RedshiftBackendConfig{
					Kind:        "postgres",
					Mode:        "managed",
					ExternalDSN: "",
					Managed:     true,
				},
				DataAPI: RedshiftDataAPIConfig{
					Enabled:                   true,
					MaxResultBytes:            500 * 1024 * 1024,
					MaxResultRows:             10000,
					StatementRetentionSeconds: 86400,
					SessionRetentionSeconds:   86400,
				},
				SQL: RedshiftSQLConfig{
					EnableExtendedProtocol: false,
					MaxResultRows:          10000,
					DefaultSearchPath:      "public",
				},
				CopyUnload: RedshiftCopyUnloadConfig{
					EnableLocalS3:    true,
					MaxInputRowBytes: 4 * 1024 * 1024,
				},
			},
			Redis: RedisServiceConfig{
				Enabled:     false,
				Mode:        "managed",
				DataDir:     "redis",
				MaxMemoryMB: 256,
				AppendOnly:  false,
			},
			SQS: SQSServiceConfig{
				Enabled:                         true,
				Region:                          "us-east-1",
				QueueURLHost:                    "127.0.0.1",
				MaxQueues:                       256,
				MaxMessageBytes:                 1024 * 1024,
				MaxReceiveBatchSize:             10,
				DefaultVisibilityTimeoutSeconds: 30,
				DefaultDelaySeconds:             0,
				DefaultMessageRetentionSeconds:  345600,
				DefaultReceiveWaitTimeSeconds:   0,
				SchedulerIntervalSeconds:        1,
			},
			PubSub: PubSubServiceConfig{
				Enabled:                   true,
				Project:                   "devcloud",
				DataDir:                   "",
				MessageDataDir:            "",
				DefaultAckDeadlineSeconds: 10,
				MessageRetentionSeconds:   604800,
				MaxAckDeadlineSeconds:     600,
				MaxPullMessages:           1000,
				PullWaitTimeoutSeconds:    1,
				EnableREST:                true,
				EnableStreamingPull:       true,
				EnablePush:                false,
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
	if cfg.Services.PubSub.DataDir != "" {
		if err := validateStoragePath(cfg.Services.PubSub.DataDir); err != nil {
			return fmt.Errorf("pubsub dataDir: %w", err)
		}
	}
	if cfg.Services.PubSub.MessageDataDir != "" {
		if err := validateStoragePath(cfg.Services.PubSub.MessageDataDir); err != nil {
			return fmt.Errorf("pubsub messageDataDir: %w", err)
		}
	}
	if err := validateStoragePath(redshiftDataDir(cfg)); err != nil {
		return fmt.Errorf("redshift dataDir: %w", err)
	}
	if err := validateStoragePath(redisDataDir(cfg)); err != nil {
		return fmt.Errorf("redis dataDir: %w", err)
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
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "bigquery"), 0o755); err != nil {
		return fmt.Errorf("create bigquery storage: %w", err)
	}
	if err := os.MkdirAll(redshiftDataDir(cfg), 0o755); err != nil {
		return fmt.Errorf("create redshift storage: %w", err)
	}
	if err := os.MkdirAll(redisDataDir(cfg), 0o755); err != nil {
		return fmt.Errorf("create redis storage: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Storage.Path, "sqs"), 0o755); err != nil {
		return fmt.Errorf("create sqs storage: %w", err)
	}
	if err := os.MkdirAll(pubsubDataDir(cfg), 0o755); err != nil {
		return fmt.Errorf("create pubsub storage: %w", err)
	}
	if err := os.MkdirAll(pubsubMessageDataDir(cfg), 0o755); err != nil {
		return fmt.Errorf("create message storage: %w", err)
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
	if cfg.Services.PubSub.DataDir != "" {
		if err := validateStoragePath(cfg.Services.PubSub.DataDir); err != nil {
			return fmt.Errorf("pubsub dataDir: %w", err)
		}
	}
	if cfg.Services.PubSub.MessageDataDir != "" {
		if err := validateStoragePath(cfg.Services.PubSub.MessageDataDir); err != nil {
			return fmt.Errorf("pubsub messageDataDir: %w", err)
		}
	}
	if err := validateStoragePath(redshiftDataDir(cfg)); err != nil {
		return fmt.Errorf("redshift dataDir: %w", err)
	}
	if err := validateStoragePath(redisDataDir(cfg)); err != nil {
		return fmt.Errorf("redis dataDir: %w", err)
	}
	if err := os.RemoveAll(cfg.Storage.Path); err != nil {
		return fmt.Errorf("remove storage: %w", err)
	}
	if cfg.Services.Redshift.DataDir != "" {
		if err := os.RemoveAll(redshiftDataDir(cfg)); err != nil {
			return fmt.Errorf("remove redshift storage: %w", err)
		}
	}
	if cfg.Services.Redis.DataDir != "" {
		if err := os.RemoveAll(redisDataDir(cfg)); err != nil {
			return fmt.Errorf("remove redis storage: %w", err)
		}
	}
	if cfg.Services.PubSub.DataDir != "" {
		if err := os.RemoveAll(pubsubDataDir(cfg)); err != nil {
			return fmt.Errorf("remove pubsub storage: %w", err)
		}
	}
	if cfg.Services.PubSub.MessageDataDir != "" {
		if err := os.RemoveAll(pubsubMessageDataDir(cfg)); err != nil {
			return fmt.Errorf("remove pubsub message storage: %w", err)
		}
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
  bigqueryPort: %d
  redshiftPort: %d
  redshiftAPIPort: %d
  redisPort: %d
  sqsPort: %d
  pubsubGrpcPort: %d
  pubsubRestPort: %d

auth:
  smtp:
    mode: %s
    user: %s
    password: %s
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
  bigquery:
    mode: %s
    project: %s
    bearerToken: %s
  redshift:
    mode: %s
    user: %s
    password: %s
    accessKeyId: %s
    secretAccessKey: %s
    accountId: "%s"
  redis:
    mode: %s
    password: %s
  sqs:
    mode: %s
    accessKeyId: %s
    secretAccessKey: %s
    accountId: "%s"
  pubsub:
    mode: %s
    projectID: %s
    bearerToken: %s

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
  bigquery:
    enabled: %t
    project: %s
    location: %s
    maxRowsPerTable: %d
    maxRequestBytes: %d
    query:
      maxResultRows: %d
      maxExecutionSeconds: %d
      defaultUseLegacySql: %t
  redshift:
    enabled: %t
    region: %s
    clusterIdentifier: %s
    database: %s
    dataDir: %s
    nodeType: %s
    numberOfNodes: %d
    maxStatementBytes: %d
    backend:
      kind: %s
      mode: %s
      externalDsn: %s
      managed: %t
    dataApi:
      enabled: %t
      maxResultBytes: %d
      maxResultRows: %d
      statementRetentionSeconds: %d
      sessionRetentionSeconds: %d
    sql:
      enableExtendedProtocol: %t
      maxResultRows: %d
      defaultSearchPath: %s
    copyUnload:
      enableLocalS3: %t
      maxInputRowBytes: %d
  redis:
    enabled: %t
    mode: %s
    binaryPath: %s
    externalUrl: %s
    dataDir: %s
    maxMemoryMB: %d
    appendOnly: %t
  sqs:
    enabled: %t
    region: %s
    queueUrlHost: %s
    maxQueues: %d
    maxMessageBytes: %d
    maxReceiveBatchSize: %d
    defaultVisibilityTimeoutSeconds: %d
    defaultDelaySeconds: %d
    defaultMessageRetentionSeconds: %d
    defaultReceiveWaitTimeSeconds: %d
    schedulerIntervalSeconds: %d
  pubsub:
    enabled: %t
    project: %s
    dataDir: %s
    messageDataDir: %s
    defaultAckDeadlineSeconds: %d
    messageRetentionSeconds: %d
    maxAckDeadlineSeconds: %d
    maxPullMessages: %d
    pullWaitTimeoutSeconds: %d
    enableREST: %t
    enableStreamingPull: %t
    enablePush: %t
`, cfg.Project, cfg.Server.SMTPPort, cfg.Server.DashboardPort, cfg.Server.S3Port, cfg.Server.GCSPort, cfg.Server.DynamoDBPort, cfg.Server.BigQueryPort, cfg.Server.RedshiftPort, cfg.Server.RedshiftAPIPort, cfg.Server.RedisPort, cfg.Server.SQSPort, cfg.Server.PubSubGRPCPort, cfg.Server.PubSubRESTPort, cfg.Auth.SMTP.Mode, cfg.Auth.SMTP.Username, cfg.Auth.SMTP.Password, cfg.Auth.S3.Mode, cfg.Auth.S3.AccessKeyID, cfg.Auth.S3.SecretAccessKey, cfg.Auth.GCS.Mode, cfg.Auth.GCS.Project, cfg.Auth.DynamoDB.Mode, cfg.Auth.DynamoDB.AccessKeyID, cfg.Auth.DynamoDB.SecretAccessKey, cfg.Auth.BigQuery.Mode, cfg.Auth.BigQuery.Project, cfg.Auth.BigQuery.BearerToken, cfg.Auth.Redshift.Mode, cfg.Auth.Redshift.User, cfg.Auth.Redshift.Password, cfg.Auth.Redshift.AccessKeyID, cfg.Auth.Redshift.SecretAccessKey, cfg.Auth.Redshift.AccountID, cfg.Auth.Redis.Mode, cfg.Auth.Redis.Password, cfg.Auth.SQS.Mode, cfg.Auth.SQS.AccessKeyID, cfg.Auth.SQS.SecretAccessKey, cfg.Auth.SQS.AccountID, cfg.Auth.PubSub.Mode, cfg.Auth.PubSub.ProjectID, cfg.Auth.PubSub.BearerToken, cfg.Storage.Path, cfg.Services.Mail.Enabled, cfg.Services.Mail.MaxMessageBytes, cfg.Services.S3.Enabled, cfg.Services.S3.Region, cfg.Services.S3.PathStyle, cfg.Services.S3.VirtualHostStyle, cfg.Services.S3.MaxObjectBytes, cfg.Services.S3.Multipart.MinPartBytes, cfg.Services.GCS.Enabled, cfg.Services.GCS.Project, cfg.Services.GCS.Location, cfg.Services.DynamoDB.Enabled, cfg.Services.DynamoDB.Region, cfg.Services.DynamoDB.BillingMode, cfg.Services.DynamoDB.MaxItemBytes, cfg.Services.DynamoDB.MaxTables, cfg.Services.DynamoDB.Streams.Enabled, cfg.Services.DynamoDB.TTL.SchedulerIntervalSeconds, cfg.Services.BigQuery.Enabled, cfg.Services.BigQuery.Project, cfg.Services.BigQuery.Location, cfg.Services.BigQuery.MaxRowsPerTable, cfg.Services.BigQuery.MaxRequestBytes, cfg.Services.BigQuery.Query.MaxResultRows, cfg.Services.BigQuery.Query.MaxExecutionSeconds, cfg.Services.BigQuery.Query.DefaultUseLegacySQL, cfg.Services.Redshift.Enabled, cfg.Services.Redshift.Region, cfg.Services.Redshift.ClusterIdentifier, cfg.Services.Redshift.Database, defaultString(cfg.Services.Redshift.DataDir, "redshift"), cfg.Services.Redshift.NodeType, cfg.Services.Redshift.NumberOfNodes, cfg.Services.Redshift.MaxStatementBytes, redshiftBackendKind(cfg.Services.Redshift.Backend), redshiftBackendMode(cfg.Services.Redshift.Backend), cfg.Services.Redshift.Backend.ExternalDSN, redshiftBackendMode(cfg.Services.Redshift.Backend) == "managed", cfg.Services.Redshift.DataAPI.Enabled, cfg.Services.Redshift.DataAPI.MaxResultBytes, cfg.Services.Redshift.DataAPI.MaxResultRows, cfg.Services.Redshift.DataAPI.StatementRetentionSeconds, cfg.Services.Redshift.DataAPI.SessionRetentionSeconds, cfg.Services.Redshift.SQL.EnableExtendedProtocol, cfg.Services.Redshift.SQL.MaxResultRows, cfg.Services.Redshift.SQL.DefaultSearchPath, cfg.Services.Redshift.CopyUnload.EnableLocalS3, cfg.Services.Redshift.CopyUnload.MaxInputRowBytes, cfg.Services.Redis.Enabled, redisMode(cfg.Services.Redis), cfg.Services.Redis.BinaryPath, cfg.Services.Redis.ExternalURL, defaultString(cfg.Services.Redis.DataDir, "redis"), cfg.Services.Redis.MaxMemoryMB, cfg.Services.Redis.AppendOnly, cfg.Services.SQS.Enabled, cfg.Services.SQS.Region, cfg.Services.SQS.QueueURLHost, cfg.Services.SQS.MaxQueues, cfg.Services.SQS.MaxMessageBytes, cfg.Services.SQS.MaxReceiveBatchSize, cfg.Services.SQS.DefaultVisibilityTimeoutSeconds, cfg.Services.SQS.DefaultDelaySeconds, cfg.Services.SQS.DefaultMessageRetentionSeconds, cfg.Services.SQS.DefaultReceiveWaitTimeSeconds, cfg.Services.SQS.SchedulerIntervalSeconds, cfg.Services.PubSub.Enabled, cfg.Services.PubSub.Project, defaultString(cfg.Services.PubSub.DataDir, filepath.Join(cfg.Storage.Path, "pubsub")), defaultString(cfg.Services.PubSub.MessageDataDir, filepath.Join(cfg.Storage.Path, "message")), cfg.Services.PubSub.DefaultAckDeadlineSeconds, cfg.Services.PubSub.MessageRetentionSeconds, cfg.Services.PubSub.MaxAckDeadlineSeconds, cfg.Services.PubSub.MaxPullMessages, cfg.Services.PubSub.PullWaitTimeoutSeconds, cfg.Services.PubSub.EnableREST, cfg.Services.PubSub.EnableStreamingPull, cfg.Services.PubSub.EnablePush)
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
	case "server.bigqueryPort", "server.bigQueryPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.bigQueryPort: %w", err)
		}
		cfg.Server.BigQueryPort = port
	case "server.redshiftPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.redshiftPort: %w", err)
		}
		cfg.Server.RedshiftPort = port
	case "server.redshiftAPIPort", "server.redshiftApiPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.redshiftAPIPort: %w", err)
		}
		cfg.Server.RedshiftAPIPort = port
	case "server.redisPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.redisPort: %w", err)
		}
		cfg.Server.RedisPort = port
	case "server.sqsPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.sqsPort: %w", err)
		}
		cfg.Server.SQSPort = port
	case "server.pubsubGrpcPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.pubsubGrpcPort: %w", err)
		}
		cfg.Server.PubSubGRPCPort = port
	case "server.pubsubRestPort":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse server.pubsubRestPort: %w", err)
		}
		cfg.Server.PubSubRESTPort = port
	case "auth.smtp.mode":
		cfg.Auth.SMTP.Mode = value
	case "auth.smtp.user":
		cfg.Auth.SMTP.Username = value
	case "auth.smtp.password":
		cfg.Auth.SMTP.Password = value
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
	case "auth.bigquery.mode":
		cfg.Auth.BigQuery.Mode = value
	case "auth.bigquery.project":
		cfg.Auth.BigQuery.Project = value
	case "auth.bigquery.bearerToken":
		cfg.Auth.BigQuery.BearerToken = value
	case "auth.redshift.mode":
		cfg.Auth.Redshift.Mode = value
	case "auth.redshift.user":
		cfg.Auth.Redshift.User = value
	case "auth.redshift.password":
		cfg.Auth.Redshift.Password = value
	case "auth.redshift.accessKeyId":
		cfg.Auth.Redshift.AccessKeyID = value
	case "auth.redshift.secretAccessKey":
		cfg.Auth.Redshift.SecretAccessKey = value
	case "auth.redshift.accountId":
		cfg.Auth.Redshift.AccountID = strings.Trim(value, `"`)
	case "auth.redis.mode":
		cfg.Auth.Redis.Mode = value
	case "auth.redis.password":
		cfg.Auth.Redis.Password = value
	case "auth.sqs.mode":
		cfg.Auth.SQS.Mode = value
	case "auth.sqs.accessKeyId":
		cfg.Auth.SQS.AccessKeyID = value
	case "auth.sqs.secretAccessKey":
		cfg.Auth.SQS.SecretAccessKey = value
	case "auth.sqs.accountId":
		cfg.Auth.SQS.AccountID = strings.Trim(value, `"`)
	case "auth.pubsub.mode":
		cfg.Auth.PubSub.Mode = value
	case "auth.pubsub.projectID", "auth.pubsub.projectId":
		cfg.Auth.PubSub.ProjectID = value
	case "auth.pubsub.bearerToken":
		cfg.Auth.PubSub.BearerToken = value
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
	case "services.bigquery.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.enabled: %w", err)
		}
		cfg.Services.BigQuery.Enabled = enabled
	case "services.bigquery.project":
		cfg.Services.BigQuery.Project = value
	case "services.bigquery.location":
		cfg.Services.BigQuery.Location = value
	case "services.bigquery.maxRowsPerTable":
		maxRows, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.maxRowsPerTable: %w", err)
		}
		if maxRows <= 0 {
			return fmt.Errorf("parse services.bigquery.maxRowsPerTable: must be positive")
		}
		cfg.Services.BigQuery.MaxRowsPerTable = maxRows
	case "services.bigquery.maxRequestBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.maxRequestBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.bigquery.maxRequestBytes: must be positive")
		}
		cfg.Services.BigQuery.MaxRequestBytes = maxBytes
	case "services.bigquery.query.maxResultRows":
		maxRows, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.query.maxResultRows: %w", err)
		}
		if maxRows <= 0 {
			return fmt.Errorf("parse services.bigquery.query.maxResultRows: must be positive")
		}
		cfg.Services.BigQuery.Query.MaxResultRows = maxRows
	case "services.bigquery.query.maxExecutionSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.query.maxExecutionSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.bigquery.query.maxExecutionSeconds: must be positive")
		}
		cfg.Services.BigQuery.Query.MaxExecutionSeconds = seconds
	case "services.bigquery.query.defaultUseLegacySql":
		useLegacySQL, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.bigquery.query.defaultUseLegacySql: %w", err)
		}
		cfg.Services.BigQuery.Query.DefaultUseLegacySQL = useLegacySQL
	case "services.redshift.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.enabled: %w", err)
		}
		cfg.Services.Redshift.Enabled = enabled
	case "services.redshift.region":
		cfg.Services.Redshift.Region = value
	case "services.redshift.clusterIdentifier":
		cfg.Services.Redshift.ClusterIdentifier = value
	case "services.redshift.database":
		cfg.Services.Redshift.Database = value
	case "services.redshift.dataDir":
		cfg.Services.Redshift.DataDir = value
	case "services.redshift.nodeType":
		cfg.Services.Redshift.NodeType = value
	case "services.redshift.numberOfNodes":
		nodes, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.numberOfNodes: %w", err)
		}
		if nodes <= 0 {
			return fmt.Errorf("parse services.redshift.numberOfNodes: must be positive")
		}
		cfg.Services.Redshift.NumberOfNodes = nodes
	case "services.redshift.maxStatementBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.redshift.maxStatementBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.redshift.maxStatementBytes: must be positive")
		}
		cfg.Services.Redshift.MaxStatementBytes = maxBytes
	case "services.redshift.backend.kind":
		cfg.Services.Redshift.Backend.Kind = value
	case "services.redshift.backend.mode":
		cfg.Services.Redshift.Backend.Mode = value
	case "services.redshift.backend.externalDsn", "services.redshift.backend.externalDSN", "services.redshift.backend.postgresDsn", "services.redshift.backend.postgresDSN":
		cfg.Services.Redshift.Backend.ExternalDSN = value
	case "services.redshift.backend.managed":
		managed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.backend.managed: %w", err)
		}
		cfg.Services.Redshift.Backend.Managed = managed
	case "services.redshift.dataApi.enabled", "services.redshift.dataAPI.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.dataApi.enabled: %w", err)
		}
		cfg.Services.Redshift.DataAPI.Enabled = enabled
	case "services.redshift.dataApi.maxResultBytes", "services.redshift.dataAPI.maxResultBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.redshift.dataApi.maxResultBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.redshift.dataApi.maxResultBytes: must be positive")
		}
		cfg.Services.Redshift.DataAPI.MaxResultBytes = maxBytes
	case "services.redshift.dataApi.maxResultRows", "services.redshift.dataAPI.maxResultRows":
		maxRows, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.dataApi.maxResultRows: %w", err)
		}
		if maxRows <= 0 {
			return fmt.Errorf("parse services.redshift.dataApi.maxResultRows: must be positive")
		}
		cfg.Services.Redshift.DataAPI.MaxResultRows = maxRows
	case "services.redshift.dataApi.statementRetentionSeconds", "services.redshift.dataAPI.statementRetentionSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.dataApi.statementRetentionSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.redshift.dataApi.statementRetentionSeconds: must be positive")
		}
		cfg.Services.Redshift.DataAPI.StatementRetentionSeconds = seconds
	case "services.redshift.dataApi.sessionRetentionSeconds", "services.redshift.dataAPI.sessionRetentionSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.dataApi.sessionRetentionSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.redshift.dataApi.sessionRetentionSeconds: must be positive")
		}
		cfg.Services.Redshift.DataAPI.SessionRetentionSeconds = seconds
	case "services.redshift.sql.enableExtendedProtocol":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.sql.enableExtendedProtocol: %w", err)
		}
		cfg.Services.Redshift.SQL.EnableExtendedProtocol = enabled
	case "services.redshift.sql.maxResultRows":
		maxRows, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.sql.maxResultRows: %w", err)
		}
		if maxRows <= 0 {
			return fmt.Errorf("parse services.redshift.sql.maxResultRows: must be positive")
		}
		cfg.Services.Redshift.SQL.MaxResultRows = maxRows
	case "services.redshift.sql.defaultSearchPath":
		cfg.Services.Redshift.SQL.DefaultSearchPath = value
	case "services.redshift.copyUnload.enableLocalS3":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redshift.copyUnload.enableLocalS3: %w", err)
		}
		cfg.Services.Redshift.CopyUnload.EnableLocalS3 = enabled
	case "services.redshift.copyUnload.maxInputRowBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.redshift.copyUnload.maxInputRowBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.redshift.copyUnload.maxInputRowBytes: must be positive")
		}
		cfg.Services.Redshift.CopyUnload.MaxInputRowBytes = maxBytes
	case "services.redis.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redis.enabled: %w", err)
		}
		cfg.Services.Redis.Enabled = enabled
	case "services.redis.mode":
		cfg.Services.Redis.Mode = value
	case "services.redis.binaryPath":
		cfg.Services.Redis.BinaryPath = value
	case "services.redis.externalUrl", "services.redis.externalURL":
		cfg.Services.Redis.ExternalURL = value
	case "services.redis.dataDir":
		cfg.Services.Redis.DataDir = value
	case "services.redis.maxMemoryMB":
		maxMemoryMB, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.redis.maxMemoryMB: %w", err)
		}
		if maxMemoryMB <= 0 {
			return fmt.Errorf("parse services.redis.maxMemoryMB: must be positive")
		}
		cfg.Services.Redis.MaxMemoryMB = maxMemoryMB
	case "services.redis.appendOnly":
		appendOnly, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.redis.appendOnly: %w", err)
		}
		cfg.Services.Redis.AppendOnly = appendOnly
	case "services.sqs.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.enabled: %w", err)
		}
		cfg.Services.SQS.Enabled = enabled
	case "services.sqs.region":
		cfg.Services.SQS.Region = value
	case "services.sqs.queueUrlHost":
		cfg.Services.SQS.QueueURLHost = value
	case "services.sqs.maxQueues":
		maxQueues, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.maxQueues: %w", err)
		}
		if maxQueues <= 0 {
			return fmt.Errorf("parse services.sqs.maxQueues: must be positive")
		}
		cfg.Services.SQS.MaxQueues = maxQueues
	case "services.sqs.maxMessageBytes":
		maxBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("parse services.sqs.maxMessageBytes: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("parse services.sqs.maxMessageBytes: must be positive")
		}
		cfg.Services.SQS.MaxMessageBytes = maxBytes
	case "services.sqs.maxReceiveBatchSize":
		maxBatch, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.maxReceiveBatchSize: %w", err)
		}
		if maxBatch <= 0 {
			return fmt.Errorf("parse services.sqs.maxReceiveBatchSize: must be positive")
		}
		cfg.Services.SQS.MaxReceiveBatchSize = maxBatch
	case "services.sqs.defaultVisibilityTimeoutSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.defaultVisibilityTimeoutSeconds: %w", err)
		}
		if seconds < 0 {
			return fmt.Errorf("parse services.sqs.defaultVisibilityTimeoutSeconds: must be non-negative")
		}
		cfg.Services.SQS.DefaultVisibilityTimeoutSeconds = seconds
	case "services.sqs.defaultDelaySeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.defaultDelaySeconds: %w", err)
		}
		if seconds < 0 {
			return fmt.Errorf("parse services.sqs.defaultDelaySeconds: must be non-negative")
		}
		cfg.Services.SQS.DefaultDelaySeconds = seconds
	case "services.sqs.defaultMessageRetentionSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.defaultMessageRetentionSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.sqs.defaultMessageRetentionSeconds: must be positive")
		}
		cfg.Services.SQS.DefaultMessageRetentionSeconds = seconds
	case "services.sqs.defaultReceiveWaitTimeSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.defaultReceiveWaitTimeSeconds: %w", err)
		}
		if seconds < 0 {
			return fmt.Errorf("parse services.sqs.defaultReceiveWaitTimeSeconds: must be non-negative")
		}
		cfg.Services.SQS.DefaultReceiveWaitTimeSeconds = seconds
	case "services.sqs.schedulerIntervalSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.sqs.schedulerIntervalSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.sqs.schedulerIntervalSeconds: must be positive")
		}
		cfg.Services.SQS.SchedulerIntervalSeconds = seconds
	case "services.pubsub.enabled":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.enabled: %w", err)
		}
		cfg.Services.PubSub.Enabled = enabled
	case "services.pubsub.project":
		cfg.Services.PubSub.Project = value
	case "services.pubsub.dataDir":
		cfg.Services.PubSub.DataDir = value
	case "services.pubsub.messageDataDir":
		cfg.Services.PubSub.MessageDataDir = value
	case "services.pubsub.defaultAckDeadlineSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.defaultAckDeadlineSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.pubsub.defaultAckDeadlineSeconds: must be positive")
		}
		cfg.Services.PubSub.DefaultAckDeadlineSeconds = seconds
	case "services.pubsub.messageRetentionSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.messageRetentionSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.pubsub.messageRetentionSeconds: must be positive")
		}
		cfg.Services.PubSub.MessageRetentionSeconds = seconds
	case "services.pubsub.maxAckDeadlineSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.maxAckDeadlineSeconds: %w", err)
		}
		if seconds <= 0 {
			return fmt.Errorf("parse services.pubsub.maxAckDeadlineSeconds: must be positive")
		}
		cfg.Services.PubSub.MaxAckDeadlineSeconds = seconds
	case "services.pubsub.maxPullMessages":
		maxMessages, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.maxPullMessages: %w", err)
		}
		if maxMessages <= 0 {
			return fmt.Errorf("parse services.pubsub.maxPullMessages: must be positive")
		}
		cfg.Services.PubSub.MaxPullMessages = maxMessages
	case "services.pubsub.pullWaitTimeoutSeconds":
		seconds, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.pullWaitTimeoutSeconds: %w", err)
		}
		if seconds < 0 {
			return fmt.Errorf("parse services.pubsub.pullWaitTimeoutSeconds: must be non-negative")
		}
		cfg.Services.PubSub.PullWaitTimeoutSeconds = seconds
	case "services.pubsub.enableREST":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.enableREST: %w", err)
		}
		cfg.Services.PubSub.EnableREST = enabled
	case "services.pubsub.enableStreamingPull":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.enableStreamingPull: %w", err)
		}
		cfg.Services.PubSub.EnableStreamingPull = enabled
	case "services.pubsub.enablePush":
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse services.pubsub.enablePush: %w", err)
		}
		cfg.Services.PubSub.EnablePush = enabled
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
