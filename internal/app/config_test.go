package app

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInitWorkspaceCreatesDefaultsWithoutOverwritingConfig(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()

	if err := InitWorkspace(cfg); err != nil {
		t.Fatalf("InitWorkspace() error = %v", err)
	}

	configPath := filepath.Join(".devcloud", "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("config not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "mail", "index.json")); err != nil {
		t.Fatalf("mail index not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "s3", "buckets")); err != nil {
		t.Fatalf("s3 bucket storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "s3", "multipart")); err != nil {
		t.Fatalf("s3 multipart storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "gcs", "upload_sessions")); err != nil {
		t.Fatalf("gcs upload session storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "dynamodb")); err != nil {
		t.Fatalf("dynamodb storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "bigquery")); err != nil {
		t.Fatalf("bigquery storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "sqs")); err != nil {
		t.Fatalf("sqs storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "pubsub")); err != nil {
		t.Fatalf("pubsub storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "message")); err != nil {
		t.Fatalf("message storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "kv")); err != nil {
		t.Fatalf("kv storage not created: %v", err)
	}

	custom := []byte("project: custom\n")
	if err := os.WriteFile(configPath, custom, 0o644); err != nil {
		t.Fatalf("write custom config: %v", err)
	}
	if err := InitWorkspace(cfg); err != nil {
		t.Fatalf("second InitWorkspace() error = %v", err)
	}
	got, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(got) != string(custom) {
		t.Fatalf("config overwritten: got %q", string(got))
	}
}

func TestLoadConfigReadsGeneratedConfigValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := []byte(`project: custom

server:
  smtpPort: 2525
  dashboardPort: 8825
  s3Port: 4567
  gcsPort: 4444
  dynamodbPort: 8100
  bigqueryPort: 9051
  sqsPort: 9325
  pubsubGrpcPort: 18085
  pubsubRestPort: 18086

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: local
    secretAccessKey: secret
  gcs:
    mode: bearer-dev
    project: custom-gcs-project
    bearerToken: local-token
  dynamodb:
    mode: strict
    accessKeyId: local-dynamo
    secretAccessKey: dynamo-secret
  bigquery:
    mode: bearer-dev
    project: custom-bigquery-project
    bearerToken: bigquery-token
  sqs:
    mode: strict
    accessKeyId: local-sqs
    secretAccessKey: sqs-secret
    accountId: "123456789012"
  pubsub:
    mode: bearer-dev
    projectID: custom-pubsub-project
    bearerToken: pubsub-token

storage:
  path: .devcloud/custom-data

services:
  mail:
    enabled: true
    maxMessageBytes: 512
  s3:
    enabled: true
    region: ap-northeast-1
    pathStyle: true
    virtualHostStyle: true
    maxObjectBytes: 1024
    multipart:
      minPartBytes: 128
  gcs:
    enabled: true
    project: custom-gcs-project
    location: ASIA-NORTHEAST1
  dynamodb:
    enabled: true
    region: ap-northeast-1
    billingMode: PAY_PER_REQUEST
    maxItemBytes: 2048
    maxTables: 32
    streams:
      enabled: true
    ttl:
      schedulerIntervalSeconds: 30
  bigquery:
    enabled: true
    project: custom-bigquery-project
    location: EU
    maxRowsPerTable: 12345
    maxRequestBytes: 4096
    query:
      maxResultRows: 500
      maxExecutionSeconds: 7
      defaultUseLegacySql: true
  sqs:
    enabled: true
    region: ap-northeast-1
    queueUrlHost: localhost
    maxQueues: 64
    maxMessageBytes: 2048
    maxReceiveBatchSize: 5
    defaultVisibilityTimeoutSeconds: 9
    defaultDelaySeconds: 1
    defaultMessageRetentionSeconds: 600
    defaultReceiveWaitTimeSeconds: 2
    schedulerIntervalSeconds: 3
  pubsub:
    enabled: true
    project: custom-pubsub-project
    dataDir: .devcloud/custom-pubsub
    messageDataDir: .devcloud/custom-message
    defaultAckDeadlineSeconds: 11
    messageRetentionSeconds: 700
    maxAckDeadlineSeconds: 120
    maxPullMessages: 50
    pullWaitTimeoutSeconds: 4
    enableREST: true
    enableStreamingPull: false
    enablePush: true
`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Project != "custom" {
		t.Fatalf("Project = %q", cfg.Project)
	}
	if cfg.Server.SMTPPort != 2525 || cfg.Server.DashboardPort != 8825 || cfg.Server.S3Port != 4567 || cfg.Server.GCSPort != 4444 || cfg.Server.DynamoDBPort != 8100 || cfg.Server.BigQueryPort != 9051 || cfg.Server.SQSPort != 9325 || cfg.Server.PubSubGRPCPort != 18085 || cfg.Server.PubSubRESTPort != 18086 {
		t.Fatalf("Server = %#v", cfg.Server)
	}
	if cfg.Auth.S3.AccessKeyID != "local" || cfg.Auth.S3.SecretAccessKey != "secret" {
		t.Fatalf("Auth.S3 = %#v", cfg.Auth.S3)
	}
	if cfg.Auth.GCS.Mode != "bearer-dev" || cfg.Auth.GCS.Project != "custom-gcs-project" || cfg.Auth.GCS.BearerToken != "local-token" {
		t.Fatalf("Auth.GCS = %#v", cfg.Auth.GCS)
	}
	if cfg.Auth.DynamoDB.Mode != "strict" || cfg.Auth.DynamoDB.AccessKeyID != "local-dynamo" || cfg.Auth.DynamoDB.SecretAccessKey != "dynamo-secret" {
		t.Fatalf("Auth.DynamoDB = %#v", cfg.Auth.DynamoDB)
	}
	if cfg.Auth.BigQuery.Mode != "bearer-dev" || cfg.Auth.BigQuery.Project != "custom-bigquery-project" || cfg.Auth.BigQuery.BearerToken != "bigquery-token" {
		t.Fatalf("Auth.BigQuery = %#v", cfg.Auth.BigQuery)
	}
	if cfg.Auth.SQS.Mode != "strict" || cfg.Auth.SQS.AccessKeyID != "local-sqs" || cfg.Auth.SQS.SecretAccessKey != "sqs-secret" || cfg.Auth.SQS.AccountID != "123456789012" {
		t.Fatalf("Auth.SQS = %#v", cfg.Auth.SQS)
	}
	if cfg.Auth.PubSub.Mode != "bearer-dev" || cfg.Auth.PubSub.ProjectID != "custom-pubsub-project" || cfg.Auth.PubSub.BearerToken != "pubsub-token" {
		t.Fatalf("Auth.PubSub = %#v", cfg.Auth.PubSub)
	}
	if cfg.Storage.Path != ".devcloud/custom-data" {
		t.Fatalf("Storage.Path = %q", cfg.Storage.Path)
	}
	if cfg.Services.Mail.MaxMessageBytes != 512 {
		t.Fatalf("MaxMessageBytes = %d", cfg.Services.Mail.MaxMessageBytes)
	}
	if cfg.Services.S3.Region != "ap-northeast-1" || !cfg.Services.S3.VirtualHostStyle {
		t.Fatalf("Services.S3 = %#v", cfg.Services.S3)
	}
	if cfg.Services.S3.MaxObjectBytes != 1024 || cfg.Services.S3.Multipart.MinPartBytes != 128 {
		t.Fatalf("Services.S3 sizing = %#v", cfg.Services.S3)
	}
	if !cfg.Services.GCS.Enabled || cfg.Services.GCS.Project != "custom-gcs-project" || cfg.Services.GCS.Location != "ASIA-NORTHEAST1" {
		t.Fatalf("Services.GCS = %#v", cfg.Services.GCS)
	}
	if !cfg.Services.DynamoDB.Enabled || cfg.Services.DynamoDB.Region != "ap-northeast-1" || cfg.Services.DynamoDB.BillingMode != "PAY_PER_REQUEST" {
		t.Fatalf("Services.DynamoDB = %#v", cfg.Services.DynamoDB)
	}
	if cfg.Services.DynamoDB.MaxItemBytes != 2048 || cfg.Services.DynamoDB.MaxTables != 32 || !cfg.Services.DynamoDB.Streams.Enabled || cfg.Services.DynamoDB.TTL.SchedulerIntervalSeconds != 30 {
		t.Fatalf("Services.DynamoDB limits = %#v", cfg.Services.DynamoDB)
	}
	if !cfg.Services.BigQuery.Enabled || cfg.Services.BigQuery.Project != "custom-bigquery-project" || cfg.Services.BigQuery.Location != "EU" {
		t.Fatalf("Services.BigQuery = %#v", cfg.Services.BigQuery)
	}
	if cfg.Services.BigQuery.MaxRowsPerTable != 12345 || cfg.Services.BigQuery.MaxRequestBytes != 4096 {
		t.Fatalf("Services.BigQuery limits = %#v", cfg.Services.BigQuery)
	}
	if cfg.Services.BigQuery.Query.MaxResultRows != 500 || cfg.Services.BigQuery.Query.MaxExecutionSeconds != 7 || !cfg.Services.BigQuery.Query.DefaultUseLegacySQL {
		t.Fatalf("Services.BigQuery.Query = %#v", cfg.Services.BigQuery.Query)
	}
	if !cfg.Services.SQS.Enabled || cfg.Services.SQS.Region != "ap-northeast-1" || cfg.Services.SQS.QueueURLHost != "localhost" {
		t.Fatalf("Services.SQS = %#v", cfg.Services.SQS)
	}
	if cfg.Services.SQS.MaxQueues != 64 || cfg.Services.SQS.MaxMessageBytes != 2048 || cfg.Services.SQS.MaxReceiveBatchSize != 5 {
		t.Fatalf("Services.SQS limits = %#v", cfg.Services.SQS)
	}
	if cfg.Services.SQS.DefaultVisibilityTimeoutSeconds != 9 || cfg.Services.SQS.DefaultDelaySeconds != 1 || cfg.Services.SQS.DefaultMessageRetentionSeconds != 600 || cfg.Services.SQS.DefaultReceiveWaitTimeSeconds != 2 || cfg.Services.SQS.SchedulerIntervalSeconds != 3 {
		t.Fatalf("Services.SQS timings = %#v", cfg.Services.SQS)
	}
	if !cfg.Services.PubSub.Enabled || cfg.Services.PubSub.Project != "custom-pubsub-project" {
		t.Fatalf("Services.PubSub = %#v", cfg.Services.PubSub)
	}
	if cfg.Services.PubSub.DataDir != ".devcloud/custom-pubsub" || cfg.Services.PubSub.MessageDataDir != ".devcloud/custom-message" {
		t.Fatalf("Services.PubSub dirs = %#v", cfg.Services.PubSub)
	}
	if cfg.Services.PubSub.DefaultAckDeadlineSeconds != 11 || cfg.Services.PubSub.MessageRetentionSeconds != 700 || cfg.Services.PubSub.MaxAckDeadlineSeconds != 120 || cfg.Services.PubSub.MaxPullMessages != 50 || cfg.Services.PubSub.PullWaitTimeoutSeconds != 4 {
		t.Fatalf("Services.PubSub limits = %#v", cfg.Services.PubSub)
	}
	if !cfg.Services.PubSub.EnableREST || cfg.Services.PubSub.EnableStreamingPull || !cfg.Services.PubSub.EnablePush {
		t.Fatalf("Services.PubSub feature flags = %#v", cfg.Services.PubSub)
	}
}

func TestLoadConfigAcceptsBigQueryPortAlias(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  bigQueryPort: 19050\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Server.BigQueryPort != 19050 {
		t.Fatalf("Server.BigQueryPort = %d", cfg.Server.BigQueryPort)
	}
}

func TestDefaultConfigIncludesS3GCSAndDynamoDBDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Server.S3Port != 4566 {
		t.Fatalf("Server.S3Port = %d", cfg.Server.S3Port)
	}
	if cfg.Server.GCSPort != 4443 {
		t.Fatalf("Server.GCSPort = %d", cfg.Server.GCSPort)
	}
	if cfg.Server.DynamoDBPort != 8000 {
		t.Fatalf("Server.DynamoDBPort = %d", cfg.Server.DynamoDBPort)
	}
	if cfg.Server.BigQueryPort != 9050 {
		t.Fatalf("Server.BigQueryPort = %d", cfg.Server.BigQueryPort)
	}
	if cfg.Server.SQSPort != 9324 {
		t.Fatalf("Server.SQSPort = %d", cfg.Server.SQSPort)
	}
	if cfg.Server.PubSubGRPCPort != 8085 {
		t.Fatalf("Server.PubSubGRPCPort = %d", cfg.Server.PubSubGRPCPort)
	}
	if cfg.Server.PubSubRESTPort != 8086 {
		t.Fatalf("Server.PubSubRESTPort = %d", cfg.Server.PubSubRESTPort)
	}
	if cfg.Auth.S3.Mode != "relaxed" || cfg.Auth.S3.AccessKeyID != "dev" || cfg.Auth.S3.SecretAccessKey != "dev" {
		t.Fatalf("Auth.S3 = %#v", cfg.Auth.S3)
	}
	if !cfg.Services.S3.Enabled || cfg.Services.S3.Region != "us-east-1" {
		t.Fatalf("Services.S3 = %#v", cfg.Services.S3)
	}
	if !cfg.Services.S3.PathStyle || cfg.Services.S3.VirtualHostStyle {
		t.Fatalf("S3 addressing defaults = %#v", cfg.Services.S3)
	}
	if cfg.Services.S3.MaxObjectBytes != 5*1024*1024*1024 {
		t.Fatalf("MaxObjectBytes = %d", cfg.Services.S3.MaxObjectBytes)
	}
	if cfg.Services.S3.Multipart.MinPartBytes != 5*1024*1024 {
		t.Fatalf("Multipart.MinPartBytes = %d", cfg.Services.S3.Multipart.MinPartBytes)
	}
	if cfg.Auth.GCS.Mode != "relaxed" || cfg.Auth.GCS.Project != "devcloud" {
		t.Fatalf("Auth.GCS = %#v", cfg.Auth.GCS)
	}
	if !cfg.Services.GCS.Enabled || cfg.Services.GCS.Project != "devcloud" || cfg.Services.GCS.Location != "US" {
		t.Fatalf("Services.GCS = %#v", cfg.Services.GCS)
	}
	if cfg.Auth.DynamoDB.Mode != "relaxed" || cfg.Auth.DynamoDB.AccessKeyID != "dev" || cfg.Auth.DynamoDB.SecretAccessKey != "dev" {
		t.Fatalf("Auth.DynamoDB = %#v", cfg.Auth.DynamoDB)
	}
	if !cfg.Services.DynamoDB.Enabled || cfg.Services.DynamoDB.Region != "us-east-1" || cfg.Services.DynamoDB.BillingMode != "PAY_PER_REQUEST" {
		t.Fatalf("Services.DynamoDB = %#v", cfg.Services.DynamoDB)
	}
	if cfg.Services.DynamoDB.MaxItemBytes != 400000 || cfg.Services.DynamoDB.MaxTables != 256 || cfg.Services.DynamoDB.Streams.Enabled || cfg.Services.DynamoDB.TTL.SchedulerIntervalSeconds != 60 {
		t.Fatalf("Services.DynamoDB limits = %#v", cfg.Services.DynamoDB)
	}
	if cfg.Auth.BigQuery.Mode != "relaxed" || cfg.Auth.BigQuery.Project != "devcloud" || cfg.Auth.BigQuery.BearerToken != "dev" {
		t.Fatalf("Auth.BigQuery = %#v", cfg.Auth.BigQuery)
	}
	if !cfg.Services.BigQuery.Enabled || cfg.Services.BigQuery.Project != "devcloud" || cfg.Services.BigQuery.Location != "US" {
		t.Fatalf("Services.BigQuery = %#v", cfg.Services.BigQuery)
	}
	if cfg.Services.BigQuery.MaxRowsPerTable != 1000000 || cfg.Services.BigQuery.MaxRequestBytes != 10*1024*1024 {
		t.Fatalf("Services.BigQuery limits = %#v", cfg.Services.BigQuery)
	}
	if cfg.Services.BigQuery.Query.MaxResultRows != 10000 || cfg.Services.BigQuery.Query.MaxExecutionSeconds != 30 || cfg.Services.BigQuery.Query.DefaultUseLegacySQL {
		t.Fatalf("Services.BigQuery.Query = %#v", cfg.Services.BigQuery.Query)
	}
	if cfg.Auth.SQS.Mode != "relaxed" || cfg.Auth.SQS.AccessKeyID != "dev" || cfg.Auth.SQS.SecretAccessKey != "dev" || cfg.Auth.SQS.AccountID != "000000000000" {
		t.Fatalf("Auth.SQS = %#v", cfg.Auth.SQS)
	}
	if !cfg.Services.SQS.Enabled || cfg.Services.SQS.Region != "us-east-1" || cfg.Services.SQS.QueueURLHost != "127.0.0.1" {
		t.Fatalf("Services.SQS = %#v", cfg.Services.SQS)
	}
	if cfg.Services.SQS.MaxQueues != 256 || cfg.Services.SQS.MaxMessageBytes != 1024*1024 || cfg.Services.SQS.MaxReceiveBatchSize != 10 {
		t.Fatalf("Services.SQS limits = %#v", cfg.Services.SQS)
	}
	if cfg.Services.SQS.DefaultVisibilityTimeoutSeconds != 30 || cfg.Services.SQS.DefaultDelaySeconds != 0 || cfg.Services.SQS.DefaultMessageRetentionSeconds != 345600 || cfg.Services.SQS.DefaultReceiveWaitTimeSeconds != 0 || cfg.Services.SQS.SchedulerIntervalSeconds != 1 {
		t.Fatalf("Services.SQS timings = %#v", cfg.Services.SQS)
	}
	if cfg.Auth.PubSub.Mode != "relaxed" || cfg.Auth.PubSub.ProjectID != "devcloud" || cfg.Auth.PubSub.BearerToken != "dev" {
		t.Fatalf("Auth.PubSub = %#v", cfg.Auth.PubSub)
	}
	if !cfg.Services.PubSub.Enabled || cfg.Services.PubSub.Project != "devcloud" {
		t.Fatalf("Services.PubSub = %#v", cfg.Services.PubSub)
	}
	if cfg.Services.PubSub.DataDir != "" || cfg.Services.PubSub.MessageDataDir != "" {
		t.Fatalf("Services.PubSub dirs = %#v", cfg.Services.PubSub)
	}
	if cfg.Services.PubSub.DefaultAckDeadlineSeconds != 10 || cfg.Services.PubSub.MessageRetentionSeconds != 604800 || cfg.Services.PubSub.MaxAckDeadlineSeconds != 600 || cfg.Services.PubSub.MaxPullMessages != 1000 || cfg.Services.PubSub.PullWaitTimeoutSeconds != 1 {
		t.Fatalf("Services.PubSub limits = %#v", cfg.Services.PubSub)
	}
	if !cfg.Services.PubSub.EnableREST || !cfg.Services.PubSub.EnableStreamingPull || cfg.Services.PubSub.EnablePush {
		t.Fatalf("Services.PubSub feature flags = %#v", cfg.Services.PubSub)
	}
}

func TestDefaultConfigIncludesBigQueryDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Server.BigQueryPort != 9050 {
		t.Fatalf("Server.BigQueryPort = %d", cfg.Server.BigQueryPort)
	}
	if cfg.Auth.BigQuery.Mode != "relaxed" || cfg.Auth.BigQuery.Project != "devcloud" || cfg.Auth.BigQuery.BearerToken != "dev" {
		t.Fatalf("Auth.BigQuery = %#v", cfg.Auth.BigQuery)
	}
	if !cfg.Services.BigQuery.Enabled || cfg.Services.BigQuery.Project != "devcloud" || cfg.Services.BigQuery.Location != "US" {
		t.Fatalf("Services.BigQuery = %#v", cfg.Services.BigQuery)
	}
}

func TestLoadConfigUsesDefaultsWhenConfigIsMissing(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg != DefaultConfig() {
		t.Fatalf("cfg = %#v, want defaults %#v", cfg, DefaultConfig())
	}
}

func TestWorkspaceStoragePathMustStayUnderDevcloud(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Storage.Path = "data"
	if err := InitWorkspace(cfg); err == nil {
		t.Fatal("InitWorkspace() error = nil for storage outside .devcloud")
	}
	if err := ResetWorkspace(cfg); err == nil {
		t.Fatal("ResetWorkspace() error = nil for storage outside .devcloud")
	}

	cfg.Storage.Path = filepath.Join(".devcloud", "custom-data")
	if err := InitWorkspace(cfg); err != nil {
		t.Fatalf("InitWorkspace() .devcloud custom path error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "mail", "index.json")); err != nil {
		t.Fatalf("custom mail index not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "s3", "buckets")); err != nil {
		t.Fatalf("custom s3 bucket storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "gcs", "upload_sessions")); err != nil {
		t.Fatalf("custom gcs upload session storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "dynamodb")); err != nil {
		t.Fatalf("custom dynamodb storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "bigquery")); err != nil {
		t.Fatalf("custom bigquery storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "pubsub")); err != nil {
		t.Fatalf("custom pubsub storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "message")); err != nil {
		t.Fatalf("custom message storage not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "kv")); err != nil {
		t.Fatalf("custom kv storage not created: %v", err)
	}
}

func TestInitWorkspaceUsesPubSubSpecificDataDirs(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Services.PubSub.DataDir = filepath.Join(".devcloud", "pubsub-store")
	cfg.Services.PubSub.MessageDataDir = filepath.Join(".devcloud", "pubsub-message-store")

	if err := InitWorkspace(cfg); err != nil {
		t.Fatalf("InitWorkspace() error = %v", err)
	}
	if _, err := os.Stat(cfg.Services.PubSub.DataDir); err != nil {
		t.Fatalf("custom pubsub dataDir not created: %v", err)
	}
	if _, err := os.Stat(cfg.Services.PubSub.MessageDataDir); err != nil {
		t.Fatalf("custom pubsub messageDataDir not created: %v", err)
	}

	cfg.Services.PubSub.DataDir = "pubsub-outside"
	if err := InitWorkspace(cfg); err == nil {
		t.Fatal("InitWorkspace() error = nil for pubsub dataDir outside .devcloud")
	}
	cfg.Services.PubSub.DataDir = filepath.Join(".devcloud", "pubsub-store")
	cfg.Services.PubSub.MessageDataDir = "message-outside"
	if err := InitWorkspace(cfg); err == nil {
		t.Fatal("InitWorkspace() error = nil for pubsub messageDataDir outside .devcloud")
	}
}

func TestResetWorkspaceRemovesPubSubSpecificDataDirs(t *testing.T) {
	chdir(t, t.TempDir())
	cfg := DefaultConfig()
	cfg.Services.PubSub.DataDir = filepath.Join(".devcloud", "pubsub-store")
	cfg.Services.PubSub.MessageDataDir = filepath.Join(".devcloud", "pubsub-message-store")

	if err := InitWorkspace(cfg); err != nil {
		t.Fatalf("InitWorkspace() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Services.PubSub.DataDir, "resources.json"), []byte(`{"topics":[]}`), 0o644); err != nil {
		t.Fatalf("write pubsub state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Services.PubSub.MessageDataDir, "message.json"), []byte(`{"messageId":"1"}`), 0o644); err != nil {
		t.Fatalf("write pubsub message state: %v", err)
	}

	if err := ResetWorkspace(cfg); err != nil {
		t.Fatalf("ResetWorkspace() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Services.PubSub.DataDir, "resources.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pubsub state still exists after reset: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Services.PubSub.MessageDataDir, "message.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pubsub message state still exists after reset: %v", err)
	}
	if _, err := os.Stat(cfg.Services.PubSub.DataDir); err != nil {
		t.Fatalf("custom pubsub dataDir not recreated: %v", err)
	}
	if _, err := os.Stat(cfg.Services.PubSub.MessageDataDir); err != nil {
		t.Fatalf("custom pubsub messageDataDir not recreated: %v", err)
	}

	cfg.Services.PubSub.DataDir = "pubsub-outside"
	if err := ResetWorkspace(cfg); err == nil {
		t.Fatal("ResetWorkspace() error = nil for pubsub dataDir outside .devcloud")
	}
	cfg.Services.PubSub.DataDir = filepath.Join(".devcloud", "pubsub-store")
	cfg.Services.PubSub.MessageDataDir = "message-outside"
	if err := ResetWorkspace(cfg); err == nil {
		t.Fatal("ResetWorkspace() error = nil for pubsub messageDataDir outside .devcloud")
	}
}

func TestLoopbackAddrUsesIPv4Loopback(t *testing.T) {
	if got, want := loopbackAddr(8025), "127.0.0.1:8025"; got != want {
		t.Fatalf("loopbackAddr() = %q, want %q", got, want)
	}
}

func TestLoadConfigIgnoresUnknownKeysAndRejectsInvalidKnownValues(t *testing.T) {
	dir := t.TempDir()
	unknownPath := filepath.Join(dir, "unknown.yaml")
	if err := os.WriteFile(unknownPath, []byte("project: custom\nfuture:\n  option: enabled\n"), 0o644); err != nil {
		t.Fatalf("write unknown config: %v", err)
	}
	cfg, err := LoadConfig(unknownPath)
	if err != nil {
		t.Fatalf("LoadConfig() unknown keys error = %v", err)
	}
	if cfg.Project != "custom" {
		t.Fatalf("Project = %q", cfg.Project)
	}

	invalidPath := filepath.Join(dir, "invalid.yaml")
	if err := os.WriteFile(invalidPath, []byte("services:\n  mail:\n    maxMessageBytes: many\n"), 0o644); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	if _, err := LoadConfig(invalidPath); err == nil {
		t.Fatal("LoadConfig() invalid known value error = nil")
	}

	unboundedPath := filepath.Join(dir, "unbounded.yaml")
	if err := os.WriteFile(unboundedPath, []byte("services:\n  mail:\n    maxMessageBytes: 0\n"), 0o644); err != nil {
		t.Fatalf("write unbounded config: %v", err)
	}
	if _, err := LoadConfig(unboundedPath); err == nil {
		t.Fatal("LoadConfig() unbounded maxMessageBytes error = nil")
	}

	invalidS3Path := filepath.Join(dir, "invalid-s3.yaml")
	if err := os.WriteFile(invalidS3Path, []byte("services:\n  s3:\n    multipart:\n      minPartBytes: 0\n"), 0o644); err != nil {
		t.Fatalf("write invalid s3 config: %v", err)
	}
	if _, err := LoadConfig(invalidS3Path); err == nil {
		t.Fatal("LoadConfig() invalid s3 multipart minPartBytes error = nil")
	}

	invalidBigQueryPath := filepath.Join(dir, "invalid-bigquery.yaml")
	if err := os.WriteFile(invalidBigQueryPath, []byte("services:\n  bigquery:\n    maxRequestBytes: 0\n"), 0o644); err != nil {
		t.Fatalf("write invalid bigquery config: %v", err)
	}
	if _, err := LoadConfig(invalidBigQueryPath); err == nil {
		t.Fatal("LoadConfig() invalid bigquery maxRequestBytes error = nil")
	}

	invalidPubSubPath := filepath.Join(dir, "invalid-pubsub.yaml")
	if err := os.WriteFile(invalidPubSubPath, []byte("services:\n  pubsub:\n    maxPullMessages: 0\n"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub config: %v", err)
	}
	if _, err := LoadConfig(invalidPubSubPath); err == nil {
		t.Fatal("LoadConfig() invalid pubsub maxPullMessages error = nil")
	}

	invalidPubSubTimeoutPath := filepath.Join(dir, "invalid-pubsub-timeout.yaml")
	if err := os.WriteFile(invalidPubSubTimeoutPath, []byte("services:\n  pubsub:\n    pullWaitTimeoutSeconds: -1\n"), 0o644); err != nil {
		t.Fatalf("write invalid pubsub timeout config: %v", err)
	}
	if _, err := LoadConfig(invalidPubSubTimeoutPath); err == nil {
		t.Fatal("LoadConfig() invalid pubsub pullWaitTimeoutSeconds error = nil")
	}
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	original, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(original); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	})
}
