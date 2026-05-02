package app

import (
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
	if cfg.Server.SMTPPort != 2525 || cfg.Server.DashboardPort != 8825 || cfg.Server.S3Port != 4567 || cfg.Server.GCSPort != 4444 || cfg.Server.DynamoDBPort != 8100 || cfg.Server.BigQueryPort != 9051 {
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
	if _, err := os.Stat(filepath.Join(".devcloud", "custom-data", "kv")); err != nil {
		t.Fatalf("custom kv storage not created: %v", err)
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
