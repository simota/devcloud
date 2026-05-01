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
	if cfg.Server.SMTPPort != 2525 || cfg.Server.DashboardPort != 8825 || cfg.Server.S3Port != 4567 || cfg.Server.GCSPort != 4444 {
		t.Fatalf("Server = %#v", cfg.Server)
	}
	if cfg.Auth.S3.AccessKeyID != "local" || cfg.Auth.S3.SecretAccessKey != "secret" {
		t.Fatalf("Auth.S3 = %#v", cfg.Auth.S3)
	}
	if cfg.Auth.GCS.Mode != "bearer-dev" || cfg.Auth.GCS.Project != "custom-gcs-project" || cfg.Auth.GCS.BearerToken != "local-token" {
		t.Fatalf("Auth.GCS = %#v", cfg.Auth.GCS)
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
}

func TestDefaultConfigIncludesS3AndGCSDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Server.S3Port != 4566 {
		t.Fatalf("Server.S3Port = %d", cfg.Server.S3Port)
	}
	if cfg.Server.GCSPort != 4443 {
		t.Fatalf("Server.GCSPort = %d", cfg.Server.GCSPort)
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
