package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitWorkspaceCreatesDefaultsWithoutOverwritingConfig(t *testing.T) {
	t.Chdir(t.TempDir())
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

auth:
  smtp:
    mode: off

storage:
  path: .devcloud/custom-data

services:
  mail:
    enabled: true
    maxMessageBytes: 512
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
	if cfg.Server.SMTPPort != 2525 || cfg.Server.DashboardPort != 8825 {
		t.Fatalf("Server = %#v", cfg.Server)
	}
	if cfg.Storage.Path != ".devcloud/custom-data" {
		t.Fatalf("Storage.Path = %q", cfg.Storage.Path)
	}
	if cfg.Services.Mail.MaxMessageBytes != 512 {
		t.Fatalf("MaxMessageBytes = %d", cfg.Services.Mail.MaxMessageBytes)
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
	t.Chdir(t.TempDir())
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
}
