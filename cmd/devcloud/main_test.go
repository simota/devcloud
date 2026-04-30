package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunInitCreatesWorkspace(t *testing.T) {
	chdir(t, t.TempDir())

	if _, err := captureStdout(func() error {
		return run([]string{"init"})
	}); err != nil {
		t.Fatalf("run init error = %v", err)
	}

	for _, path := range []string{
		filepath.Join(".devcloud", "config.yaml"),
		filepath.Join(".devcloud", "data", "blobs"),
		filepath.Join(".devcloud", "data", "mail", "index.json"),
		filepath.Join(".devcloud", "logs"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
}

func TestRunDashboardPrintsConfiguredURL(t *testing.T) {
	chdir(t, t.TempDir())
	if err := os.MkdirAll(".devcloud", 0o755); err != nil {
		t.Fatalf("create .devcloud: %v", err)
	}
	config := []byte("server:\n  dashboardPort: 8825\n")
	if err := os.WriteFile(filepath.Join(".devcloud", "config.yaml"), config, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	output, err := captureStdout(func() error {
		return run([]string{"dashboard"})
	})
	if err != nil {
		t.Fatalf("run dashboard error = %v", err)
	}
	if !strings.Contains(output, "http://localhost:8825") {
		t.Fatalf("dashboard output = %q", output)
	}
}

func TestRunResetRecreatesStorage(t *testing.T) {
	chdir(t, t.TempDir())

	if _, err := captureStdout(func() error {
		return run([]string{"init"})
	}); err != nil {
		t.Fatalf("run init error = %v", err)
	}
	stalePath := filepath.Join(".devcloud", "data", "mail", "stale.txt")
	if err := os.WriteFile(stalePath, []byte("old"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	if _, err := captureStdout(func() error {
		return run([]string{"reset"})
	}); err != nil {
		t.Fatalf("run reset error = %v", err)
	}

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale file stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(filepath.Join(".devcloud", "data", "mail", "index.json")); err != nil {
		t.Fatalf("mail index not recreated: %v", err)
	}
}

func captureStdout(fn func() error) (string, error) {
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = writer

	runErr := fn()
	closeErr := writer.Close()
	os.Stdout = original

	data, readErr := io.ReadAll(reader)
	reader.Close()
	if runErr != nil {
		return string(data), runErr
	}
	if closeErr != nil {
		return string(data), closeErr
	}
	return string(data), readErr
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
