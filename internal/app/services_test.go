package app

import (
	"strings"
	"testing"
)

func TestApplyServiceSelectionEmptyKeepsConfig(t *testing.T) {
	cfg := DefaultConfig()
	got, err := ApplyServiceSelection(cfg, nil)
	if err != nil {
		t.Fatalf("ApplyServiceSelection(nil): %v", err)
	}
	if got != cfg {
		t.Fatalf("expected cfg unchanged when selection is empty")
	}

	got, err = ApplyServiceSelection(cfg, []string{})
	if err != nil {
		t.Fatalf("ApplyServiceSelection([]): %v", err)
	}
	if got != cfg {
		t.Fatalf("expected cfg unchanged when selection is empty slice")
	}
}

func TestApplyServiceSelectionEnablesOnlyChosen(t *testing.T) {
	cfg := DefaultConfig()
	// Pick something not enabled by default to prove enable+disable both work.
	cfg.Services.Redis.Enabled = false

	got, err := ApplyServiceSelection(cfg, []string{"s3", "Redis", "BQ"})
	if err != nil {
		t.Fatalf("ApplyServiceSelection: %v", err)
	}

	enabled := map[string]bool{
		"mail":           got.Services.Mail.Enabled,
		"s3":             got.Services.S3.Enabled,
		"gcs":            got.Services.GCS.Enabled,
		"dynamodb":       got.Services.DynamoDB.Enabled,
		"bigquery":       got.Services.BigQuery.Enabled,
		"redshift":       got.Services.Redshift.Enabled,
		"redis":          got.Services.Redis.Enabled,
		"sqs":            got.Services.SQS.Enabled,
		"pubsub":         got.Services.PubSub.Enabled,
		"appautoscaling": got.Services.AppAutoScaling.Enabled,
	}
	want := map[string]bool{
		"s3":       true,
		"redis":    true,
		"bigquery": true,
	}
	for name, on := range enabled {
		if on != want[name] {
			t.Errorf("service %s enabled=%v, want %v", name, on, want[name])
		}
	}
}

func TestApplyServiceSelectionUnknownService(t *testing.T) {
	_, err := ApplyServiceSelection(DefaultConfig(), []string{"s3", "kinesis"})
	if err == nil {
		t.Fatal("expected error for unknown service, got nil")
	}
	if !strings.Contains(err.Error(), "kinesis") {
		t.Errorf("error should mention the offending name; got %v", err)
	}
	if !strings.Contains(err.Error(), "known:") {
		t.Errorf("error should list known services; got %v", err)
	}
}

func TestApplyServiceSelectionDoesNotMutateInput(t *testing.T) {
	cfg := DefaultConfig()
	original := cfg
	if _, err := ApplyServiceSelection(cfg, []string{"s3"}); err != nil {
		t.Fatalf("ApplyServiceSelection: %v", err)
	}
	if cfg != original {
		t.Fatalf("input cfg was mutated")
	}
}

func TestServiceNamesCoversAllToggles(t *testing.T) {
	names := ServiceNames()
	if len(names) != len(serviceToggles) {
		t.Fatalf("ServiceNames length=%d, serviceToggles=%d", len(names), len(serviceToggles))
	}
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if seen[n] {
			t.Errorf("duplicate service name: %s", n)
		}
		seen[n] = true
		if _, ok := serviceToggles[n]; !ok {
			t.Errorf("ServiceNames contains %q which is not in serviceToggles", n)
		}
	}
}
