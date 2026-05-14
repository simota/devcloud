package app

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintEndpointsListsEveryEnabledService(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.PubSub.EnableREST = true

	var buf bytes.Buffer
	printEndpoints(&buf, cfg)
	out := buf.String()

	for _, want := range []string{
		devcloudBanner[0],
		devcloudBanner[len(devcloudBanner)-1],
		"Endpoints:",
		"Dashboard",
		"http://127.0.0.1:8025/",
		"Mail (SMTP)",
		"smtp://127.0.0.1:1025",
		"http://127.0.0.1:8025/dashboard/mail",
		"S3",
		"http://127.0.0.1:4566",
		"http://127.0.0.1:8025/dashboard/s3",
		"GCS",
		"http://127.0.0.1:4443",
		"DynamoDB",
		"http://127.0.0.1:8000",
		"BigQuery",
		"http://127.0.0.1:9050",
		"SQS",
		"http://127.0.0.1:9324",
		"Pub/Sub (gRPC)",
		"127.0.0.1:8085",
		"Pub/Sub (REST)",
		"http://127.0.0.1:8086",
		"Redshift (SQL)",
		"postgres://127.0.0.1:5439",
		"Redshift (API)",
		"http://127.0.0.1:9099",
		"Press Ctrl-C to stop.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("endpoint banner missing %q\n--- banner ---\n%s", want, out)
		}
	}
}

func TestPrintEndpointsOmitsDisabledServices(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.Mail.Enabled = false
	cfg.Services.S3.Enabled = false
	cfg.Services.GCS.Enabled = false
	cfg.Services.DynamoDB.Enabled = false
	cfg.Services.BigQuery.Enabled = false
	cfg.Services.SQS.Enabled = false
	cfg.Services.PubSub.Enabled = false
	cfg.Services.Redshift.Enabled = false

	var buf bytes.Buffer
	printEndpoints(&buf, cfg)
	out := buf.String()

	if !strings.Contains(out, "Dashboard") {
		t.Fatalf("dashboard line missing: %s", out)
	}
	for _, unwanted := range []string{"Mail (SMTP)", "S3", "GCS", "DynamoDB", "BigQuery", "SQS", "Pub/Sub", "Redshift"} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("banner unexpectedly contains %q for disabled service\n--- banner ---\n%s", unwanted, out)
		}
	}
}

func TestPrintEndpointsOmitsPubSubRESTWhenDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Services.PubSub.EnableREST = false

	var buf bytes.Buffer
	printEndpoints(&buf, cfg)
	out := buf.String()

	if !strings.Contains(out, "Pub/Sub (gRPC)") {
		t.Fatalf("banner missing pubsub grpc line: %s", out)
	}
	if strings.Contains(out, "Pub/Sub (REST)") {
		t.Fatalf("banner unexpectedly lists pubsub REST when disabled: %s", out)
	}
}
