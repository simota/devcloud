package dashboard

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	s3svc "devcloud/internal/services/s3"
)

func TestGCSDashboardPageAndAPIExposeObjects(t *testing.T) {
	gcsStore := s3svc.NewFileBucketStore(t.TempDir())
	sessionDir := t.TempDir()
	sessionID := "session-test"
	if err := os.MkdirAll(filepath.Join(sessionDir, sessionID), 0o755); err != nil {
		t.Fatalf("create upload session dir: %v", err)
	}
	sessionCreatedAt := time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC)
	sessionJSON := `{"Bucket":"demo-bucket","Name":"docs/resumable.txt","ContentType":"text/plain","CreatedAt":"` + sessionCreatedAt.Format(time.RFC3339Nano) + `","ReceivedBytes":9}` + "\n"
	if err := os.WriteFile(filepath.Join(sessionDir, sessionID, "session.json"), []byte(sessionJSON), 0o644); err != nil {
		t.Fatalf("write upload session: %v", err)
	}
	if _, created, err := gcsStore.CreateBucket(context.Background(), "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := gcsStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		Body:        strings.NewReader("hello from gcs dashboard\n"),
		ContentType: "text/plain",
		Metadata:    map[string]string{"source": "gcs-dashboard-test"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, found, err := gcsStore.UpdateObjectMetadata(context.Background(), s3svc.UpdateObjectMetadataInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		ContentType: "text/markdown",
		Metadata:    map[string]string{"source": "gcs-dashboard-test", "owner": "dashboard"},
	}); err != nil || !found {
		t.Fatalf("update object metadata found=%t err=%v", found, err)
	}
	routes := NewServer(Config{
		GCSEndpoint:          "http://127.0.0.1:4443",
		GCSProject:           "devcloud",
		GCSStoragePath:       ".devcloud/data/s3",
		GCSUploadSessionPath: sessionDir,
	}, newDashboardStore(nil, nil), nil, gcsStore).routes()

	legacy := performRequest(routes, http.MethodGet, "/gcs")
	if legacy.Code != http.StatusMovedPermanently {
		t.Fatalf("legacy /gcs status = %d, want %d", legacy.Code, http.StatusMovedPermanently)
	}
	if got := legacy.Header().Get("Location"); got != "/dashboard/gcs" {
		t.Fatalf("legacy /gcs redirect target = %q, want /dashboard/gcs", got)
	}

	page := performRequest(routes, http.MethodGet, "/dashboard/gcs")
	if page.Code != http.StatusOK {
		t.Fatalf("gcs page status = %d, want %d", page.Code, http.StatusOK)
	}
	if body := page.Body.String(); !strings.Contains(body, "devcloud Dashboard") {
		t.Fatalf("gcs page missing React shell: %s", body)
	}

	status := performRequest(routes, http.MethodGet, "/api/gcs/status")
	if status.Code != http.StatusOK {
		t.Fatalf("gcs status code = %d, want %d", status.Code, http.StatusOK)
	}
	if !strings.Contains(status.Body.String(), `"running"`) || !strings.Contains(status.Body.String(), `"project":"devcloud"`) {
		t.Fatalf("gcs status missing running project: %s", status.Body.String())
	}

	buckets := performRequest(routes, http.MethodGet, "/api/gcs/buckets")
	if buckets.Code != http.StatusOK {
		t.Fatalf("gcs buckets code = %d, want %d", buckets.Code, http.StatusOK)
	}
	if !strings.Contains(buckets.Body.String(), "demo-bucket") || !strings.Contains(buckets.Body.String(), "gs://demo-bucket") {
		t.Fatalf("gcs buckets missing bucket: %s", buckets.Body.String())
	}

	objects := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects?prefix=docs/")
	if objects.Code != http.StatusOK {
		t.Fatalf("gcs objects code = %d, want %d", objects.Code, http.StatusOK)
	}
	body := objects.Body.String()
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/markdown"`, `"source":"gcs-dashboard-test"`, `"owner":"dashboard"`, "gs://demo-bucket/docs/readme.txt", `"generation"`, `"metageneration":"2"`, `"crc32c"`, `"storageClass":"STANDARD"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("gcs objects missing %q: %s", want, body)
		}
	}

	detail := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if detail.Code != http.StatusOK {
		t.Fatalf("gcs object detail code = %d, want %d; body=%s", detail.Code, http.StatusOK, detail.Body.String())
	}
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/markdown"`, `"storageClass":"STANDARD"`, `"metageneration":"2"`} {
		if !strings.Contains(detail.Body.String(), want) {
			t.Fatalf("gcs object detail missing %q: %s", want, detail.Body.String())
		}
	}

	download := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt/download")
	if download.Code != http.StatusOK {
		t.Fatalf("gcs download code = %d, want %d", download.Code, http.StatusOK)
	}
	if got := download.Body.String(); got != "hello from gcs dashboard\n" {
		t.Fatalf("gcs download body = %q", got)
	}
	if got := download.Header().Get("x-goog-meta-source"); got != "gcs-dashboard-test" {
		t.Fatalf("gcs download metadata = %q, want gcs-dashboard-test", got)
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("gcs object delete code = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
	deletedObject := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if deletedObject.Code != http.StatusNotFound {
		t.Fatalf("deleted gcs object code = %d, want %d; body=%s", deletedObject.Code, http.StatusNotFound, deletedObject.Body.String())
	}

	createBucket := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":"dashboard-created"}`)
	if createBucket.Code != http.StatusCreated {
		t.Fatalf("gcs bucket create code = %d, want %d; body=%s", createBucket.Code, http.StatusCreated, createBucket.Body.String())
	}
	if !strings.Contains(createBucket.Body.String(), `"name":"dashboard-created"`) {
		t.Fatalf("gcs bucket create missing name: %s", createBucket.Body.String())
	}
	deleteBucket := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/dashboard-created")
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("gcs bucket delete code = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	sessions := performRequest(routes, http.MethodGet, "/api/gcs/upload-sessions")
	if sessions.Code != http.StatusOK {
		t.Fatalf("gcs upload sessions code = %d, want %d", sessions.Code, http.StatusOK)
	}
	for _, want := range []string{`"id":"session-test"`, `"bucket":"demo-bucket"`, `"name":"docs/resumable.txt"`, `"receivedBytes":9`} {
		if !strings.Contains(sessions.Body.String(), want) {
			t.Fatalf("gcs upload sessions missing %q: %s", want, sessions.Body.String())
		}
	}

	uploads := performRequest(routes, http.MethodGet, "/api/gcs/uploads")
	if uploads.Code != http.StatusOK {
		t.Fatalf("gcs uploads alias code = %d, want %d", uploads.Code, http.StatusOK)
	}
	if !strings.Contains(uploads.Body.String(), `"id":"session-test"`) {
		t.Fatalf("gcs uploads alias missing session: %s", uploads.Body.String())
	}

	deleteSession := performRequest(routes, http.MethodDelete, "/api/gcs/uploads/session-test")
	if deleteSession.Code != http.StatusNoContent {
		t.Fatalf("gcs upload session delete code = %d, want %d", deleteSession.Code, http.StatusNoContent)
	}
	if _, err := os.Stat(filepath.Join(sessionDir, sessionID)); !os.IsNotExist(err) {
		t.Fatalf("upload session dir still exists or stat failed: %v", err)
	}

	rejectTraversal := performRequest(routes, http.MethodDelete, "/api/gcs/uploads/..%2Foutside")
	if rejectTraversal.Code != http.StatusNotFound {
		t.Fatalf("gcs upload session traversal code = %d, want %d", rejectTraversal.Code, http.StatusNotFound)
	}
}

func TestGCSDashboardAPIHandlesDisabledValidationAndMissingResources(t *testing.T) {
	disabledRoutes := NewServer(Config{}, newDashboardStore(nil, nil)).routes()

	status := performRequest(disabledRoutes, http.MethodGet, "/api/gcs/status")
	if status.Code != http.StatusOK {
		t.Fatalf("disabled status code = %d, want %d", status.Code, http.StatusOK)
	}
	if body := status.Body.String(); !strings.Contains(body, `"status":"disabled"`) || !strings.Contains(body, `"running":false`) {
		t.Fatalf("disabled status body = %s", body)
	}
	for _, target := range []string{"/api/gcs/buckets", "/api/gcs/upload-sessions", "/api/gcs/uploads/session-test"} {
		rec := performRequest(disabledRoutes, http.MethodGet, target)
		if target == "/api/gcs/uploads/session-test" {
			rec = performRequest(disabledRoutes, http.MethodDelete, target)
		}
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("disabled %s status = %d, want %d", target, rec.Code, http.StatusServiceUnavailable)
		}
	}
	if rec := performRequest(disabledRoutes, http.MethodPost, "/api/gcs/status"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET" {
		t.Fatalf("disabled status POST = %d Allow=%q, want 405 GET", rec.Code, rec.Header().Get("Allow"))
	}

	gcsStore := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{GCSUploadSessionPath: t.TempDir()}, newDashboardStore(nil, nil), nil, gcsStore).routes()

	invalidJSON := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":`)
	if invalidJSON.Code != http.StatusBadRequest {
		t.Fatalf("invalid bucket json status = %d, want %d; body=%s", invalidJSON.Code, http.StatusBadRequest, invalidJSON.Body.String())
	}
	invalidBucket := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":"Bad_Bucket"}`)
	if invalidBucket.Code != http.StatusBadRequest {
		t.Fatalf("invalid bucket name status = %d, want %d; body=%s", invalidBucket.Code, http.StatusBadRequest, invalidBucket.Body.String())
	}
	createBucket := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":"managed-bucket"}`)
	if createBucket.Code != http.StatusCreated {
		t.Fatalf("create bucket status = %d, want %d; body=%s", createBucket.Code, http.StatusCreated, createBucket.Body.String())
	}
	duplicateBucket := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":"managed-bucket"}`)
	if duplicateBucket.Code != http.StatusConflict {
		t.Fatalf("duplicate bucket status = %d, want %d; body=%s", duplicateBucket.Code, http.StatusConflict, duplicateBucket.Body.String())
	}

	missingBucket := performRequest(routes, http.MethodGet, "/api/gcs/buckets/missing-bucket")
	if missingBucket.Code != http.StatusNotFound {
		t.Fatalf("missing bucket status = %d, want %d; body=%s", missingBucket.Code, http.StatusNotFound, missingBucket.Body.String())
	}
	missingObjects := performRequest(routes, http.MethodGet, "/api/gcs/buckets/missing-bucket/objects")
	if missingObjects.Code != http.StatusNotFound {
		t.Fatalf("missing bucket objects status = %d, want %d; body=%s", missingObjects.Code, http.StatusNotFound, missingObjects.Body.String())
	}
	missingObject := performRequest(routes, http.MethodGet, "/api/gcs/buckets/managed-bucket/objects/docs/missing.txt")
	if missingObject.Code != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want %d; body=%s", missingObject.Code, http.StatusNotFound, missingObject.Body.String())
	}
	missingDownload := performRequest(routes, http.MethodGet, "/api/gcs/buckets/managed-bucket/objects/docs/missing.txt/download")
	if missingDownload.Code != http.StatusNotFound {
		t.Fatalf("missing download status = %d, want %d; body=%s", missingDownload.Code, http.StatusNotFound, missingDownload.Body.String())
	}
	if rec := performRequest(routes, http.MethodPut, "/api/gcs/buckets/managed-bucket/objects/docs/missing.txt"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, DELETE" {
		t.Fatalf("object PUT status = %d Allow=%q, want 405 GET, DELETE", rec.Code, rec.Header().Get("Allow"))
	}

	if _, err := gcsStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket: "managed-bucket",
		Key:    "docs/live.txt",
		Body:   strings.NewReader("dashboard object"),
	}); err != nil {
		t.Fatalf("put gcs object: %v", err)
	}
	deleteNonEmptyBucket := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/managed-bucket")
	if deleteNonEmptyBucket.Code != http.StatusConflict {
		t.Fatalf("delete non-empty bucket status = %d, want %d; body=%s", deleteNonEmptyBucket.Code, http.StatusConflict, deleteNonEmptyBucket.Body.String())
	}
	deleteObject := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/managed-bucket/objects/docs/live.txt")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("delete object status = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
	deleteMissingBucket := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/missing-bucket")
	if deleteMissingBucket.Code != http.StatusNotFound {
		t.Fatalf("delete missing bucket status = %d, want %d; body=%s", deleteMissingBucket.Code, http.StatusNotFound, deleteMissingBucket.Body.String())
	}
	if rec := performRequest(routes, http.MethodPost, "/api/gcs/upload-sessions"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET" {
		t.Fatalf("upload sessions POST status = %d Allow=%q, want 405 GET", rec.Code, rec.Header().Get("Allow"))
	}
	if rec := performRequest(routes, http.MethodGet, "/api/gcs/uploads/session-test"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "DELETE" {
		t.Fatalf("upload session GET status = %d Allow=%q, want 405 DELETE", rec.Code, rec.Header().Get("Allow"))
	}
}
