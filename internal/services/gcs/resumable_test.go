package gcs

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestResumableUploadCreatesSessionAndCommitsObject(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt", `{"name":"docs/resumable.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
		"x-goog-meta-source":    "resumable-test",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	location := init.Header().Get("Location")
	if location == "" {
		t.Fatalf("init Location is empty")
	}
	uploadID := init.Header().Get("X-GUploader-UploadID")
	if uploadID == "" {
		t.Fatalf("init X-GUploader-UploadID is empty")
	}
	sessionURL, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}
	if got := sessionURL.Query().Get("upload_id"); got != uploadID {
		t.Fatalf("session upload_id = %q, want X-GUploader-UploadID %q", got, uploadID)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "resumable body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/resumable.txt" || committed.Metadata["source"] != "resumable-test" {
		t.Fatalf("committed object = %#v", committed)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fresumable.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "resumable body" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestResumableUploadUsesJSONMetadataFromInitiationRequest(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable", `{"name":"docs/resumable-metadata.txt","contentType":"text/plain","contentEncoding":"gzip","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"json-init","override":"json"}}`, map[string]string{
		"Content-Type":         "application/json",
		"x-goog-meta-override": "header",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "metadata body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-12/13",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/resumable-metadata.txt" || committed.ContentType != "text/plain" {
		t.Fatalf("committed object = %#v", committed)
	}
	if committed.ContentEncoding != "gzip" || committed.CacheControl != "no-cache" || committed.ContentDisposition != "inline" {
		t.Fatalf("committed cache/disposition/encoding = %#v", committed)
	}
	if committed.Metadata["source"] != "json-init" || committed.Metadata["override"] != "header" {
		t.Fatalf("committed metadata = %#v", committed.Metadata)
	}
}

func TestResumableUploadReadsJSONMetadataWithQueryNameAndUploadContentType(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/query-name.txt", `{"metadata":{"source":"json-init"},"cacheControl":"no-cache","contentDisposition":"inline"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "query metadata", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/query-name.txt" || committed.ContentType != "text/plain" {
		t.Fatalf("committed object = %#v", committed)
	}
	if committed.Metadata["source"] != "json-init" || committed.CacheControl != "no-cache" || committed.ContentDisposition != "inline" {
		t.Fatalf("committed metadata/cache/disposition = %#v", committed)
	}
}

func TestResumableUploadRechecksStoredPreconditionsAtCommit(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/race.txt&ifGenerationMatch=0", `{"name":"docs/race.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	racingWrite := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/race.txt", "existing body", map[string]string{
		"Content-Type": "text/plain",
	})
	if racingWrite.Code != http.StatusOK {
		t.Fatalf("racing write status = %d, want %d; body=%s", racingWrite.Code, http.StatusOK, racingWrite.Body.String())
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "new body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-7/8",
	})
	if commit.Code != http.StatusPreconditionFailed {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusPreconditionFailed, commit.Body.String())
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Frace.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "existing body" {
		t.Fatalf("download body = %q, want existing body", download.Body.String())
	}
}

func TestResumableUploadPersistsChunkStatusAcrossServerRestart(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	sessionDir := t.TempDir()
	routes := NewServer(Config{UploadSessionPath: sessionDir}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/chunked.txt", `{"name":"docs/chunked.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	initialStatus := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "", map[string]string{
		"Content-Range": "bytes */14",
	})
	if initialStatus.Code != 308 {
		t.Fatalf("initial status query code = %d, want 308; body=%s", initialStatus.Code, initialStatus.Body.String())
	}
	if got := initialStatus.Header().Get("Range"); got != "" {
		t.Fatalf("initial status query Range = %q, want empty", got)
	}

	firstChunk := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "resumable ", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-9/14",
	})
	if firstChunk.Code != 308 {
		t.Fatalf("first chunk status = %d, want 308; body=%s", firstChunk.Code, firstChunk.Body.String())
	}
	if got := firstChunk.Header().Get("Range"); got != "bytes=0-9" {
		t.Fatalf("first chunk Range = %q, want bytes=0-9", got)
	}

	restartedRoutes := NewServer(Config{UploadSessionPath: sessionDir}, store).routes()
	status := performRequestWithHeaders(restartedRoutes, http.MethodPut, sessionURL.RequestURI(), "", map[string]string{
		"Content-Range": "bytes */14",
	})
	if status.Code != 308 {
		t.Fatalf("status query code = %d, want 308; body=%s", status.Code, status.Body.String())
	}
	if got := status.Header().Get("Range"); got != "bytes=0-9" {
		t.Fatalf("status Range = %q, want bytes=0-9", got)
	}

	commit := performRequestWithHeaders(restartedRoutes, http.MethodPut, sessionURL.RequestURI(), "body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 10-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}

	download := performRequest(restartedRoutes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fchunked.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if got := download.Body.String(); got != "resumable body" {
		t.Fatalf("download body = %q", got)
	}
}

func TestResumableUploadRejectsMalformedCommitRequests(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	missingID := performRequestWithHeaders(routes, http.MethodPut, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable", "body", map[string]string{
		"Content-Range": "bytes 0-3/4",
	})
	if missingID.Code != http.StatusBadRequest {
		t.Fatalf("missing upload id status = %d, want %d; body=%s", missingID.Code, http.StatusBadRequest, missingID.Body.String())
	}

	unknownID := performRequestWithHeaders(routes, http.MethodPut, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&upload_id=missing", "body", map[string]string{
		"Content-Range": "bytes 0-3/4",
	})
	if unknownID.Code != http.StatusNotFound {
		t.Fatalf("unknown upload id status = %d, want %d; body=%s", unknownID.Code, http.StatusNotFound, unknownID.Body.String())
	}

	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt", `{"contentType":"text/plain"}`, map[string]string{
		"Content-Type": "application/json",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	for _, tc := range []struct {
		name         string
		contentRange string
		body         string
	}{
		{name: "wrong unit", contentRange: "items 0-3/4", body: "body"},
		{name: "payload size mismatch", contentRange: "bytes 0-9/10", body: "body"},
		{name: "wrong committed offset", contentRange: "bytes 1-4/5", body: "body"},
	} {
		rec := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), tc.body, map[string]string{
			"Content-Type":  "text/plain",
			"Content-Range": tc.contentRange,
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
	}
}
