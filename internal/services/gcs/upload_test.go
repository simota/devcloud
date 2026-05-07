package gcs

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestObjectMultipartUploadUsesMetadataAndMediaParts(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	body := strings.Join([]string{
		"--devcloud-boundary",
		"Content-Type: application/json; charset=UTF-8",
		"",
		`{"name":"docs/multipart.txt","contentType":"text/plain","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"multipart-test"}}`,
		"--devcloud-boundary",
		"Content-Type: text/plain",
		"",
		"hello multipart",
		"--devcloud-boundary--",
		"",
	}, "\r\n")
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=multipart", body, map[string]string{
		"Content-Type": `multipart/related; boundary="devcloud-boundary"`,
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("multipart upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode multipart response: %v", err)
	}
	if uploaded.Name != "docs/multipart.txt" || uploaded.ContentType != "text/plain" || uploaded.Metadata["source"] != "multipart-test" {
		t.Fatalf("uploaded object = %#v", uploaded)
	}
	if uploaded.CacheControl != "no-cache" || uploaded.ContentDisposition != "inline" {
		t.Fatalf("uploaded cache/disposition = %#v", uploaded)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fmultipart.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "hello multipart" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}
