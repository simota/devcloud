package gcs

import (
	"encoding/json"
	"net/http"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestObjectCopyEnforcesSourcePreconditions(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "copy source", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	mismatch := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch=999999", `{}`)
	if mismatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("mismatch status = %d, want %d; body=%s", mismatch.Code, http.StatusPreconditionFailed, mismatch.Body.String())
	}

	copyObject := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch="+uploaded.Generation+"&ifSourceMetagenerationMatch=1", `{}`)
	if copyObject.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyObject.Code, http.StatusOK, copyObject.Body.String())
	}
	var copied objectResource
	if err := json.NewDecoder(copyObject.Body).Decode(&copied); err != nil {
		t.Fatalf("decode copy response: %v", err)
	}
	if copied.Name != "docs/copy.txt" {
		t.Fatalf("copied object = %#v", copied)
	}
}

func TestObjectCopyUsesDestinationMetadataWhenProvided(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "copy source", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "original",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}

	copyObject := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt", `{"contentType":"text/markdown","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"copy-request","owner":"gcs"}}`)
	if copyObject.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyObject.Code, http.StatusOK, copyObject.Body.String())
	}
	var copied objectResource
	if err := json.NewDecoder(copyObject.Body).Decode(&copied); err != nil {
		t.Fatalf("decode copy response: %v", err)
	}
	if copied.ContentType != "text/markdown" || copied.CacheControl != "no-cache" || copied.ContentDisposition != "inline" {
		t.Fatalf("copied metadata fields = %#v", copied)
	}
	if copied.Metadata["source"] != "copy-request" || copied.Metadata["owner"] != "gcs" {
		t.Fatalf("copied user metadata = %#v", copied.Metadata)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fcopy.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "copy source" {
		t.Fatalf("download body = %q", download.Body.String())
	}
	if got := download.Header().Get("Content-Type"); got != "text/markdown" {
		t.Fatalf("download Content-Type = %q, want text/markdown", got)
	}
}

func TestObjectRewriteCopiesExistingObject(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "rewrite source", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "rewrite-test",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	rewrite := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/rewriteTo/b/demo-bucket/o/docs%2Frewrite.txt?ifSourceGenerationMatch="+uploaded.Generation, `{}`)
	if rewrite.Code != http.StatusOK {
		t.Fatalf("rewrite status = %d, want %d; body=%s", rewrite.Code, http.StatusOK, rewrite.Body.String())
	}
	var response rewriteResponse
	if err := json.NewDecoder(rewrite.Body).Decode(&response); err != nil {
		t.Fatalf("decode rewrite response: %v", err)
	}
	if response.Kind != "storage#rewriteResponse" || !response.Done || response.Resource.Name != "docs/rewrite.txt" {
		t.Fatalf("rewrite response = %#v", response)
	}
	if response.TotalBytesRewritten != "14" || response.ObjectSize != "14" {
		t.Fatalf("rewrite size fields = %#v", response)
	}
	if response.Resource.ContentType != "text/plain" || response.Resource.Metadata["source"] != "rewrite-test" {
		t.Fatalf("rewritten resource = %#v", response.Resource)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Frewrite.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "rewrite source" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestObjectComposeConcatenatesSources(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	firstUpload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/one.txt", "hello ", map[string]string{
		"Content-Type": "text/plain",
	})
	if firstUpload.Code != http.StatusOK {
		t.Fatalf("first upload status = %d, want %d; body=%s", firstUpload.Code, http.StatusOK, firstUpload.Body.String())
	}
	var first objectResource
	if err := json.NewDecoder(firstUpload.Body).Decode(&first); err != nil {
		t.Fatalf("decode first upload response: %v", err)
	}
	secondUpload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/two.txt", "compose", map[string]string{
		"Content-Type": "text/plain",
	})
	if secondUpload.Code != http.StatusOK {
		t.Fatalf("second upload status = %d, want %d; body=%s", secondUpload.Code, http.StatusOK, secondUpload.Body.String())
	}

	compose := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose", `{"sourceObjects":[{"name":"parts/one.txt","generation":"`+first.Generation+`"},{"name":"parts/two.txt"}],"destination":{"contentType":"text/plain","metadata":{"source":"compose-test"}}}`)
	if compose.Code != http.StatusOK {
		t.Fatalf("compose status = %d, want %d; body=%s", compose.Code, http.StatusOK, compose.Body.String())
	}
	var composed objectResource
	if err := json.NewDecoder(compose.Body).Decode(&composed); err != nil {
		t.Fatalf("decode compose response: %v", err)
	}
	if composed.Name != "parts/joined.txt" || composed.ContentType != "text/plain" || composed.Metadata["source"] != "compose-test" {
		t.Fatalf("composed object = %#v", composed)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "hello compose" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestObjectComposeRejectsSourceGenerationMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/source.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}

	compose := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose", `{"sourceObjects":[{"name":"parts/source.txt","objectPreconditions":{"ifGenerationMatch":"999999"}}]}`)
	if compose.Code != http.StatusPreconditionFailed {
		t.Fatalf("compose status = %d, want %d; body=%s", compose.Code, http.StatusPreconditionFailed, compose.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("composed object status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}
