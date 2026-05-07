package gcs

import (
	"encoding/json"
	"net/http"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestObjectInsertRejectsGenerationPreconditionMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	mismatch := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999", "mismatch\n", map[string]string{
		"Content-Type": "text/plain",
	})
	if mismatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("mismatch status = %d, want %d; body=%s", mismatch.Code, http.StatusPreconditionFailed, mismatch.Body.String())
	}

	createOnly := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=0", "created\n", map[string]string{
		"Content-Type": "text/plain",
	})
	if createOnly.Code != http.StatusOK {
		t.Fatalf("create-only status = %d, want %d; body=%s", createOnly.Code, http.StatusOK, createOnly.Body.String())
	}
}

func TestObjectReadDownloadAndDeleteRejectGenerationPreconditionMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition-read.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?alt=media&ifGenerationMatch=999999",
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, "")
		if rec.Code != http.StatusPreconditionFailed {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
		}
	}

	metadata := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch="+uploaded.Generation, "")
	if metadata.Code != http.StatusOK {
		t.Fatalf("matching metadata status = %d, want %d; body=%s", metadata.Code, http.StatusOK, metadata.Body.String())
	}
	deleteObject := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch="+uploaded.Generation, "")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("matching delete status = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
}

func TestObjectOperationsRejectInvalidPreconditionValues(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	if upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "body", map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d; body=%s", upload.Code, upload.Body.String())
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
		body   string
	}{
		{
			name:   "media upload invalid generation match",
			method: http.MethodPost,
			target: "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/new.txt&ifGenerationMatch=bad",
			body:   "new body",
		},
		{
			name:   "metadata invalid metageneration match",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationMatch=bad",
		},
		{
			name:   "download invalid generation not match",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?alt=media&ifGenerationNotMatch=bad",
		},
		{
			name:   "patch invalid metageneration not match",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationNotMatch=bad",
			body:   `{"metadata":{"source":"invalid"}}`,
		},
		{
			name:   "delete invalid generation match",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifGenerationMatch=bad",
		},
		{
			name:   "copy invalid source metageneration",
			method: http.MethodPost,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceMetagenerationMatch=bad",
			body:   `{}`,
		},
		{
			name:   "compose invalid destination generation",
			method: http.MethodPost,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fjoined.txt/compose?ifGenerationMatch=bad",
			body:   `{"sourceObjects":[{"name":"docs/source.txt"}]}`,
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
	}
}

func TestObjectOperationsHonorGenerationQuery(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/versioned.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=" + uploaded.Generation,
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation=" + uploaded.Generation,
		},
		{
			name:   "patch",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=" + uploaded.Generation,
		},
	} {
		body := ""
		if tc.method == http.MethodPatch {
			body = `{"metadata":{"source":"generation-query"}}`
		}
		rec := performRequest(routes, tc.method, tc.target, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation=1",
		},
		{
			name:   "patch",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, `{"metadata":{"source":"stale"}}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s stale generation status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusNotFound, rec.Body.String())
		}
	}

	invalid := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=bad", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid generation status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation="+uploaded.Generation, "")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("matching delete status = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
}
