package s3

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

func TestBucketLifecycleMetadataEndpointsPersistAndDelete(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>expire-logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config))
	if put.Code != http.StatusOK {
		t.Fatalf("put lifecycle status = %d, want %d; body=%s", put.Code, http.StatusOK, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?lifecycle", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get lifecycle status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var parsed LifecycleConfiguration
	if err := xml.NewDecoder(get.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode lifecycle config: %v", err)
	}
	if len(parsed.Rules) != 1 || parsed.Rules[0].ID != "expire-logs" || parsed.Rules[0].Filter.Prefix != "logs/" || parsed.Rules[0].Expiration.Days == nil || *parsed.Rules[0].Expiration.Days != 30 {
		t.Fatalf("lifecycle config = %#v", parsed)
	}

	deleteLifecycle := performRequest(routes, http.MethodDelete, "/demo-bucket?lifecycle", nil)
	if deleteLifecycle.Code != http.StatusNoContent {
		t.Fatalf("delete lifecycle status = %d, want %d; body=%s", deleteLifecycle.Code, http.StatusNoContent, deleteLifecycle.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?lifecycle", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing lifecycle status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
	var parsedError errorResponse
	if err := xml.NewDecoder(missing.Body).Decode(&parsedError); err != nil {
		t.Fatalf("decode missing lifecycle error: %v", err)
	}
	if parsedError.Code != "NoSuchLifecycleConfiguration" {
		t.Fatalf("missing lifecycle code = %q, want NoSuchLifecycleConfiguration", parsedError.Code)
	}
}

func TestBucketLifecycleExpirationAppliesDeterministically(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if putLog := performRequest(routes, http.MethodPut, "/demo-bucket/logs/old.txt", strings.NewReader("old log")); putLog.Code != http.StatusOK {
		t.Fatalf("put log status = %d; body=%s", putLog.Code, putLog.Body.String())
	}
	if putDoc := performRequest(routes, http.MethodPut, "/demo-bucket/docs/keep.txt", strings.NewReader("keep doc")); putDoc.Code != http.StatusOK {
		t.Fatalf("put doc status = %d; body=%s", putDoc.Code, putDoc.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>expire-logs-now</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>`
	if putLifecycle := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config)); putLifecycle.Code != http.StatusOK {
		t.Fatalf("put lifecycle status = %d; body=%s", putLifecycle.Code, putLifecycle.Body.String())
	}

	expired := performRequest(routes, http.MethodGet, "/demo-bucket/logs/old.txt", nil)
	if expired.Code != http.StatusNotFound {
		t.Fatalf("expired object status = %d, want %d; body=%s", expired.Code, http.StatusNotFound, expired.Body.String())
	}
	kept := performRequest(routes, http.MethodGet, "/demo-bucket/docs/keep.txt", nil)
	if kept.Code != http.StatusOK || kept.Body.String() != "keep doc" {
		t.Fatalf("kept object status=%d body=%q", kept.Code, kept.Body.String())
	}
	list := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d; body=%s", list.Code, list.Body.String())
	}
	if body := list.Body.String(); strings.Contains(body, "logs/old.txt") || !strings.Contains(body, "docs/keep.txt") {
		t.Fatalf("list body after lifecycle expiration = %s", body)
	}
}

func TestBucketLifecycleRejectsUnsupportedTransitions(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	config := `<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?lifecycle", strings.NewReader(config))
	if put.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported lifecycle status = %d, want %d; body=%s", put.Code, http.StatusNotImplemented, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode unsupported lifecycle error: %v", err)
	}
	if parsed.Code != "NotImplemented" {
		t.Fatalf("unsupported lifecycle code = %q, want NotImplemented", parsed.Code)
	}
}

