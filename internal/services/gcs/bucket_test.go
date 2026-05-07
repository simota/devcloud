package gcs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestBucketLifecycleAndListBuckets(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Project: "devcloud", Location: "US"}, store).routes()

	create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket","location":"US","storageClass":"STANDARD"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	var created bucketResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Name != "demo-bucket" || created.Kind != "storage#bucket" || created.StorageClass != "STANDARD" {
		t.Fatalf("created bucket = %#v", created)
	}
	if _, err := strconv.ParseUint(created.ProjectNumber, 10, 64); err != nil {
		t.Fatalf("created projectNumber = %q, want uint64 string", created.ProjectNumber)
	}

	get := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var got bucketResource
	if err := json.NewDecoder(get.Body).Decode(&got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.Name != "demo-bucket" {
		t.Fatalf("get bucket name = %q", got.Name)
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed bucketsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Name != "demo-bucket" {
		t.Fatalf("listed buckets = %#v", listed.Items)
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket", "")
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	missing := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing get status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestBucketInsertRejectsDuplicateAndInvalidJSON(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	duplicate := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, want %d; body=%s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}

	invalid := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid json status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestBucketsListSupportsPagination(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{"alpha-bucket", "beta-bucket", "gamma-bucket"} {
		create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"`+name+`"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create %q status = %d; body=%s", name, create.Code, create.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&maxResults=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first bucketsListResponse
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Items) != 2 || first.Items[0].Name != "alpha-bucket" || first.Items[1].Name != "beta-bucket" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&pageToken="+url.QueryEscape(first.NextPageToken), "")
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second bucketsListResponse
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Items) != 1 || second.Items[0].Name != "gamma-bucket" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}

	invalid := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&pageToken=bad", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid page token status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestBucketsListSupportsPrefixFilter(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{"app-assets", "app-logs", "backup-archive"} {
		create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"`+name+`"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create %q status = %d; body=%s", name, create.Code, create.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&prefix=app-", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed bucketsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 2 || listed.Items[0].Name != "app-assets" || listed.Items[1].Name != "app-logs" {
		t.Fatalf("listed buckets = %#v", listed.Items)
	}
}

func TestBucketDeleteRequiresEmptyBucket(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if _, err := store.PutObject(httptest.NewRequest(http.MethodGet, "/", nil).Context(), s3svc.PutObjectInput{
		Bucket: "demo-bucket",
		Key:    "object.txt",
		Body:   strings.NewReader("body"),
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket", "")
	if deleteBucket.Code != http.StatusConflict {
		t.Fatalf("delete non-empty status = %d, want %d; body=%s", deleteBucket.Code, http.StatusConflict, deleteBucket.Body.String())
	}
}
