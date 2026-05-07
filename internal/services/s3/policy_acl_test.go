package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestBucketPolicyMetadataEndpointsPersistAndDelete(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo-bucket/*"}]}`
	put := performRequest(routes, http.MethodPut, "/demo-bucket?policy", strings.NewReader(policy))
	if put.Code != http.StatusNoContent {
		t.Fatalf("put policy status = %d, want %d; body=%s", put.Code, http.StatusNoContent, put.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket?policy", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get policy status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != policy {
		t.Fatalf("policy body = %q, want %q", got, policy)
	}
	if got := get.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("policy content type = %q, want application/json", got)
	}

	deletePolicy := performRequest(routes, http.MethodDelete, "/demo-bucket?policy", nil)
	if deletePolicy.Code != http.StatusNoContent {
		t.Fatalf("delete policy status = %d, want %d; body=%s", deletePolicy.Code, http.StatusNoContent, deletePolicy.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/demo-bucket?policy", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing policy status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(missing.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode missing policy error: %v", err)
	}
	if parsed.Code != "NoSuchBucketPolicy" {
		t.Fatalf("missing policy code = %q, want NoSuchBucketPolicy", parsed.Code)
	}
}

func TestBucketPolicyRejectsMalformedJSON(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	put := performRequest(routes, http.MethodPut, "/demo-bucket?policy", strings.NewReader(`{"Version":`))
	if put.Code != http.StatusBadRequest {
		t.Fatalf("malformed policy status = %d, want %d; body=%s", put.Code, http.StatusBadRequest, put.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(put.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode malformed policy error: %v", err)
	}
	if parsed.Code != "MalformedPolicy" {
		t.Fatalf("malformed policy code = %q, want MalformedPolicy", parsed.Code)
	}
}

func TestBucketAndObjectACLMetadataEndpoints(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if putObject := performRequest(routes, http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("body")); putObject.Code != http.StatusOK {
		t.Fatalf("put object status = %d; body=%s", putObject.Code, putObject.Body.String())
	}

	bucketACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket?acl", nil)
	bucketACLReq.Header.Set("x-amz-acl", "public-read")
	bucketACL := httptest.NewRecorder()
	routes.ServeHTTP(bucketACL, bucketACLReq)
	if bucketACL.Code != http.StatusOK {
		t.Fatalf("put bucket acl status = %d, want %d; body=%s", bucketACL.Code, http.StatusOK, bucketACL.Body.String())
	}
	getBucketACL := performRequest(routes, http.MethodGet, "/demo-bucket?acl", nil)
	if getBucketACL.Code != http.StatusOK {
		t.Fatalf("get bucket acl status = %d, want %d; body=%s", getBucketACL.Code, http.StatusOK, getBucketACL.Body.String())
	}
	var bucketPolicy accessControlPolicy
	if err := xml.NewDecoder(getBucketACL.Body).Decode(&bucketPolicy); err != nil {
		t.Fatalf("decode bucket acl: %v", err)
	}
	if bucketPolicy.CannedACL != "public-read" || len(bucketPolicy.AccessControlList.Grants) != 1 || bucketPolicy.AccessControlList.Grants[0].Permission != "READ" {
		t.Fatalf("bucket acl response = %#v", bucketPolicy)
	}

	objectACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt?acl", nil)
	objectACLReq.Header.Set("x-amz-acl", "bucket-owner-full-control")
	objectACL := httptest.NewRecorder()
	routes.ServeHTTP(objectACL, objectACLReq)
	if objectACL.Code != http.StatusOK {
		t.Fatalf("put object acl status = %d, want %d; body=%s", objectACL.Code, http.StatusOK, objectACL.Body.String())
	}
	getObjectACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt?acl", nil)
	if getObjectACL.Code != http.StatusOK {
		t.Fatalf("get object acl status = %d, want %d; body=%s", getObjectACL.Code, http.StatusOK, getObjectACL.Body.String())
	}
	var objectPolicy accessControlPolicy
	if err := xml.NewDecoder(getObjectACL.Body).Decode(&objectPolicy); err != nil {
		t.Fatalf("decode object acl: %v", err)
	}
	if objectPolicy.CannedACL != "bucket-owner-full-control" || objectPolicy.AccessControlList.Grants[0].Permission != "FULL_CONTROL" {
		t.Fatalf("object acl response = %#v", objectPolicy)
	}

	missingObjectACL := performRequest(routes, http.MethodGet, "/demo-bucket/missing.txt?acl", nil)
	if missingObjectACL.Code != http.StatusNotFound {
		t.Fatalf("missing object acl status = %d, want %d; body=%s", missingObjectACL.Code, http.StatusNotFound, missingObjectACL.Body.String())
	}
}

func TestObjectACLSupportsVersionID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("enable versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}
	first := performRequest(routes, http.MethodPut, "/demo-bucket/docs/versioned-acl.txt", strings.NewReader("first"))
	if first.Code != http.StatusOK {
		t.Fatalf("put first status = %d; body=%s", first.Code, first.Body.String())
	}
	firstVersionID := first.Header().Get("x-amz-version-id")
	if firstVersionID == "" {
		t.Fatal("first version id is empty")
	}
	second := performRequest(routes, http.MethodPut, "/demo-bucket/docs/versioned-acl.txt", strings.NewReader("second"))
	if second.Code != http.StatusOK {
		t.Fatalf("put second status = %d; body=%s", second.Code, second.Body.String())
	}
	secondVersionID := second.Header().Get("x-amz-version-id")
	if secondVersionID == "" || secondVersionID == firstVersionID {
		t.Fatalf("second version id = %q, first = %q", secondVersionID, firstVersionID)
	}

	putFirstACLReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/versioned-acl.txt?acl&versionId="+url.QueryEscape(firstVersionID), nil)
	putFirstACLReq.Header.Set("x-amz-acl", "public-read")
	putFirstACL := httptest.NewRecorder()
	routes.ServeHTTP(putFirstACL, putFirstACLReq)
	if putFirstACL.Code != http.StatusOK {
		t.Fatalf("put first version acl status = %d, want %d; body=%s", putFirstACL.Code, http.StatusOK, putFirstACL.Body.String())
	}

	getFirstACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/versioned-acl.txt?acl&versionId="+url.QueryEscape(firstVersionID), nil)
	if getFirstACL.Code != http.StatusOK {
		t.Fatalf("get first version acl status = %d, want %d; body=%s", getFirstACL.Code, http.StatusOK, getFirstACL.Body.String())
	}
	var firstPolicy accessControlPolicy
	if err := xml.NewDecoder(getFirstACL.Body).Decode(&firstPolicy); err != nil {
		t.Fatalf("decode first version acl: %v", err)
	}
	if firstPolicy.CannedACL != "public-read" {
		t.Fatalf("first version acl = %#v", firstPolicy)
	}

	getLatestACL := performRequest(routes, http.MethodGet, "/demo-bucket/docs/versioned-acl.txt?acl", nil)
	if getLatestACL.Code != http.StatusOK {
		t.Fatalf("get latest acl status = %d, want %d; body=%s", getLatestACL.Code, http.StatusOK, getLatestACL.Body.String())
	}
	var latestPolicy accessControlPolicy
	if err := xml.NewDecoder(getLatestACL.Body).Decode(&latestPolicy); err != nil {
		t.Fatalf("decode latest acl: %v", err)
	}
	if latestPolicy.CannedACL != "private" {
		t.Fatalf("latest acl = %#v, want private", latestPolicy)
	}
}

