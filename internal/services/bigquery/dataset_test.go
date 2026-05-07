package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestDatasetCatalogCRUDPersistsBigQueryShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "EU", StoragePath: t.TempDir()})

	create := httptest.NewRecorder()
	createBody := `{"datasetReference":{"datasetId":"analytics"},"friendlyName":"Analytics","labels":{"env":"test"}}`
	server.routes().ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets", strings.NewReader(createBody)))
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	var created datasetResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created dataset: %v", err)
	}
	if created.Kind != "bigquery#dataset" || created.DatasetReference.ProjectID != "local-project" || created.DatasetReference.DatasetID != "analytics" {
		t.Fatalf("created dataset = %#v", created)
	}
	if created.Location != "EU" || created.FriendlyName != "Analytics" || created.Labels["env"] != "test" {
		t.Fatalf("created metadata = %#v", created)
	}
	if created.CreationTime == "" || created.LastModifiedTime == "" || created.ETag == "" {
		t.Fatalf("created timestamps/etag missing = %#v", created)
	}
	if created.SelfLink != "/bigquery/v2/projects/local-project/datasets/analytics" {
		t.Fatalf("selfLink = %q", created.SelfLink)
	}

	get := httptest.NewRecorder()
	server.routes().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"datasetId":"analytics"`) {
		t.Fatalf("get status/body = %d %s", get.Code, get.Body.String())
	}

	patch := httptest.NewRecorder()
	server.routes().ServeHTTP(patch, httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/local-project/datasets/analytics", strings.NewReader(`{"description":"patched"}`)))
	if patch.Code != http.StatusOK || !strings.Contains(patch.Body.String(), `"description":"patched"`) {
		t.Fatalf("patch status/body = %d %s", patch.Code, patch.Body.String())
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var listed datasetsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listed.Kind != "bigquery#datasetList" || listed.TotalItems != 1 || len(listed.Datasets) != 1 {
		t.Fatalf("listed datasets = %#v", listed)
	}
	if listed.Datasets[0].DatasetReference.DatasetID != "analytics" {
		t.Fatalf("listed item = %#v", listed.Datasets[0])
	}

	deleteRec := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteRec, httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/local-project/datasets/analytics", nil))
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missing := httptest.NewRecorder()
	server.routes().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestDatasetCreateRejectsDuplicateAndUnsafeIDs(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"analytics"}}`))
	first := httptest.NewRecorder()
	server.routes().ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, body = %s", first.Code, first.Body.String())
	}

	duplicate := httptest.NewRecorder()
	server.routes().ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"analytics"}}`)))
	if duplicate.Code != http.StatusConflict || strings.Contains(duplicate.Body.String(), string(filepath.Separator)) {
		t.Fatalf("duplicate status/body = %d %s", duplicate.Code, duplicate.Body.String())
	}

	unsafe := httptest.NewRecorder()
	server.routes().ServeHTTP(unsafe, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets", strings.NewReader(`{"datasetReference":{"datasetId":"../secret"}}`)))
	if unsafe.Code != http.StatusBadRequest {
		t.Fatalf("unsafe status = %d, body = %s", unsafe.Code, unsafe.Body.String())
	}
}

func TestDatasetListPaginatesWithNextPageToken(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	for _, datasetID := range []string{"alpha", "beta", "gamma"} {
		createDatasetForTest(t, server, "local-project", datasetID)
	}

	firstPage := httptest.NewRecorder()
	server.routes().ServeHTTP(firstPage, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets?maxResults=2", nil))
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body = %s", firstPage.Code, firstPage.Body.String())
	}
	var first datasetsListResponse
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if first.TotalItems != 3 || first.NextPageToken != "2" || len(first.Datasets) != 2 || first.Datasets[0].DatasetReference.DatasetID != "alpha" || first.Datasets[1].DatasetReference.DatasetID != "beta" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := httptest.NewRecorder()
	server.routes().ServeHTTP(secondPage, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets?pageToken="+first.NextPageToken+"&maxResults=2", nil))
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, body = %s", secondPage.Code, secondPage.Body.String())
	}
	var second datasetsListResponse
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.TotalItems != 3 || second.NextPageToken != "" || len(second.Datasets) != 1 || second.Datasets[0].DatasetReference.DatasetID != "gamma" {
		t.Fatalf("second page = %#v", second)
	}
}
