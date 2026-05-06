package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestProjectsListUsesBigQueryShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project", AuthMode: "relaxed"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects", nil)

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var response projectsListResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Kind != "bigquery#projectList" || response.TotalItems != 1 {
		t.Fatalf("response metadata = %#v", response)
	}
	if len(response.Projects) != 1 || response.Projects[0].ProjectRef.ProjectID != "local-project" {
		t.Fatalf("projects = %#v", response.Projects)
	}
}

func TestProjectServiceAccountUsesBigQueryShapeAndValidatesProjectID(t *testing.T) {
	server := NewServer(Config{Project: "local-project", AuthMode: "relaxed"})

	rec := httptest.NewRecorder()
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/serviceAccount", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var response serviceAccountResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Kind != "bigquery#getServiceAccountResponse" || response.Email != "devcloud-bigquery@local-project.iam.gserviceaccount.com" {
		t.Fatalf("service account response = %#v", response)
	}

	invalid := httptest.NewRecorder()
	server.routes().ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/bad.project/serviceAccount", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, body = %s", invalid.Code, invalid.Body.String())
	}
	if strings.Contains(invalid.Body.String(), "devcloud-bigquery@bad.project") {
		t.Fatalf("invalid project error leaked synthesized email: %s", invalid.Body.String())
	}
}

func TestBearerDevModeRequiresMatchingBearerToken(t *testing.T) {
	server := NewServer(Config{Project: "local-project", AuthMode: "bearer-dev", BearerToken: "expected"})

	unauthorized := httptest.NewRecorder()
	server.routes().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}
	if strings.Contains(unauthorized.Body.String(), "expected") {
		t.Fatalf("error response leaked bearer token: %s", unauthorized.Body.String())
	}

	authorized := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects", nil)
	req.Header.Set("Authorization", "Bearer expected")
	server.routes().ServeHTTP(authorized, req)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d, want %d", authorized.Code, http.StatusOK)
	}
}

func TestStrictModeRequiresMatchingBearerToken(t *testing.T) {
	server := NewServer(Config{Project: "local-project", AuthMode: "strict", BearerToken: "expected"})

	wrongToken := httptest.NewRecorder()
	wrongReq := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects", nil)
	wrongReq.Header.Set("Authorization", "Bearer wrong")
	server.routes().ServeHTTP(wrongToken, wrongReq)
	if wrongToken.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want %d", wrongToken.Code, http.StatusUnauthorized)
	}
	if strings.Contains(wrongToken.Body.String(), "expected") || strings.Contains(wrongToken.Body.String(), "wrong") {
		t.Fatalf("error response leaked bearer token: %s", wrongToken.Body.String())
	}

	authorized := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects", nil)
	req.Header.Set("Authorization", "Bearer expected")
	server.routes().ServeHTTP(authorized, req)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d, want %d", authorized.Code, http.StatusOK)
	}
}

func TestNotFoundUsesBigQueryErrorShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project"})
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	var response errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if response.Error.Code != http.StatusNotFound || response.Error.Status != "NOT_FOUND" {
		t.Fatalf("error = %#v", response.Error)
	}
	if len(response.Error.Errors) != 1 || response.Error.Errors[0].Reason != "notFound" {
		t.Fatalf("error details = %#v", response.Error.Errors)
	}
}

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

func TestTableCatalogCRUDPersistsBigQueryShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")

	create := httptest.NewRecorder()
	createBody := `{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"age","type":"INTEGER"}]},"friendlyName":"People"}`
	server.routes().ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(createBody)))
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	var created tableResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created table: %v", err)
	}
	if created.Kind != "bigquery#table" || created.TableReference.ProjectID != "local-project" || created.TableReference.DatasetID != "analytics" || created.TableReference.TableID != "people" {
		t.Fatalf("created table = %#v", created)
	}
	if created.Type != "TABLE" || created.NumRows != "0" || created.NumBytes != "0" || len(created.Schema.Fields) != 2 {
		t.Fatalf("created table metadata = %#v", created)
	}
	if created.SelfLink != "/bigquery/v2/projects/local-project/datasets/analytics/tables/people" {
		t.Fatalf("selfLink = %q", created.SelfLink)
	}

	get := httptest.NewRecorder()
	server.routes().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"tableId":"people"`) {
		t.Fatalf("get status/body = %d %s", get.Code, get.Body.String())
	}

	patch := httptest.NewRecorder()
	server.routes().ServeHTTP(patch, httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people", strings.NewReader(`{"description":"patched table"}`)))
	if patch.Code != http.StatusOK || !strings.Contains(patch.Body.String(), `"description":"patched table"`) {
		t.Fatalf("patch status/body = %d %s", patch.Code, patch.Body.String())
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var listed tablesListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listed.Kind != "bigquery#tableList" || listed.TotalItems != 1 || len(listed.Tables) != 1 {
		t.Fatalf("listed tables = %#v", listed)
	}
	if listed.Tables[0].TableReference.TableID != "people" {
		t.Fatalf("listed item = %#v", listed.Tables[0])
	}

	deleteRec := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteRec, httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people", nil))
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missing := httptest.NewRecorder()
	server.routes().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestTableListPaginatesWithNextPageToken(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	for _, tableID := range []string{"events", "people", "sessions"} {
		createTableForTest(t, server, "local-project", "analytics", tableID)
	}

	firstPage := httptest.NewRecorder()
	server.routes().ServeHTTP(firstPage, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables?maxResults=2", nil))
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body = %s", firstPage.Code, firstPage.Body.String())
	}
	var first tablesListResponse
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if first.TotalItems != 3 || first.NextPageToken != "2" || len(first.Tables) != 2 || first.Tables[0].TableReference.TableID != "events" || first.Tables[1].TableReference.TableID != "people" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := httptest.NewRecorder()
	server.routes().ServeHTTP(secondPage, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables?pageToken="+first.NextPageToken+"&maxResults=2", nil))
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, body = %s", secondPage.Code, secondPage.Body.String())
	}
	var second tablesListResponse
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.TotalItems != 3 || second.NextPageToken != "" || len(second.Tables) != 1 || second.Tables[0].TableReference.TableID != "sessions" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestTablePartitioningAndClusteringMetadataPersists(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")

	createBody := `{
		"tableReference":{"tableId":"events"},
		"schema":{"fields":[
			{"name":"event_date","type":"DATE"},
			{"name":"tenant_id","type":"STRING"},
			{"name":"event_id","type":"STRING"}
		]},
		"timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"86400000","requirePartitionFilter":true},
		"clustering":{"fields":["tenant_id","event_id"]}
	}`
	create := httptest.NewRecorder()
	server.routes().ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(createBody)))
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	var created tableResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created table: %v", err)
	}
	if created.TimePartitioning == nil || created.TimePartitioning.Type != "DAY" || !created.TimePartitioning.RequireFilter {
		t.Fatalf("timePartitioning = %#v", created.TimePartitioning)
	}
	if created.Clustering == nil || strings.Join(created.Clustering.Fields, ",") != "tenant_id,event_id" {
		t.Fatalf("clustering = %#v", created.Clustering)
	}

	patch := httptest.NewRecorder()
	server.routes().ServeHTTP(patch, httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/local-project/datasets/analytics/tables/events", strings.NewReader(`{
		"rangePartitioning":{"field":"event_id","range":{"start":"1","end":"100","interval":"10"}},
		"clustering":{"fields":["tenant_id"]}
	}`)))
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", patch.Code, patch.Body.String())
	}
	var patched tableResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched table: %v", err)
	}
	if patched.TimePartitioning == nil || patched.TimePartitioning.Field != "event_date" {
		t.Fatalf("patch dropped timePartitioning = %#v", patched.TimePartitioning)
	}
	if patched.RangePartitioning == nil || patched.RangePartitioning.Range.Interval != "10" {
		t.Fatalf("rangePartitioning = %#v", patched.RangePartitioning)
	}
	if patched.Clustering == nil || strings.Join(patched.Clustering.Fields, ",") != "tenant_id" {
		t.Fatalf("patched clustering = %#v", patched.Clustering)
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var listed tablesListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Tables) != 1 || listed.Tables[0].TimePartitioning == nil || listed.Tables[0].Clustering == nil {
		t.Fatalf("listed table metadata = %#v", listed.Tables)
	}

	snapshot, found := server.TableSnapshot("local-project", "analytics", "events", 0)
	if !found {
		t.Fatal("table snapshot missing")
	}
	if snapshot.TimePartitioning == nil || snapshot.RangePartitioning == nil || snapshot.Clustering == nil {
		t.Fatalf("snapshot metadata = %#v", snapshot)
	}
}

func TestTableCreateRejectsMissingDatasetDuplicateAndInvalidSchema(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	body := `{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING"}]}}`

	missingDataset := httptest.NewRecorder()
	server.routes().ServeHTTP(missingDataset, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/missing/tables", strings.NewReader(body)))
	if missingDataset.Code != http.StatusNotFound {
		t.Fatalf("missing dataset status = %d, body = %s", missingDataset.Code, missingDataset.Body.String())
	}

	createDatasetForTest(t, server, "local-project", "analytics")
	first := httptest.NewRecorder()
	server.routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(body)))
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, body = %s", first.Code, first.Body.String())
	}
	duplicate := httptest.NewRecorder()
	server.routes().ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(body)))
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, body = %s", duplicate.Code, duplicate.Body.String())
	}

	invalidSchema := httptest.NewRecorder()
	server.routes().ServeHTTP(invalidSchema, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(`{"tableReference":{"tableId":"bad"},"schema":{"fields":[{"name":"payload","type":"UNSUPPORTED"}]}}`)))
	if invalidSchema.Code != http.StatusBadRequest {
		t.Fatalf("invalid schema status = %d, body = %s", invalidSchema.Code, invalidSchema.Body.String())
	}
}

func TestDatasetAndTableIAMPolicyCompatibilityStubs(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	getDefault := httptest.NewRecorder()
	server.routes().ServeHTTP(getDefault, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics:getIamPolicy", strings.NewReader(`{}`)))
	if getDefault.Code != http.StatusOK {
		t.Fatalf("default dataset policy status = %d, body = %s", getDefault.Code, getDefault.Body.String())
	}
	var defaultPolicy iamPolicy
	if err := json.NewDecoder(getDefault.Body).Decode(&defaultPolicy); err != nil {
		t.Fatalf("decode default policy: %v", err)
	}
	if defaultPolicy.Version != 1 || len(defaultPolicy.Bindings) != 0 || defaultPolicy.ETag == "" {
		t.Fatalf("default policy = %#v", defaultPolicy)
	}

	setTable := httptest.NewRecorder()
	setBody := `{"policy":{"version":1,"bindings":[{"role":"roles/bigquery.dataViewer","members":["user:local@example.com"]}]}}`
	server.routes().ServeHTTP(setTable, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:setIamPolicy", strings.NewReader(setBody)))
	if setTable.Code != http.StatusOK {
		t.Fatalf("set table policy status = %d, body = %s", setTable.Code, setTable.Body.String())
	}
	var setPolicy iamPolicy
	if err := json.NewDecoder(setTable.Body).Decode(&setPolicy); err != nil {
		t.Fatalf("decode set policy: %v", err)
	}
	if len(setPolicy.Bindings) != 1 || setPolicy.Bindings[0].Role != "roles/bigquery.dataViewer" || setPolicy.ETag == "" {
		t.Fatalf("set policy = %#v", setPolicy)
	}

	getTable := httptest.NewRecorder()
	server.routes().ServeHTTP(getTable, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:getIamPolicy", strings.NewReader(`{}`)))
	if getTable.Code != http.StatusOK || !strings.Contains(getTable.Body.String(), "roles/bigquery.dataViewer") {
		t.Fatalf("persisted table policy status/body = %d %s", getTable.Code, getTable.Body.String())
	}

	testPermissions := httptest.NewRecorder()
	permissionsBody := `{"permissions":["bigquery.tables.get","bigquery.tables.update"]}`
	server.routes().ServeHTTP(testPermissions, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:testIamPermissions", strings.NewReader(permissionsBody)))
	if testPermissions.Code != http.StatusOK || !strings.Contains(testPermissions.Body.String(), "bigquery.tables.update") {
		t.Fatalf("test permissions status/body = %d %s", testPermissions.Code, testPermissions.Body.String())
	}
}

func TestTableDataInsertAllPersistsRowsAndListReturnsBigQueryShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	insertBody := `{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},{"insertId":"row-2","json":{"id":"2","name":"Grace","age":"31","active":false}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var insertResponse insertAllResponse
	if err := json.NewDecoder(insert.Body).Decode(&insertResponse); err != nil {
		t.Fatalf("decode insert response: %v", err)
	}
	if insertResponse.Kind != "bigquery#tableDataInsertAllResponse" || len(insertResponse.InsertErrors) != 0 {
		t.Fatalf("insert response = %#v", insertResponse)
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data?maxResults=1", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var listResponse tableDataListResponse
	if err := json.NewDecoder(list.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if listResponse.Kind != "bigquery#tableDataList" || listResponse.TotalRows != "2" || listResponse.PageToken != "1" || len(listResponse.Rows) != 1 {
		t.Fatalf("list response = %#v", listResponse)
	}
	if got := listResponse.Rows[0].F[1].V; got != "Ada" {
		t.Fatalf("first row name = %#v", got)
	}

	next := httptest.NewRecorder()
	server.routes().ServeHTTP(next, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data?pageToken=1&selectedFields=name", nil))
	if next.Code != http.StatusOK {
		t.Fatalf("next status = %d, body = %s", next.Code, next.Body.String())
	}
	var nextResponse tableDataListResponse
	if err := json.NewDecoder(next.Body).Decode(&nextResponse); err != nil {
		t.Fatalf("decode next response: %v", err)
	}
	if len(nextResponse.Rows) != 1 || len(nextResponse.Rows[0].F) != 1 || nextResponse.Rows[0].F[0].V != "Grace" {
		t.Fatalf("selected next response = %#v", nextResponse)
	}

	tableRec := httptest.NewRecorder()
	server.routes().ServeHTTP(tableRec, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people", nil))
	var table tableResource
	if err := json.NewDecoder(tableRec.Body).Decode(&table); err != nil {
		t.Fatalf("decode table: %v", err)
	}
	if table.NumRows != "2" || table.NumBytes == "0" {
		t.Fatalf("table stats = %#v", table)
	}
}

func TestTableDataInsertAllReturnsPartialErrorsAndHonorsSkipInvalidRows(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	insertBody := `{"skipInvalidRows":true,"ignoreUnknownValues":true,"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","extra":"ignored"}},{"insertId":"row-2","json":{"name":"Missing ID","age":"old"}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var response insertAllResponse
	if err := json.NewDecoder(insert.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.InsertErrors) != 1 || response.InsertErrors[0].Index != 1 {
		t.Fatalf("insert errors = %#v", response.InsertErrors)
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var listResponse tableDataListResponse
	if err := json.NewDecoder(list.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listResponse.TotalRows != "1" || len(listResponse.Rows) != 1 || listResponse.Rows[0].F[1].V != "Ada" {
		t.Fatalf("list response after partial insert = %#v", listResponse)
	}

	blocked := httptest.NewRecorder()
	blockedBody := `{"rows":[{"insertId":"row-3","json":{"id":"3","name":"Katherine"}},{"insertId":"row-4","json":{"name":"Invalid"}}]}`
	server.routes().ServeHTTP(blocked, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(blockedBody)))
	if blocked.Code != http.StatusOK {
		t.Fatalf("blocked status = %d, body = %s", blocked.Code, blocked.Body.String())
	}
	afterBlocked := httptest.NewRecorder()
	server.routes().ServeHTTP(afterBlocked, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var afterBlockedResponse tableDataListResponse
	if err := json.NewDecoder(afterBlocked.Body).Decode(&afterBlockedResponse); err != nil {
		t.Fatalf("decode after blocked: %v", err)
	}
	if afterBlockedResponse.TotalRows != "1" {
		t.Fatalf("skipInvalidRows=false persisted valid rows from invalid request: %#v", afterBlockedResponse)
	}
}

func TestTableDataInsertAllBestEffortDeduplicatesInsertIDs(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	first := httptest.NewRecorder()
	firstBody := `{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}`
	server.routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(firstBody)))
	if first.Code != http.StatusOK {
		t.Fatalf("first insert status = %d, body = %s", first.Code, first.Body.String())
	}

	duplicate := httptest.NewRecorder()
	duplicateBody := `{"rows":[
		{"insertId":"row-1","json":{"id":"1-duplicate","name":"Duplicate","age":99,"active":false}},
		{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}},
		{"insertId":"row-2","json":{"id":"2-duplicate","name":"Duplicate Grace","age":32,"active":false}}
	]}`
	server.routes().ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(duplicateBody)))
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate insert status = %d, body = %s", duplicate.Code, duplicate.Body.String())
	}
	var insertResponse insertAllResponse
	if err := json.NewDecoder(duplicate.Body).Decode(&insertResponse); err != nil {
		t.Fatalf("decode duplicate insert response: %v", err)
	}
	if len(insertResponse.InsertErrors) != 0 {
		t.Fatalf("duplicate insert errors = %#v", insertResponse.InsertErrors)
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var listResponse tableDataListResponse
	if err := json.NewDecoder(list.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listResponse.TotalRows != "2" || len(listResponse.Rows) != 2 {
		t.Fatalf("deduplicated rows = %#v", listResponse)
	}
	if listResponse.Rows[0].F[1].V != "Ada" || listResponse.Rows[1].F[1].V != "Grace" {
		t.Fatalf("duplicate insertId rows were persisted: %#v", listResponse.Rows)
	}
}

func TestTableDataInsertAllHonorsConfiguredRequestAndRowLimits(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir(), MaxRowsPerTable: 1})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	server.config.MaxRequestBytes = 64

	tooLarge := httptest.NewRecorder()
	tooLargeBody := `{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}`
	server.routes().ServeHTTP(tooLarge, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(tooLargeBody)))
	if tooLarge.Code != http.StatusBadRequest {
		t.Fatalf("large request status = %d, body = %s", tooLarge.Code, tooLarge.Body.String())
	}
	if strings.Contains(tooLarge.Body.String(), "Ada") {
		t.Fatalf("large request error leaked row payload: %s", tooLarge.Body.String())
	}

	server.config.MaxRequestBytes = 1024
	first := httptest.NewRecorder()
	firstBody := `{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}`
	server.routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(firstBody)))
	if first.Code != http.StatusOK {
		t.Fatalf("first insert status = %d, body = %s", first.Code, first.Body.String())
	}

	overLimit := httptest.NewRecorder()
	overLimitBody := `{"rows":[{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":false}}]}`
	server.routes().ServeHTTP(overLimit, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(overLimitBody)))
	if overLimit.Code != http.StatusBadRequest {
		t.Fatalf("row limit status = %d, body = %s", overLimit.Code, overLimit.Body.String())
	}
	if strings.Contains(overLimit.Body.String(), "Grace") {
		t.Fatalf("row limit error leaked row payload: %s", overLimit.Body.String())
	}

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var listResponse tableDataListResponse
	if err := json.NewDecoder(list.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listResponse.TotalRows != "1" || len(listResponse.Rows) != 1 || listResponse.Rows[0].F[1].V != "Ada" {
		t.Fatalf("rows after limit rejection = %#v", listResponse)
	}
}

func TestJobsQueryPersistsResultsAndGetQueryResultsReturnsBigQueryShape(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + ` WHERE age >= 30 ORDER BY id","useLegacySql":false,"location":"US"}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.Kind != "bigquery#queryResponse" || !response.JobComplete || response.TotalRows != "2" {
		t.Fatalf("query response = %#v", response)
	}
	if response.JobReference.ProjectID != "local-project" || response.JobReference.JobID == "" || response.JobReference.Location != "US" {
		t.Fatalf("job reference = %#v", response.JobReference)
	}
	if len(response.Schema.Fields) != 2 || response.Schema.Fields[0].Name != "id" || response.Schema.Fields[1].Name != "age" {
		t.Fatalf("schema = %#v", response.Schema)
	}
	if len(response.Rows) != 2 || response.Rows[0].F[0].V != "1" {
		t.Fatalf("rows = %#v", response.Rows)
	}

	results := httptest.NewRecorder()
	server.routes().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/queries/"+response.JobReference.JobID+"?maxResults=1", nil))
	if results.Code != http.StatusOK {
		t.Fatalf("results status = %d, body = %s", results.Code, results.Body.String())
	}
	var resultsResponse queryResponse
	if err := json.NewDecoder(results.Body).Decode(&resultsResponse); err != nil {
		t.Fatalf("decode results response: %v", err)
	}
	if !resultsResponse.JobComplete || resultsResponse.TotalRows != "2" || resultsResponse.PageToken != "1" || len(resultsResponse.Rows) != 1 {
		t.Fatalf("results response = %#v", resultsResponse)
	}

	canonicalResults := httptest.NewRecorder()
	server.routes().ServeHTTP(canonicalResults, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/"+response.JobReference.JobID+"/getQueryResults?maxResults=1", nil))
	if canonicalResults.Code != http.StatusOK {
		t.Fatalf("canonical results status = %d, body = %s", canonicalResults.Code, canonicalResults.Body.String())
	}
	var canonicalResponse queryResponse
	if err := json.NewDecoder(canonicalResults.Body).Decode(&canonicalResponse); err != nil {
		t.Fatalf("decode canonical results response: %v", err)
	}
	if !canonicalResponse.JobComplete || canonicalResponse.TotalRows != "2" || canonicalResponse.PageToken != "1" || len(canonicalResponse.Rows) != 1 {
		t.Fatalf("canonical results response = %#v", canonicalResponse)
	}

	job := httptest.NewRecorder()
	server.routes().ServeHTTP(job, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/"+response.JobReference.JobID, nil))
	if job.Code != http.StatusOK || !strings.Contains(job.Body.String(), `"state":"DONE"`) {
		t.Fatalf("job status/body = %d %s", job.Code, job.Body.String())
	}
}

func TestJobsQueryRejectsLegacySQLWithoutLeakingQueryText(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT token FROM secret_sensitive_table","useLegacySql":true}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("legacy query status = %d, body = %s", query.Code, query.Body.String())
	}
	if !strings.Contains(query.Body.String(), "legacy SQL is not supported") {
		t.Fatalf("legacy query error missing reason: %s", query.Body.String())
	}
	if strings.Contains(query.Body.String(), "secret_sensitive_table") || strings.Contains(query.Body.String(), "token") {
		t.Fatalf("legacy query error leaked query text: %s", query.Body.String())
	}
}

func TestJobsQueryUsesConfiguredDefaultLegacySQLWhenUseLegacySQLIsMissing(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), DefaultLegacySQL: true})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + `"}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("default legacy query status = %d, body = %s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "local-project.analytics.people") {
		t.Fatalf("default legacy query error leaked query text: %s", query.Body.String())
	}
}

func TestJobsInsertQueryJobCanOverrideDefaultLegacySQL(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), DefaultLegacySQL: true})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"standard_sql_job","location":"US"},"configuration":{"query":{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` ORDER BY id","useLegacySql":false}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var rawJob struct {
		Statistics map[string]any `json:"statistics"`
	}
	if err := json.NewDecoder(bytes.NewReader(insert.Body.Bytes())).Decode(&rawJob); err != nil {
		t.Fatalf("decode raw job: %v", err)
	}
	for _, field := range []string{"creationTime", "startTime", "endTime"} {
		if _, ok := rawJob.Statistics[field].(string); !ok {
			t.Fatalf("statistics.%s = %#v, want JSON string", field, rawJob.Statistics[field])
		}
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if job.JobReference.JobID != "standard_sql_job" || job.Configuration.Query.UseLegacySQL == nil || *job.Configuration.Query.UseLegacySQL {
		t.Fatalf("job query configuration = %#v", job.Configuration.Query)
	}
}

func TestJobsQueryMaxResultsPagesResponseWithoutTruncatingPersistedResults(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` ORDER BY id","useLegacySql":false,"maxResults":1}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || response.PageToken != "1" || len(response.Rows) != 1 {
		t.Fatalf("paged query response = %#v", response)
	}

	results := httptest.NewRecorder()
	server.routes().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/queries/"+response.JobReference.JobID, nil))
	if results.Code != http.StatusOK {
		t.Fatalf("results status = %d, body = %s", results.Code, results.Body.String())
	}
	var resultsResponse queryResponse
	if err := json.NewDecoder(results.Body).Decode(&resultsResponse); err != nil {
		t.Fatalf("decode results response: %v", err)
	}
	if resultsResponse.TotalRows != "2" || len(resultsResponse.Rows) != 2 || resultsResponse.Rows[1].F[1].V != "Grace" {
		t.Fatalf("persisted results were truncated: %#v", resultsResponse)
	}
}

func TestJobsQueryHonorsConfiguredMaxResultRows(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), MaxResultRows: 1})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` ORDER BY id","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || response.PageToken != "1" || len(response.Rows) != 1 {
		t.Fatalf("configured page response = %#v", response)
	}
}

func TestJobsQueryDryRunValidatesAndReturnsSchemaWithoutRows(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + ` WHERE age >= 30","useLegacySql":false,"dryRun":true}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("dry run status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode dry run response: %v", err)
	}
	if response.Kind != "bigquery#queryResponse" || !response.JobComplete || response.TotalRows != "0" || len(response.Rows) != 0 {
		t.Fatalf("dry run response = %#v", response)
	}
	if len(response.Schema.Fields) != 2 || response.Schema.Fields[0].Name != "id" || response.Schema.Fields[1].Name != "age" {
		t.Fatalf("dry run schema = %#v", response.Schema)
	}

	job := httptest.NewRecorder()
	server.routes().ServeHTTP(job, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/"+response.JobReference.JobID, nil))
	if job.Code != http.StatusOK {
		t.Fatalf("dry run job status = %d, body = %s", job.Code, job.Body.String())
	}
	var resource jobResource
	if err := json.NewDecoder(job.Body).Decode(&resource); err != nil {
		t.Fatalf("decode dry run job: %v", err)
	}
	if !resource.Statistics.Query.DryRun || resource.Statistics.Query.TotalRows != "0" {
		t.Fatalf("dry run job statistics = %#v", resource.Statistics.Query)
	}
}

func TestJobsQuerySupportsLimitOffset(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` ORDER BY id LIMIT 1 OFFSET 1","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "2" || response.Rows[0].F[1].V != "Grace" {
		t.Fatalf("offset row = %#v", response.Rows[0])
	}
}

func TestJobsQuerySupportsOrderByDesc(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + ` ORDER BY age DESC","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || len(response.Rows) != 2 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "37" {
		t.Fatalf("desc first row = %#v", response.Rows[0])
	}
	if response.Rows[1].F[0].V != "2" || response.Rows[1].F[1].V != "31" {
		t.Fatalf("desc second row = %#v", response.Rows[1])
	}
}

func TestJobsQuerySupportsWhereAndComparisons(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` WHERE age >= 30 AND active = true AND id != '2'","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "Ada" {
		t.Fatalf("filtered row = %#v", response.Rows[0])
	}
}

func TestJobsQuerySupportsNamedScalarQueryParameters(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{
		"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` WHERE age >= @min_age AND active = @active ORDER BY id",
		"useLegacySql":false,
		"queryParameters":[
			{"name":"min_age","parameterType":{"type":"INT64"},"parameterValue":{"value":"35"}},
			{"name":"active","parameterType":{"type":"BOOL"},"parameterValue":{"value":"true"}}
		]
	}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("parameter query response = %#v", response)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "Ada" {
		t.Fatalf("parameter filtered row = %#v", response.Rows[0])
	}
}

func TestJobsInsertQueryJobPersistsNamedQueryParameters(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{
		"jobReference":{"jobId":"parameterized_query","location":"US"},
		"configuration":{"query":{
			"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` WHERE name = @name",
			"useLegacySql":false,
			"queryParameters":[{"name":"name","parameterType":{"type":"STRING"},"parameterValue":{"value":"Grace"}}]
		}}
	}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if len(job.Configuration.Query.QueryParameters) != 1 || job.Configuration.Query.QueryParameters[0].Name != "name" {
		t.Fatalf("query parameters were not persisted: %#v", job.Configuration.Query.QueryParameters)
	}

	results := httptest.NewRecorder()
	server.routes().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/queries/parameterized_query", nil))
	if results.Code != http.StatusOK {
		t.Fatalf("results status = %d, body = %s", results.Code, results.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(results.Body).Decode(&response); err != nil {
		t.Fatalf("decode results: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 || response.Rows[0].F[0].V != "2" {
		t.Fatalf("parameterized job results = %#v", response)
	}
}

func TestJobsQueryRejectsMissingParameterWithoutLeakingQueryText(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` WHERE name = @secret_name","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "local-project.analytics.people") {
		t.Fatalf("parameter error leaked query text: %s", query.Body.String())
	}
}

func TestJobsQuerySupportsOrWhere(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` WHERE name = 'Ada' OR age < 35 ORDER BY id","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || len(response.Rows) != 2 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "Ada" {
		t.Fatalf("first OR row = %#v", response.Rows[0])
	}
	if response.Rows[1].F[0].V != "2" || response.Rows[1].F[1].V != "Grace" {
		t.Fatalf("second OR row = %#v", response.Rows[1])
	}
}

func TestJobsQuerySupportsNotWhere(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.people`" + ` WHERE NOT name = 'Grace' AND NOT age < 35","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "Ada" {
		t.Fatalf("NOT filtered row = %#v", response.Rows[0])
	}
}

func TestJobsQueryRejectsMalformedNotWhereWithoutLeakingQueryText(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` WHERE NOT secret_value","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "secret_value") || strings.Contains(query.Body.String(), "local-project.analytics.people") {
		t.Fatalf("query error leaked query text: %s", query.Body.String())
	}
}

func TestJobsQueryRejectsMalformedOrWhereWithoutLeakingQueryText(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` WHERE name = 'secret-value' OR","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "secret-value") || strings.Contains(query.Body.String(), "local-project.analytics.people") {
		t.Fatalf("query error leaked query text: %s", query.Body.String())
	}
}

func TestJobsQuerySupportsCountAggregate(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT COUNT(*) AS total FROM ` + "`local-project.analytics.people`" + ` WHERE active = true","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("response rows = %#v", response)
	}
	if len(response.Schema.Fields) != 1 || response.Schema.Fields[0].Name != "total" || response.Schema.Fields[0].Type != "INTEGER" {
		t.Fatalf("schema = %#v", response.Schema)
	}
	if response.Rows[0].F[0].V != "2" {
		t.Fatalf("count value = %#v", response.Rows[0].F[0].V)
	}
}

func TestJobsQuerySupportsCountFieldAggregate(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insert := httptest.NewRecorder()
	insertBody := `{"rows":[{"insertId":"row-3","json":{"id":"3","name":"No Age","active":true}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}

	query := httptest.NewRecorder()
	body := `{"query":"SELECT COUNT(age) FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(response.Schema.Fields) != 1 || response.Schema.Fields[0].Name != "f0_" {
		t.Fatalf("schema = %#v", response.Schema)
	}
	if len(response.Rows) != 1 || response.Rows[0].F[0].V != "2" {
		t.Fatalf("count field response = %#v", response)
	}
}

func TestJobsQuerySupportsNumericAggregates(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	tests := []struct {
		name      string
		selector  string
		fieldName string
		fieldType string
		value     any
	}{
		{name: "sum", selector: "SUM(age) AS total_age", fieldName: "total_age", fieldType: "INTEGER", value: "68"},
		{name: "avg", selector: "AVG(age) AS average_age", fieldName: "average_age", fieldType: "FLOAT", value: "34"},
		{name: "min", selector: "MIN(age) AS youngest", fieldName: "youngest", fieldType: "INTEGER", value: float64(31)},
		{name: "max", selector: "MAX(age) AS oldest", fieldName: "oldest", fieldType: "INTEGER", value: float64(37)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := httptest.NewRecorder()
			body := `{"query":"SELECT ` + tt.selector + ` FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false}`
			server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
			if query.Code != http.StatusOK {
				t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
			}
			var response queryResponse
			if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
				t.Fatalf("decode query response: %v", err)
			}
			if len(response.Schema.Fields) != 1 || response.Schema.Fields[0].Name != tt.fieldName || response.Schema.Fields[0].Type != tt.fieldType {
				t.Fatalf("schema = %#v", response.Schema)
			}
			if len(response.Rows) != 1 || response.Rows[0].F[0].V != tt.value {
				t.Fatalf("aggregate response = %#v", response)
			}
		})
	}
}

func TestJobsQuerySupportsGroupedCountAggregate(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insert := httptest.NewRecorder()
	insertBody := `{"rows":[{"insertId":"row-3","json":{"id":"3","name":"Margaret","age":29,"active":false}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}

	query := httptest.NewRecorder()
	body := `{"query":"SELECT active, COUNT(*) AS total FROM ` + "`local-project.analytics.people`" + ` GROUP BY active ORDER BY active","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || len(response.Rows) != 2 {
		t.Fatalf("response rows = %#v", response)
	}
	if len(response.Schema.Fields) != 2 || response.Schema.Fields[0].Name != "active" || response.Schema.Fields[1].Name != "total" {
		t.Fatalf("schema = %#v", response.Schema)
	}
	if response.Rows[0].F[0].V != "false" || response.Rows[0].F[1].V != "1" {
		t.Fatalf("false group = %#v", response.Rows[0])
	}
	if response.Rows[1].F[0].V != "true" || response.Rows[1].F[1].V != "2" {
		t.Fatalf("true group = %#v", response.Rows[1])
	}
}

func TestJobsQuerySupportsGroupedOrderByDesc(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insert := httptest.NewRecorder()
	insertBody := `{"rows":[{"insertId":"row-3","json":{"id":"3","name":"Margaret","age":29,"active":false}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}

	query := httptest.NewRecorder()
	body := `{"query":"SELECT active, COUNT(*) AS total FROM ` + "`local-project.analytics.people`" + ` GROUP BY active ORDER BY active DESC","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if response.TotalRows != "2" || len(response.Rows) != 2 {
		t.Fatalf("response rows = %#v", response)
	}
	if response.Rows[0].F[0].V != "true" || response.Rows[0].F[1].V != "2" {
		t.Fatalf("true group first = %#v", response.Rows[0])
	}
	if response.Rows[1].F[0].V != "false" || response.Rows[1].F[1].V != "1" {
		t.Fatalf("false group second = %#v", response.Rows[1])
	}
}

func TestJobsQueryRejectsGroupByFieldMismatch(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	query := httptest.NewRecorder()
	body := `{"query":"SELECT active, COUNT(*) AS total FROM ` + "`local-project.analytics.people`" + ` GROUP BY name","useLegacySql":false}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusBadRequest {
		t.Fatalf("query status = %d, body = %s", query.Code, query.Body.String())
	}
}

func TestJobsInsertQueryJobPersistsResults(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"job_123","location":"US"},"configuration":{"query":{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + ` WHERE age >= 30 ORDER BY id","useLegacySql":false}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "job_123" || job.Status.State != "DONE" {
		t.Fatalf("job = %#v", job)
	}
	if job.Statistics.Query.TotalRows != "2" {
		t.Fatalf("job statistics = %#v", job.Statistics)
	}

	get := httptest.NewRecorder()
	server.routes().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/job_123", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"jobId":"job_123"`) {
		t.Fatalf("get status/body = %d %s", get.Code, get.Body.String())
	}

	results := httptest.NewRecorder()
	server.routes().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/queries/job_123", nil))
	if results.Code != http.StatusOK {
		t.Fatalf("results status = %d, body = %s", results.Code, results.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(results.Body).Decode(&response); err != nil {
		t.Fatalf("decode results: %v", err)
	}
	if response.TotalRows != "2" || len(response.Rows) != 2 {
		t.Fatalf("results response = %#v", response)
	}
}

func TestJobsInsertQueryJobDryRunPersistsSchemaOnly(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"dry_run_job","location":"US"},"configuration":{"dryRun":true,"query":{"query":"SELECT COUNT(*) AS total FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("dry run insert status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode dry run job: %v", err)
	}
	if !job.Configuration.DryRun || !job.Statistics.Query.DryRun || job.Statistics.Query.TotalRows != "0" {
		t.Fatalf("dry run job = %#v", job)
	}

	results := httptest.NewRecorder()
	server.routes().ServeHTTP(results, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/queries/dry_run_job", nil))
	if results.Code != http.StatusOK {
		t.Fatalf("dry run results status = %d, body = %s", results.Code, results.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(results.Body).Decode(&response); err != nil {
		t.Fatalf("decode dry run results: %v", err)
	}
	if response.TotalRows != "0" || len(response.Rows) != 0 || len(response.Schema.Fields) != 1 || response.Schema.Fields[0].Name != "total" {
		t.Fatalf("dry run results = %#v", response)
	}
}

func TestJobsListCancelAndDeleteMetadata(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insertQueryJobForTest(t, server, "local-project", "job_a")
	insertQueryJobForTest(t, server, "local-project", "job_b")

	list := httptest.NewRecorder()
	server.routes().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs?maxResults=1", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var listResponse jobsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if listResponse.Kind != "bigquery#jobList" || listResponse.NextPageToken != "1" || len(listResponse.Jobs) != 1 || listResponse.Jobs[0].JobReference.JobID != "job_a" {
		t.Fatalf("list response = %#v", listResponse)
	}

	cancel := httptest.NewRecorder()
	server.routes().ServeHTTP(cancel, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs/job_a/cancel", nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body = %s", cancel.Code, cancel.Body.String())
	}
	var cancelResponse jobCancelResponse
	if err := json.NewDecoder(cancel.Body).Decode(&cancelResponse); err != nil {
		t.Fatalf("decode cancel: %v", err)
	}
	if cancelResponse.Kind != "bigquery#jobCancelResponse" || cancelResponse.Job.Status.State != "DONE" {
		t.Fatalf("cancel response = %#v", cancelResponse)
	}

	deleteRec := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteRec, httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/local-project/jobs/job_a/delete", nil))
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missing := httptest.NewRecorder()
	server.routes().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/job_a", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s", missing.Code, missing.Body.String())
	}

	deleteStandard := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteStandard, httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/local-project/jobs/job_b", nil))
	if deleteStandard.Code != http.StatusNoContent {
		t.Fatalf("standard delete status = %d, body = %s", deleteStandard.Code, deleteStandard.Body.String())
	}
	missingStandard := httptest.NewRecorder()
	server.routes().ServeHTTP(missingStandard, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/jobs/job_b", nil))
	if missingStandard.Code != http.StatusNotFound {
		t.Fatalf("standard missing status = %d, body = %s", missingStandard.Code, missingStandard.Body.String())
	}
}

func TestJobsInsertCopyJobCopiesTableMetadataAndRows(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"copy_people","location":"US"},"configuration":{"copy":{"sourceTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people"},"destinationTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people_copy"},"createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_TRUNCATE"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("copy job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode copy job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "copy_people" || job.Status.State != "DONE" {
		t.Fatalf("copy job = %#v", job)
	}
	if job.Configuration.Copy.DestinationTable.TableID != "people_copy" {
		t.Fatalf("copy configuration = %#v", job.Configuration.Copy)
	}

	tableRec := httptest.NewRecorder()
	server.routes().ServeHTTP(tableRec, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people_copy", nil))
	if tableRec.Code != http.StatusOK {
		t.Fatalf("copied table status = %d, body = %s", tableRec.Code, tableRec.Body.String())
	}
	var table tableResource
	if err := json.NewDecoder(tableRec.Body).Decode(&table); err != nil {
		t.Fatalf("decode copied table: %v", err)
	}
	if table.TableReference.TableID != "people_copy" || table.NumRows != "2" || len(table.Schema.Fields) != 4 {
		t.Fatalf("copied table = %#v", table)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people_copy/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode copied rows: %v", err)
	}
	if rowList.TotalRows != "2" || len(rowList.Rows) != 2 || rowList.Rows[0].F[1].V != "Ada" {
		t.Fatalf("copied rows = %#v", rowList)
	}
}

func TestJobsInsertCopyJobCopiesMultipleSourceTables(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people_a")
	createTableForTest(t, server, "local-project", "analytics", "people_b")
	insertRowsForTest(t, server, "local-project", "analytics", "people_a")
	insertRowsForTest(t, server, "local-project", "analytics", "people_b")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"copy_many_people","location":"US"},"configuration":{"copy":{"sourceTables":[{"projectId":"local-project","datasetId":"analytics","tableId":"people_a"},{"projectId":"local-project","datasetId":"analytics","tableId":"people_b"}],"destinationTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people_many"},"createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_TRUNCATE"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("copy job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode copy job: %v", err)
	}
	if len(job.Configuration.Copy.SourceTables) != 2 || job.Configuration.Copy.DestinationTable.TableID != "people_many" {
		t.Fatalf("copy configuration = %#v", job.Configuration.Copy)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people_many/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode copied rows: %v", err)
	}
	if rowList.TotalRows != "4" || len(rowList.Rows) != 4 {
		t.Fatalf("copied rows = %#v", rowList)
	}
}

func TestJobsInsertCopyJobHonorsCreateNeverAndWriteEmpty(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	createNever := httptest.NewRecorder()
	createNeverBody := `{"configuration":{"copy":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationTable":{"datasetId":"analytics","tableId":"missing"},"createDisposition":"CREATE_NEVER"}}}`
	server.routes().ServeHTTP(createNever, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(createNeverBody)))
	if createNever.Code != http.StatusBadRequest {
		t.Fatalf("CREATE_NEVER status = %d, body = %s", createNever.Code, createNever.Body.String())
	}

	createTableForTest(t, server, "local-project", "analytics", "occupied")
	insertRowsForTest(t, server, "local-project", "analytics", "occupied")
	writeEmpty := httptest.NewRecorder()
	writeEmptyBody := `{"configuration":{"copy":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationTable":{"datasetId":"analytics","tableId":"occupied"},"writeDisposition":"WRITE_EMPTY"}}}`
	server.routes().ServeHTTP(writeEmpty, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(writeEmptyBody)))
	if writeEmpty.Code != http.StatusBadRequest {
		t.Fatalf("WRITE_EMPTY status = %d, body = %s", writeEmpty.Code, writeEmpty.Body.String())
	}
}

func TestJobsInsertLoadJobReadsGCSNDJSON(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-fixtures"); err != nil || !created {
		t.Fatalf("create fixture bucket: created=%t err=%v", created, err)
	}
	if _, err := objectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "bq-fixtures",
		Key:         "people.ndjson",
		Body:        strings.NewReader("{\"id\":\"3\",\"name\":\"Katherine\",\"age\":42,\"active\":true}\n{\"id\":\"4\",\"name\":\"Edsger\",\"age\":\"51\",\"active\":false}\n"),
		ContentType: "application/x-ndjson",
	}); err != nil {
		t.Fatalf("put fixture object: %v", err)
	}

	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"load_people","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("load job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode load job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "load_people" || job.Status.State != "DONE" {
		t.Fatalf("load job = %#v", job)
	}
	if job.Configuration.Load.DestinationTable.TableID != "people" {
		t.Fatalf("load configuration = %#v", job.Configuration.Load)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode loaded rows: %v", err)
	}
	if rowList.TotalRows != "2" || len(rowList.Rows) != 2 || rowList.Rows[0].F[1].V != "Katherine" {
		t.Fatalf("loaded rows = %#v", rowList)
	}
}

func TestJobsInsertLoadJobCanCreateDestinationTableWithSchema(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-fixtures"); err != nil || !created {
		t.Fatalf("create fixture bucket: created=%t err=%v", created, err)
	}
	if _, err := objectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "bq-fixtures",
		Key:         "new-people.ndjson",
		Body:        strings.NewReader("{\"id\":\"5\",\"name\":\"Dorothy\",\"age\":36,\"active\":true}\n"),
		ContentType: "application/x-ndjson",
	}); err != nil {
		t.Fatalf("put fixture object: %v", err)
	}

	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"load_new_people","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/new-people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"new_people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"},{"name":"active","type":"BOOLEAN"}]},"sourceFormat":"NEWLINE_DELIMITED_JSON","createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_APPEND"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("load job status = %d, body = %s", insert.Code, insert.Body.String())
	}

	tableRec := httptest.NewRecorder()
	server.routes().ServeHTTP(tableRec, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/new_people", nil))
	if tableRec.Code != http.StatusOK {
		t.Fatalf("created table status = %d, body = %s", tableRec.Code, tableRec.Body.String())
	}
	var table tableResource
	if err := json.NewDecoder(tableRec.Body).Decode(&table); err != nil {
		t.Fatalf("decode created table: %v", err)
	}
	if table.TableReference.TableID != "new_people" || table.NumRows != "1" || len(table.Schema.Fields) != 4 {
		t.Fatalf("created load table = %#v", table)
	}

	createNever := httptest.NewRecorder()
	createNeverBody := `{"configuration":{"load":{"sourceUris":["gs://bq-fixtures/new-people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"missing"},"schema":{"fields":[{"name":"id","type":"STRING"}]},"sourceFormat":"NEWLINE_DELIMITED_JSON","createDisposition":"CREATE_NEVER"}}}`
	server.routes().ServeHTTP(createNever, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(createNeverBody)))
	if createNever.Code != http.StatusBadRequest {
		t.Fatalf("CREATE_NEVER status = %d, body = %s", createNever.Code, createNever.Body.String())
	}
}

func TestJobsInsertLoadJobReadsGCSCSV(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-fixtures"); err != nil || !created {
		t.Fatalf("create fixture bucket: created=%t err=%v", created, err)
	}
	if _, err := objectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "bq-fixtures",
		Key:         "people.csv",
		Body:        strings.NewReader("6,Barbara,39,true\n7,Donald,44,false\n"),
		ContentType: "text/csv",
	}); err != nil {
		t.Fatalf("put fixture object: %v", err)
	}

	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"load_people_csv","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people.csv"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"CSV","writeDisposition":"WRITE_APPEND"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("CSV load job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode CSV load job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "load_people_csv" || job.Status.State != "DONE" {
		t.Fatalf("CSV load job = %#v", job)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode CSV loaded rows: %v", err)
	}
	if rowList.TotalRows != "2" || len(rowList.Rows) != 2 || rowList.Rows[0].F[1].V != "Barbara" || rowList.Rows[0].F[2].V != "39" {
		t.Fatalf("CSV loaded rows = %#v", rowList)
	}
}

func TestJobsInsertLoadJobCSVSkipsLeadingRows(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-fixtures"); err != nil || !created {
		t.Fatalf("create fixture bucket: created=%t err=%v", created, err)
	}
	if _, err := objectStore.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "bq-fixtures",
		Key:         "people-with-header.csv",
		Body:        strings.NewReader("id,name,age,active\n8,Joan,41,true\n"),
		ContentType: "text/csv",
	}); err != nil {
		t.Fatalf("put fixture object: %v", err)
	}

	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"load_people_csv_header","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people-with-header.csv"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"CSV","skipLeadingRows":1,"writeDisposition":"WRITE_APPEND"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("CSV load job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode CSV load job: %v", err)
	}
	if job.Configuration.Load.SkipLeadingRows != 1 {
		t.Fatalf("skipLeadingRows = %d", job.Configuration.Load.SkipLeadingRows)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode CSV loaded rows: %v", err)
	}
	if rowList.TotalRows != "1" || len(rowList.Rows) != 1 || rowList.Rows[0].F[0].V != "8" || rowList.Rows[0].F[1].V != "Joan" {
		t.Fatalf("CSV loaded rows = %#v", rowList)
	}
}

func TestJobsInsertUploadLoadJobReadsMultipartNDJSON(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadata, err := writer.CreatePart(map[string][]string{
		"Content-Type": {"application/json; charset=UTF-8"},
	})
	if err != nil {
		t.Fatalf("create metadata part: %v", err)
	}
	_, _ = metadata.Write([]byte(`{"jobReference":{"jobId":"upload_load_people","location":"US"},"configuration":{"load":{"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}`))
	media, err := writer.CreatePart(map[string][]string{
		"Content-Type": {"application/octet-stream"},
	})
	if err != nil {
		t.Fatalf("create media part: %v", err)
	}
	_, _ = media.Write([]byte("{\"id\":\"5\",\"name\":\"Dorothy\",\"age\":36,\"active\":true}\n"))
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	insert := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/local-project/jobs?uploadType=multipart", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	server.routes().ServeHTTP(insert, req)
	if insert.Code != http.StatusOK {
		t.Fatalf("upload load job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode upload load job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "upload_load_people" || job.Status.State != "DONE" {
		t.Fatalf("upload load job = %#v", job)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode uploaded rows: %v", err)
	}
	if rowList.TotalRows != "1" || len(rowList.Rows) != 1 || rowList.Rows[0].F[1].V != "Dorothy" {
		t.Fatalf("uploaded rows = %#v", rowList)
	}
}

func TestJobsInsertExtractJobWritesGCSNDJSON(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-exports"); err != nil || !created {
		t.Fatalf("create export bucket: created=%t err=%v", created, err)
	}
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"extract_people","location":"US"},"configuration":{"extract":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationUris":["gs://bq-exports/people.ndjson"],"destinationFormat":"NEWLINE_DELIMITED_JSON"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("extract job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode extract job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "extract_people" || job.Status.State != "DONE" {
		t.Fatalf("extract job = %#v", job)
	}

	object, bodyBytes, found, err := objectStore.GetObject(ctx, "bq-exports", "people.ndjson")
	if err != nil || !found {
		t.Fatalf("get exported object: found=%t err=%v", found, err)
	}
	if object.ContentType != "application/x-ndjson" {
		t.Fatalf("export content type = %q", object.ContentType)
	}
	bodyString := string(bodyBytes)
	if !strings.Contains(bodyString, `"name":"Ada"`) || !strings.Contains(bodyString, `"name":"Grace"`) {
		t.Fatalf("export body missing expected rows: %s", bodyString)
	}
}

func TestJobsInsertExtractJobWritesGCSCSV(t *testing.T) {
	ctx := context.Background()
	objectStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := objectStore.CreateBucket(ctx, "bq-exports"); err != nil || !created {
		t.Fatalf("create export bucket: created=%t err=%v", created, err)
	}
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir(), ObjectStore: objectStore})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"extract_people_csv","location":"US"},"configuration":{"extract":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationUris":["gs://bq-exports/people.csv"],"destinationFormat":"CSV"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("CSV extract job status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode CSV extract job: %v", err)
	}
	if job.Kind != "bigquery#job" || job.JobReference.JobID != "extract_people_csv" || job.Status.State != "DONE" {
		t.Fatalf("CSV extract job = %#v", job)
	}

	object, bodyBytes, found, err := objectStore.GetObject(ctx, "bq-exports", "people.csv")
	if err != nil || !found {
		t.Fatalf("get CSV exported object: found=%t err=%v", found, err)
	}
	if object.ContentType != "text/csv" {
		t.Fatalf("CSV export content type = %q", object.ContentType)
	}
	bodyString := string(bodyBytes)
	if !strings.Contains(bodyString, "1,Ada,37,true") || !strings.Contains(bodyString, "2,Grace,31,true") {
		t.Fatalf("CSV export body missing expected rows: %s", bodyString)
	}
}

func TestJobsQueryRejectsUnsupportedQueryWithoutLeakingQueryText(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})

	rec := httptest.NewRecorder()
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(`{"query":"DELETE FROM secret.dataset.table"}`)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret.dataset.table") {
		t.Fatalf("error response leaked query text: %s", rec.Body.String())
	}
}

func TestSnapshotsExposeDatasetsTablesRowsAndJobs(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insertQueryJobForTest(t, server, "local-project", "snapshot_job")

	snapshot := server.Snapshot()
	if !snapshot.Running || len(snapshot.Datasets) != 1 || len(snapshot.Datasets[0].Tables) != 1 || len(snapshot.Jobs) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	dataset, found := server.DatasetSnapshot("local-project", "analytics")
	if !found || dataset.DatasetID != "analytics" || len(dataset.Tables) != 1 {
		t.Fatalf("dataset snapshot found=%t value=%#v", found, dataset)
	}
	table, found := server.TableSnapshot("local-project", "analytics", "people", 1)
	if !found || table.TableID != "people" || len(table.Rows) != 1 || table.Rows[0].JSON["name"] != "Ada" {
		t.Fatalf("table snapshot found=%t value=%#v", found, table)
	}
	job, found := server.JobSnapshot("local-project", "snapshot_job")
	if !found || job.JobID != "snapshot_job" || job.State != "DONE" {
		t.Fatalf("job snapshot found=%t value=%#v", found, job)
	}
	if _, found := server.DatasetSnapshot("local-project", "missing"); found {
		t.Fatal("missing dataset snapshot found")
	}
}

func TestTableDataInsertAllValidatesNestedRepeatedAndFloatFields(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	rec := httptest.NewRecorder()
	body := `{"tableReference":{"tableId":"metrics"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"score","type":"FLOAT"},{"name":"tags","type":"STRING","mode":"REPEATED"},{"name":"meta","type":"RECORD","fields":[{"name":"active","type":"BOOLEAN","mode":"REQUIRED"}]},{"name":"payload","type":"JSON"}]}}`
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create metrics table status = %d, body = %s", rec.Code, rec.Body.String())
	}

	insert := httptest.NewRecorder()
	insertBody := `{"skipInvalidRows":true,"rows":[{"insertId":"ok","json":{"id":"1","score":"12.5","tags":["red","blue"],"meta":{"active":true},"payload":{"nested":1}}},{"insertId":"bad","json":{"id":"2","score":{"bad":true},"tags":"red","meta":{"active":"yes"}}}]}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables/metrics/insertAll", strings.NewReader(insertBody)))
	if insert.Code != http.StatusOK {
		t.Fatalf("insert metrics rows status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var response insertAllResponse
	if err := json.NewDecoder(insert.Body).Decode(&response); err != nil {
		t.Fatalf("decode insert response: %v", err)
	}
	if len(response.InsertErrors) != 1 || len(response.InsertErrors[0].Errors) != 3 {
		t.Fatalf("insert errors = %#v", response.InsertErrors)
	}
	table, found := server.TableSnapshot("local-project", "analytics", "metrics", 10)
	if !found || len(table.Rows) != 1 || table.Rows[0].JSON["score"] != "12.5" {
		t.Fatalf("metrics table snapshot found=%t value=%#v", found, table)
	}
}

func createDatasetForTest(t *testing.T, server *Server, projectID string, datasetID string) {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"datasetReference":{"datasetId":"` + datasetID + `"}}`
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/"+projectID+"/datasets", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create dataset status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func createTableForTest(t *testing.T, server *Server, projectID string, datasetID string, tableID string) {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"tableReference":{"tableId":"` + tableID + `"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"},{"name":"active","type":"BOOLEAN"}]}}`
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/"+projectID+"/datasets/"+datasetID+"/tables", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create table status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func insertRowsForTest(t *testing.T, server *Server, projectID string, datasetID string, tableID string) {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}}]}`
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/"+projectID+"/datasets/"+datasetID+"/tables/"+tableID+"/insertAll", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("insert rows status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func insertQueryJobForTest(t *testing.T, server *Server, projectID string, jobID string) {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"` + jobID + `","location":"US"},"configuration":{"query":{"query":"SELECT id, age FROM ` + "`" + projectID + `.analytics.people` + "`" + ` WHERE age >= 30 ORDER BY id","useLegacySql":false}}}`
	server.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/"+projectID+"/jobs", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("insert query job status = %d, body = %s", rec.Code, rec.Body.String())
	}
}
