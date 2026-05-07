package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestTableViewAndRoutineMetadataCatalogPersists(t *testing.T) {
	server := NewServer(Config{Project: "local-project", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")

	viewBody := `{
		"tableReference":{"tableId":"active_people"},
		"type":"VIEW",
		"view":{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + ` WHERE active = TRUE","useLegacySql":false}
	}`
	createView := httptest.NewRecorder()
	server.routes().ServeHTTP(createView, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(viewBody)))
	if createView.Code != http.StatusOK {
		t.Fatalf("create view status = %d, body = %s", createView.Code, createView.Body.String())
	}
	createViewResponse := createView.Body.String()
	var view tableResource
	if err := json.NewDecoder(strings.NewReader(createViewResponse)).Decode(&view); err != nil {
		t.Fatalf("decode view: %v", err)
	}
	if view.Type != "VIEW" || view.View == nil || !strings.Contains(view.View.Query, "active = TRUE") {
		t.Fatalf("view metadata = %#v", view)
	}
	if !strings.Contains(createViewResponse, `"useLegacySql":false`) {
		t.Fatalf("view response omitted useLegacySql=false: %s", createViewResponse)
	}

	createRoutineBody := `{
		"routineReference":{"routineId":"normalize_name"},
		"routineType":"SCALAR_FUNCTION",
		"language":"SQL",
		"arguments":[{"name":"name","dataType":{"typeKind":"STRING"}}],
		"returnType":{"typeKind":"STRING"},
		"definitionBody":"LOWER(name)",
		"description":"Normalize display names",
		"determinismLevel":"DETERMINISTIC"
	}`
	createRoutine := httptest.NewRecorder()
	server.routes().ServeHTTP(createRoutine, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/routines", strings.NewReader(createRoutineBody)))
	if createRoutine.Code != http.StatusOK {
		t.Fatalf("create routine status = %d, body = %s", createRoutine.Code, createRoutine.Body.String())
	}
	var routine routineResource
	if err := json.NewDecoder(createRoutine.Body).Decode(&routine); err != nil {
		t.Fatalf("decode routine: %v", err)
	}
	if routine.Kind != "bigquery#routine" || routine.RoutineReference.RoutineID != "normalize_name" || routine.ReturnType == nil || routine.ReturnType.TypeKind != "STRING" {
		t.Fatalf("routine metadata = %#v", routine)
	}
	if routine.SelfLink != "/bigquery/v2/projects/local-project/datasets/analytics/routines/normalize_name" {
		t.Fatalf("routine selfLink = %q", routine.SelfLink)
	}

	patchRoutine := httptest.NewRecorder()
	server.routes().ServeHTTP(patchRoutine, httptest.NewRequest(http.MethodPatch, "/bigquery/v2/projects/local-project/datasets/analytics/routines/normalize_name", strings.NewReader(`{"description":"patched routine"}`)))
	if patchRoutine.Code != http.StatusOK {
		t.Fatalf("patch routine status = %d, body = %s", patchRoutine.Code, patchRoutine.Body.String())
	}
	var patched routineResource
	if err := json.NewDecoder(patchRoutine.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched routine: %v", err)
	}
	if patched.Description != "patched routine" || patched.DefinitionBody != "LOWER(name)" {
		t.Fatalf("patched routine = %#v", patched)
	}

	listRoutines := httptest.NewRecorder()
	server.routes().ServeHTTP(listRoutines, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/routines?maxResults=1", nil))
	if listRoutines.Code != http.StatusOK {
		t.Fatalf("list routines status = %d, body = %s", listRoutines.Code, listRoutines.Body.String())
	}
	var listed routinesListResponse
	if err := json.NewDecoder(listRoutines.Body).Decode(&listed); err != nil {
		t.Fatalf("decode routines: %v", err)
	}
	if listed.Kind != "bigquery#routineList" || listed.TotalItems != 1 || len(listed.Routines) != 1 {
		t.Fatalf("listed routines = %#v", listed)
	}

	snapshot, found := server.DatasetSnapshot("local-project", "analytics")
	if !found {
		t.Fatal("dataset snapshot missing")
	}
	if len(snapshot.Routines) != 1 || snapshot.Routines[0].RoutineReference.RoutineID != "normalize_name" {
		t.Fatalf("snapshot routines = %#v", snapshot.Routines)
	}
	if len(snapshot.Tables) != 1 || snapshot.Tables[0].View == nil {
		t.Fatalf("snapshot tables = %#v", snapshot.Tables)
	}

	deleteRoutine := httptest.NewRecorder()
	server.routes().ServeHTTP(deleteRoutine, httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/local-project/datasets/analytics/routines/normalize_name", nil))
	if deleteRoutine.Code != http.StatusNoContent {
		t.Fatalf("delete routine status = %d, body = %s", deleteRoutine.Code, deleteRoutine.Body.String())
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
