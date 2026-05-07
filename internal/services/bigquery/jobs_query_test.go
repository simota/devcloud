package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestJobsQueryCanReadViewMetadataQuery(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	createView := httptest.NewRecorder()
	viewBody := `{
		"tableReference":{"tableId":"active_people"},
		"type":"VIEW",
		"view":{"query":"SELECT id, name, age FROM ` + "`local-project.analytics.people`" + ` WHERE active = TRUE","useLegacySql":false}
	}`
	server.routes().ServeHTTP(createView, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/datasets/analytics/tables", strings.NewReader(viewBody)))
	if createView.Code != http.StatusOK {
		t.Fatalf("create view status = %d, body = %s", createView.Code, createView.Body.String())
	}

	query := httptest.NewRecorder()
	body := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.active_people`" + ` WHERE age >= 35 ORDER BY id","useLegacySql":false,"location":"US"}`
	server.routes().ServeHTTP(query, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(body)))
	if query.Code != http.StatusOK {
		t.Fatalf("query view status = %d, body = %s", query.Code, query.Body.String())
	}
	var response queryResponse
	if err := json.NewDecoder(query.Body).Decode(&response); err != nil {
		t.Fatalf("decode view query response: %v", err)
	}
	if response.TotalRows != "1" || len(response.Rows) != 1 {
		t.Fatalf("view query response = %#v", response)
	}
	if len(response.Schema.Fields) != 2 || response.Schema.Fields[0].Name != "id" || response.Schema.Fields[1].Name != "name" {
		t.Fatalf("view query schema = %#v", response.Schema)
	}
	if response.Rows[0].F[0].V != "1" || response.Rows[0].F[1].V != "Ada" {
		t.Fatalf("view query rows = %#v", response.Rows)
	}

	dryRun := httptest.NewRecorder()
	dryRunBody := `{"query":"SELECT id, name FROM ` + "`local-project.analytics.active_people`" + `","useLegacySql":false,"dryRun":true}`
	server.routes().ServeHTTP(dryRun, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/queries", strings.NewReader(dryRunBody)))
	if dryRun.Code != http.StatusOK {
		t.Fatalf("dry run view status = %d, body = %s", dryRun.Code, dryRun.Body.String())
	}
	var dryRunResponse queryResponse
	if err := json.NewDecoder(dryRun.Body).Decode(&dryRunResponse); err != nil {
		t.Fatalf("decode dry run view response: %v", err)
	}
	if dryRunResponse.TotalRows != "0" || len(dryRunResponse.Schema.Fields) != 2 {
		t.Fatalf("dry run view response = %#v", dryRunResponse)
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
