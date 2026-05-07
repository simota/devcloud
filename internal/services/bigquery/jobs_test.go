package bigquery

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestJobsInsertQueryJobWritesDestinationTable(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	insert := httptest.NewRecorder()
	body := `{"jobReference":{"jobId":"query_to_table","location":"US"},"configuration":{"query":{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + ` WHERE age >= 30 ORDER BY id","useLegacySql":false,"destinationTable":{"datasetId":"analytics","tableId":"people_query"},"createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_TRUNCATE"}}}`
	server.routes().ServeHTTP(insert, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(body)))
	if insert.Code != http.StatusOK {
		t.Fatalf("query destination status = %d, body = %s", insert.Code, insert.Body.String())
	}
	var job jobResource
	if err := json.NewDecoder(insert.Body).Decode(&job); err != nil {
		t.Fatalf("decode query destination job: %v", err)
	}
	if job.Configuration.Query.DestinationTable.TableID != "people_query" || job.Statistics.Query.TotalRows != "2" {
		t.Fatalf("query destination job = %#v", job)
	}

	tableRec := httptest.NewRecorder()
	server.routes().ServeHTTP(tableRec, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people_query", nil))
	if tableRec.Code != http.StatusOK {
		t.Fatalf("destination table status = %d, body = %s", tableRec.Code, tableRec.Body.String())
	}
	var table tableResource
	if err := json.NewDecoder(tableRec.Body).Decode(&table); err != nil {
		t.Fatalf("decode destination table: %v", err)
	}
	if table.NumRows != "2" || len(table.Schema.Fields) != 2 || table.Schema.Fields[0].Name != "id" || table.Schema.Fields[1].Name != "age" {
		t.Fatalf("destination table = %#v", table)
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/people_query/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode query destination rows: %v", err)
	}
	if rowList.TotalRows != "2" || len(rowList.Rows) != 2 || rowList.Rows[0].F[0].V != "1" || rowList.Rows[0].F[1].V != "37" {
		t.Fatalf("query destination rows = %#v", rowList)
	}
}

func TestJobsInsertQueryJobHonorsDestinationDispositions(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")

	createNever := httptest.NewRecorder()
	createNeverBody := `{"configuration":{"query":{"query":"SELECT id FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false,"destinationTable":{"datasetId":"analytics","tableId":"missing"},"createDisposition":"CREATE_NEVER"}}}`
	server.routes().ServeHTTP(createNever, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(createNeverBody)))
	if createNever.Code != http.StatusBadRequest {
		t.Fatalf("CREATE_NEVER status = %d, body = %s", createNever.Code, createNever.Body.String())
	}

	first := httptest.NewRecorder()
	firstBody := `{"jobReference":{"jobId":"query_write_empty_first"},"configuration":{"query":{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false,"destinationTable":{"datasetId":"analytics","tableId":"query_disposition"},"writeDisposition":"WRITE_EMPTY"}}}`
	server.routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(firstBody)))
	if first.Code != http.StatusOK {
		t.Fatalf("first WRITE_EMPTY status = %d, body = %s", first.Code, first.Body.String())
	}

	writeEmptyAgain := httptest.NewRecorder()
	writeEmptyAgainBody := `{"configuration":{"query":{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false,"destinationTable":{"datasetId":"analytics","tableId":"query_disposition"},"writeDisposition":"WRITE_EMPTY"}}}`
	server.routes().ServeHTTP(writeEmptyAgain, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(writeEmptyAgainBody)))
	if writeEmptyAgain.Code != http.StatusBadRequest {
		t.Fatalf("second WRITE_EMPTY status = %d, body = %s", writeEmptyAgain.Code, writeEmptyAgain.Body.String())
	}

	appendRec := httptest.NewRecorder()
	appendBody := `{"jobReference":{"jobId":"query_append"},"configuration":{"query":{"query":"SELECT id, age FROM ` + "`local-project.analytics.people`" + `","useLegacySql":false,"destinationTable":{"datasetId":"analytics","tableId":"query_disposition"},"writeDisposition":"WRITE_APPEND"}}}`
	server.routes().ServeHTTP(appendRec, httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/local-project/jobs", strings.NewReader(appendBody)))
	if appendRec.Code != http.StatusOK {
		t.Fatalf("WRITE_APPEND status = %d, body = %s", appendRec.Code, appendRec.Body.String())
	}

	rows := httptest.NewRecorder()
	server.routes().ServeHTTP(rows, httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/local-project/datasets/analytics/tables/query_disposition/data", nil))
	var rowList tableDataListResponse
	if err := json.NewDecoder(rows.Body).Decode(&rowList); err != nil {
		t.Fatalf("decode disposition rows: %v", err)
	}
	if rowList.TotalRows != "4" || len(rowList.Rows) != 4 {
		t.Fatalf("disposition rows = %#v", rowList)
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
