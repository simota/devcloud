package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	bigquerysvc "devcloud/internal/services/bigquery"
	s3svc "devcloud/internal/services/s3"
)

func TestBigQueryDashboardPageAndAPIExposeCatalog(t *testing.T) {
	gcsStore := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := gcsStore.CreateBucket(context.Background(), "bq-fixtures"); err != nil || !created {
		t.Fatalf("create GCS fixture bucket created=%t err=%v", created, err)
	}
	if _, created, err := gcsStore.CreateBucket(context.Background(), "bq-exports"); err != nil || !created {
		t.Fatalf("create GCS export bucket created=%t err=%v", created, err)
	}
	if _, err := gcsStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "bq-fixtures",
		Key:         "events.ndjson",
		Body:        strings.NewReader(`{"event_id":"imported","count":2}` + "\n"),
		ContentType: "application/x-ndjson",
	}); err != nil {
		t.Fatalf("put GCS import fixture: %v", err)
	}
	bq := bigquerysvc.NewServer(bigquerysvc.Config{
		Project:     "devcloud",
		Location:    "US",
		StoragePath: t.TempDir(),
		ObjectStore: gcsStore,
	})
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets", `{"datasetReference":{"datasetId":"analytics"}}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets/analytics/tables", `{
		"tableReference":{"tableId":"people"},
		"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"}]}
	}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets/analytics/tables/people/insertAll", `{
		"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37}}]
	}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/queries", `{
		"query":"SELECT id, name FROM `+"`devcloud.analytics.people`"+` WHERE age >= 30",
		"useLegacySql":false
	}`)

	server := NewServer(Config{
		BigQueryEndpoint:    "http://127.0.0.1:9050",
		BigQueryProject:     "devcloud",
		BigQueryLocation:    "US",
		BigQueryAuthMode:    "bearer-dev",
		BigQueryStoragePath: ".devcloud/test/bigquery",
	}, newDashboardStore(nil, nil))
	server.SetBigQuery(bq)
	routes := server.routes()

	page := performRequest(routes, http.MethodGet, "/dashboard/bigquery")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "devcloud Dashboard") {
		t.Fatalf("BigQuery dashboard route changed: status=%d body=%s", page.Code, page.Body.String())
	}
	compatPage := performRequest(routes, http.MethodGet, "/bigquery")
	if compatPage.Code != http.StatusOK || !strings.Contains(compatPage.Body.String(), "devcloud Dashboard") {
		t.Fatalf("BigQuery compat route changed: status=%d body=%s", compatPage.Code, compatPage.Body.String())
	}

	status := performRequest(routes, http.MethodGet, "/api/bigquery/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"service":"bigquery"`) || !strings.Contains(status.Body.String(), `"running":true`) || !strings.Contains(status.Body.String(), `"authMode":"bearer-dev"`) {
		t.Fatalf("BigQuery status = %d body=%s", status.Code, status.Body.String())
	}
	projects := performRequest(routes, http.MethodGet, "/api/services")
	if projects.Code != http.StatusOK || !strings.Contains(projects.Body.String(), `"id":"bigquery"`) {
		t.Fatalf("service alias = %d body=%s", projects.Code, projects.Body.String())
	}
	projectList := performRequest(routes, http.MethodGet, "/api/bigquery/projects")
	if projectList.Code != http.StatusOK || !strings.Contains(projectList.Body.String(), `"projectId":"devcloud"`) {
		t.Fatalf("BigQuery projects = %d body=%s", projectList.Code, projectList.Body.String())
	}
	datasets := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets")
	if datasets.Code != http.StatusOK || !strings.Contains(datasets.Body.String(), `"datasetId":"analytics"`) {
		t.Fatalf("BigQuery datasets = %d body=%s", datasets.Code, datasets.Body.String())
	}
	tables := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables")
	if tables.Code != http.StatusOK || !strings.Contains(tables.Body.String(), `"tableId":"people"`) {
		t.Fatalf("BigQuery tables = %d body=%s", tables.Code, tables.Body.String())
	}
	table := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people")
	if table.Code != http.StatusOK || !strings.Contains(table.Body.String(), `"tableId":"people"`) || !strings.Contains(table.Body.String(), `"numRows":"1"`) {
		t.Fatalf("BigQuery table detail = %d body=%s", table.Code, table.Body.String())
	}
	schema := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people/schema")
	if schema.Code != http.StatusOK || !strings.Contains(schema.Body.String(), `"name":"age"`) || !strings.Contains(schema.Body.String(), `"type":"INTEGER"`) {
		t.Fatalf("BigQuery schema = %d body=%s", schema.Code, schema.Body.String())
	}
	rows := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people/rows?limit=1")
	if rows.Code != http.StatusOK || !strings.Contains(rows.Body.String(), `"name":"Ada"`) {
		t.Fatalf("BigQuery rows = %d body=%s", rows.Code, rows.Body.String())
	}
	jobs := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/jobs")
	if jobs.Code != http.StatusOK || !strings.Contains(jobs.Body.String(), `"state":"DONE"`) {
		t.Fatalf("BigQuery jobs = %d body=%s", jobs.Code, jobs.Body.String())
	}
	jobDetail := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/jobs/devcloud_query_")
	if jobDetail.Code != http.StatusNotFound {
		t.Fatalf("BigQuery unknown job detail = %d body=%s", jobDetail.Code, jobDetail.Body.String())
	}
	query := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/queries", `{
		"query":"SELECT id, name FROM `+"`devcloud.analytics.people`"+` WHERE age >= 30",
		"useLegacySql":false
	}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"kind":"bigquery#queryResponse"`) || !strings.Contains(query.Body.String(), `"totalRows":"1"`) {
		t.Fatalf("BigQuery dashboard query = %d body=%s", query.Code, query.Body.String())
	}
	var queryPayload struct {
		JobReference struct {
			JobID string `json:"jobId"`
		} `json:"jobReference"`
	}
	if err := json.Unmarshal(query.Body.Bytes(), &queryPayload); err != nil {
		t.Fatalf("decode BigQuery dashboard query response: %v", err)
	}
	queryJobDetail := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/jobs/"+queryPayload.JobReference.JobID)
	if queryJobDetail.Code != http.StatusOK || !strings.Contains(queryJobDetail.Body.String(), `"jobId":"`+queryPayload.JobReference.JobID+`"`) || !strings.Contains(queryJobDetail.Body.String(), `"state":"DONE"`) {
		t.Fatalf("BigQuery query job detail = %d body=%s", queryJobDetail.Code, queryJobDetail.Body.String())
	}
	managementDataset := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/datasets", `{
		"datasetReference":{"datasetId":"dashboard_ops"},
		"location":"US",
		"friendlyName":"Dashboard Ops"
	}`)
	if managementDataset.Code != http.StatusOK || !strings.Contains(managementDataset.Body.String(), `"datasetId":"dashboard_ops"`) {
		t.Fatalf("BigQuery dashboard dataset create = %d body=%s", managementDataset.Code, managementDataset.Body.String())
	}
	managementTable := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/datasets/dashboard_ops/tables", `{
		"tableReference":{"tableId":"events"},
		"schema":{"fields":[{"name":"event_id","type":"STRING","mode":"REQUIRED"},{"name":"count","type":"INTEGER"}]}
	}`)
	if managementTable.Code != http.StatusOK || !strings.Contains(managementTable.Body.String(), `"tableId":"events"`) {
		t.Fatalf("BigQuery dashboard table create = %d body=%s", managementTable.Code, managementTable.Body.String())
	}
	managementInsert := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/datasets/dashboard_ops/tables/events/insertAll", `{
		"skipInvalidRows":true,
		"rows":[{"insertId":"event-1","json":{"event_id":"signup","count":1}}]
	}`)
	if managementInsert.Code != http.StatusOK || !strings.Contains(managementInsert.Body.String(), `"kind":"bigquery#tableDataInsertAllResponse"`) {
		t.Fatalf("BigQuery dashboard insertAll = %d body=%s", managementInsert.Code, managementInsert.Body.String())
	}
	managementRows := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/dashboard_ops/tables/events/rows?limit=1")
	if managementRows.Code != http.StatusOK || !strings.Contains(managementRows.Body.String(), `"event_id":"signup"`) {
		t.Fatalf("BigQuery dashboard inserted rows = %d body=%s", managementRows.Code, managementRows.Body.String())
	}
	importJob := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/jobs", `{
		"jobReference":{"jobId":"dashboard_gcs_import","location":"US"},
		"configuration":{"load":{
			"sourceUris":["gs://bq-fixtures/events.ndjson"],
			"destinationTable":{"datasetId":"dashboard_ops","tableId":"events"},
			"sourceFormat":"NEWLINE_DELIMITED_JSON",
			"writeDisposition":"WRITE_APPEND"
		}}
	}`)
	if importJob.Code != http.StatusOK || !strings.Contains(importJob.Body.String(), `"jobId":"dashboard_gcs_import"`) || !strings.Contains(importJob.Body.String(), `"state":"DONE"`) {
		t.Fatalf("BigQuery dashboard GCS import job = %d body=%s", importJob.Code, importJob.Body.String())
	}
	importedRows := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/dashboard_ops/tables/events/rows?limit=10")
	if importedRows.Code != http.StatusOK || !strings.Contains(importedRows.Body.String(), `"event_id":"imported"`) {
		t.Fatalf("BigQuery dashboard imported rows = %d body=%s", importedRows.Code, importedRows.Body.String())
	}
	exportJob := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/jobs", `{
		"jobReference":{"jobId":"dashboard_gcs_export","location":"US"},
		"configuration":{"extract":{
			"sourceTable":{"datasetId":"dashboard_ops","tableId":"events"},
			"destinationUris":["gs://bq-exports/events.ndjson"],
			"destinationFormat":"NEWLINE_DELIMITED_JSON"
		}}
	}`)
	if exportJob.Code != http.StatusOK || !strings.Contains(exportJob.Body.String(), `"jobId":"dashboard_gcs_export"`) || !strings.Contains(exportJob.Body.String(), `"state":"DONE"`) {
		t.Fatalf("BigQuery dashboard GCS export job = %d body=%s", exportJob.Code, exportJob.Body.String())
	}
	_, exportedBody, found, err := gcsStore.GetObject(context.Background(), "bq-exports", "events.ndjson")
	if err != nil || !found || !strings.Contains(string(exportedBody), `"event_id":"imported"`) {
		t.Fatalf("exported GCS object found=%t err=%v body=%s", found, err, string(exportedBody))
	}
}
