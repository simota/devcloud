package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

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
