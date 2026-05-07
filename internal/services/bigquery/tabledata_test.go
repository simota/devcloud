package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
