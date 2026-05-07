package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
