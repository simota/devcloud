package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
