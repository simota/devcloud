package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
