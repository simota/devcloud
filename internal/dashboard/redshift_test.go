package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	redshiftsvc "devcloud/internal/services/redshift"
)

func TestRedshiftDashboardAPIListsClusters(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		Region:            "us-east-1",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	server := NewServer(Config{
		RedshiftSQLEndpoint: "127.0.0.1:15439",
		RedshiftAPIEndpoint: "http://127.0.0.1:19099",
		RedshiftRegion:      "us-east-1",
	}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":true`) || !strings.Contains(status.Body.String(), `"clusterCount":1`) {
		t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
	}

	clusters := performRequest(server.routes(), http.MethodGet, "/api/redshift/clusters")
	if clusters.Code != http.StatusOK || !strings.Contains(clusters.Body.String(), `"clusterIdentifier":"devcloud"`) || !strings.Contains(clusters.Body.String(), `"port":15439`) {
		t.Fatalf("clusters = %d body=%s", clusters.Code, clusters.Body.String())
	}
}

func TestRedshiftDashboardAPIExposesBackendMode(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		ClusterIdentifier: "devcloud",
		BackendKind:       "postgres",
		BackendMode:       "external",
	})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"backendKind":"postgres"`) || !strings.Contains(status.Body.String(), `"backendMode":"external"`) {
		t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
	}
	if strings.Contains(status.Body.String(), "postgres://") || strings.Contains(status.Body.String(), "secret") {
		t.Fatalf("status leaked backend credentials: %s", status.Body.String())
	}
}

func TestRedshiftDashboardAPIExposesCatalogAndStatementMetadata(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, sql := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id)",
		"insert into loop.events values (1, 'created')",
		"select id from loop.events where id = 1",
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Sql":`+strconv.Quote(sql)+`}`))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "RedshiftData.ExecuteStatement")
		redshiftServer.ServeHTTP(httptest.NewRecorder(), req)
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	catalog := performRequest(server.routes(), http.MethodGet, "/api/redshift/catalog")
	if catalog.Code != http.StatusOK || !strings.Contains(catalog.Body.String(), `"database":"dev"`) || !strings.Contains(catalog.Body.String(), `"name":"events"`) || !strings.Contains(catalog.Body.String(), `"encoding":"raw"`) {
		t.Fatalf("catalog = %d body=%s", catalog.Code, catalog.Body.String())
	}

	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusOK || !strings.Contains(statements.Body.String(), `"status":"FINISHED"`) || !strings.Contains(statements.Body.String(), `"queryPreview":"select id from loop.events where id = 1"`) {
		t.Fatalf("statements = %d body=%s", statements.Code, statements.Body.String())
	}
	if strings.Contains(statements.Body.String(), `"created"`) {
		t.Fatalf("statements response leaked statement result value: %s", statements.Body.String())
	}
}

func TestRedshiftDashboardAPITableDetailAndQueryRunner(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, sql := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
		"insert into loop.events values (2, 'queued')",
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Sql":`+strconv.Quote(sql)+`}`))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "RedshiftData.ExecuteStatement")
		redshiftServer.ServeHTTP(httptest.NewRecorder(), req)
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	table := performRequest(server.routes(), http.MethodGet, "/api/redshift/tables/loop/events?limit=1")
	if table.Code != http.StatusOK || !strings.Contains(table.Body.String(), `"rowCount":2`) || !strings.Contains(table.Body.String(), `"rows":[["1","created"]]`) || !strings.Contains(table.Body.String(), `"encoding":"raw"`) {
		t.Fatalf("redshift table detail = %d body=%s", table.Code, table.Body.String())
	}

	query := performRequestWithBody(server.routes(), http.MethodPost, "/api/redshift/query", `{"sql":"select id, payload from loop.events where id = 2","maxRows":5}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"rowCount":1`) || !strings.Contains(query.Body.String(), `"rows":[["2","queued"]]`) || !strings.Contains(query.Body.String(), `"typeName":"int4"`) {
		t.Fatalf("redshift dashboard query = %d body=%s", query.Code, query.Body.String())
	}

	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusOK || !strings.Contains(statements.Body.String(), `"queryPreview":"select id, payload from loop.events where id = 2"`) {
		t.Fatalf("redshift statements after query = %d body=%s", statements.Code, statements.Body.String())
	}
}

func TestRedshiftDashboardQueryErrorIsSafe(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	query := performRequestWithBody(server.routes(), http.MethodPost, "/api/redshift/query", `{"sql":"select * from missing where token = 'secret-token'","maxRows":5}`)
	if query.Code != http.StatusBadRequest || !strings.Contains(query.Body.String(), `"error":"redshift query failed"`) {
		t.Fatalf("redshift dashboard query error = %d body=%s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "secret-token") || strings.Contains(query.Body.String(), "queryPreview") || strings.Contains(query.Body.String(), "statement") {
		t.Fatalf("redshift dashboard query error leaked SQL details: %s", query.Body.String())
	}
}

func TestRedshiftDashboardAPIMarksDisabled(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":false`) {
		t.Fatalf("disabled status = %d body=%s", status.Code, status.Body.String())
	}
	clusters := performRequest(server.routes(), http.MethodGet, "/api/redshift/clusters")
	if clusters.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled clusters status = %d body=%s", clusters.Code, clusters.Body.String())
	}
	catalog := performRequest(server.routes(), http.MethodGet, "/api/redshift/catalog")
	if catalog.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled catalog status = %d body=%s", catalog.Code, catalog.Body.String())
	}
	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled statements status = %d body=%s", statements.Code, statements.Body.String())
	}
}
