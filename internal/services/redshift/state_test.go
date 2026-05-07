package redshift

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatePersistsCatalogRowsAndClusterMetadata(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		SQLAddr:           "127.0.0.1:15439",
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateCluster&ClusterIdentifier=analytics&DBName=warehouse&MasterUsername=analyst"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	server.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateCluster status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	reloaded := NewServer(Config{
		SQLAddr:     "127.0.0.1:25439",
		StoragePath: storagePath,
		Database:    "dev",
		User:        "dev",
	})
	result, err := reloaded.executeSQL("select id, payload from loop.events where id = 1")
	if err != nil {
		t.Fatalf("select after reload: %v", err)
	}
	if len(result.rows) != 1 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows after reload = %#v", result.rows)
	}
	tableInfo, err := reloaded.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info after reload: %v", err)
	}
	if !resultContainsRow(tableInfo, "loop", "events", "key", "id", "id", "1") {
		t.Fatalf("table metadata after reload = %#v", tableInfo.rows)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Clusters) != 2 {
		t.Fatalf("clusters after reload = %#v", snapshot.Clusters)
	}
	for _, cluster := range snapshot.Clusters {
		if cluster.Endpoint.Port != 25439 {
			t.Fatalf("cluster endpoint was not normalized to current config: %#v", cluster)
		}
	}
}

func TestStatePersistsDataAPIStatementHistoryAndResults(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id",
		"ClientToken":"persist-token"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	reloaded := NewServer(Config{
		StoragePath:       storagePath,
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	retryRec := redshiftDataAPIRequest(t, reloaded, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id",
		"ClientToken":"persist-token"
	}`)
	if retryRec.Code != http.StatusOK {
		t.Fatalf("idempotent ExecuteStatement status = %d, body = %s", retryRec.Code, retryRec.Body.String())
	}
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("reloaded idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	listRec := redshiftDataAPIRequest(t, reloaded, "ListStatements", `{}`)
	if listRec.Code != http.StatusOK || !strings.Contains(listRec.Body.String(), `"Status":"FINISHED"`) || !strings.Contains(listRec.Body.String(), "select 1 as id") {
		t.Fatalf("ListStatements after reload = %d, body = %s", listRec.Code, listRec.Body.String())
	}

	resultRec := redshiftDataAPIRequest(t, reloaded, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult after reload status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	if !strings.Contains(resultRec.Body.String(), `"longValue":1`) {
		t.Fatalf("GetStatementResult after reload body = %s", resultRec.Body.String())
	}
}
