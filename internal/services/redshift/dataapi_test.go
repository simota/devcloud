package redshift

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDataAPIExecuteStatementSupportsCreateTableAsSelect(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64))",
		"insert into public.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"create table public.created_events as select id, payload from public.events where id = 1"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}

	result, err := server.executeSQL("select id, payload from public.created_events")
	if err != nil {
		t.Fatalf("select Data API CTAS table: %v", err)
	}
	if len(result.rows) != 1 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("Data API CTAS rows = %#v", result.rows)
	}
}

func TestDataAPIExecuteDescribeGetResultAndIdempotency(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"ClientToken":"token-1"
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
	if executeResponse.ID == "" {
		t.Fatal("ExecuteStatement returned empty Id")
	}

	retryRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"ClientToken":"token-1"
	}`)
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Status       string
		ResultRows   int64
		HasResultSet bool
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode describe response: %v", err)
	}
	if describeResponse.Status != "FINISHED" || describeResponse.ResultRows != 1 || !describeResponse.HasResultSet {
		t.Fatalf("describe response = %#v", describeResponse)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{"ColumnMetadata", "Records", "longValue"} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResult missing %q: %s", want, body)
		}
	}
}

func TestDataAPIResultFieldsPreserveZeroFalseAndDoubleTypes(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 0 as zero_value, false as active, 1.5 as score"
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

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{`"longValue":0`, `"booleanValue":false`, `"doubleValue":1.5`, `"typeName":"int4"`, `"typeName":"bool"`, `"typeName":"float8"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResult missing %q: %s", want, body)
		}
	}
}

func TestDataAPIGetStatementResultV2ReturnsCSVRecords(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1 as id, 'hello, csv' as payload",
		"ResultFormat":"CSV"
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

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResultV2", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK {
		t.Fatalf("GetStatementResultV2 status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
	body := resultRec.Body.String()
	for _, want := range []string{`"ResultFormat":"CSV"`, `"CSVRecords":"1,\"hello, csv\""`, `"TotalNumRows":1`} {
		if !strings.Contains(body, want) {
			t.Fatalf("GetStatementResultV2 missing %q: %s", want, body)
		}
	}
}

func TestDataAPIGetStatementResultV2RequiresCSVResultFormat(t *testing.T) {
	server := NewServer(Config{})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{"Sql":"select 1"}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResultV2", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusBadRequest || !strings.Contains(resultRec.Body.String(), "ResultFormat CSV") {
		t.Fatalf("GetStatementResultV2 status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
}

func TestDataAPIExecuteStatementTracksSessionMetadata(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select 1",
		"SessionKeepAliveSeconds":60
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID        string `json:"Id"`
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if executeResponse.ID == "" || executeResponse.SessionID == "" {
		t.Fatalf("execute response = %#v", executeResponse)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode describe response: %v", err)
	}
	if describeResponse.SessionID != executeResponse.SessionID {
		t.Fatalf("describe SessionId = %q, want %q", describeResponse.SessionID, executeResponse.SessionID)
	}

	batchRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":["select 1"],
		"SessionId":"`+executeResponse.SessionID+`",
		"SessionKeepAliveSeconds":120
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var batchResponse struct {
		SessionID string `json:"SessionId"`
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&batchResponse); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if batchResponse.SessionID != executeResponse.SessionID {
		t.Fatalf("batch SessionId = %q, want %q", batchResponse.SessionID, executeResponse.SessionID)
	}

	statements := server.StatementSnapshots()
	var found bool
	for _, statement := range statements {
		if statement.SessionID == executeResponse.SessionID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("statement snapshots missing SessionId %q: %#v", executeResponse.SessionID, statements)
	}
}

func TestDataAPIExecuteStatementRejectsInvalidSessionKeepAlive(t *testing.T) {
	server := NewServer(Config{})

	rec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Sql":"select 1",
		"SessionKeepAliveSeconds":-1
	}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "SessionKeepAliveSeconds") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestDataAPIRejectsOversizeStatementsWithoutPersistingSQL(t *testing.T) {
	server := NewServer(Config{MaxStatementBytes: 8})

	rec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"Sql":"select 123456789"
	}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "maxStatementBytes") {
		t.Fatalf("ExecuteStatement status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if statements := server.StatementSnapshots(); len(statements) != 0 {
		t.Fatalf("oversize ExecuteStatement persisted statement history: %#v", statements)
	}

	batchRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":["select 1","select 123456789"]
	}`)
	if batchRec.Code != http.StatusBadRequest || !strings.Contains(batchRec.Body.String(), "maxStatementBytes") {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	if statements := server.StatementSnapshots(); len(statements) != 0 {
		t.Fatalf("oversize BatchExecuteStatement persisted statement history: %#v", statements)
	}
}

func TestDataAPIGetStatementResultPaginatesRows(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create table public.page_events(id integer, payload varchar(64))",
		"insert into public.page_events values (1, 'one')",
		"insert into public.page_events values (2, 'two')",
		"insert into public.page_events values (3, 'three')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"select id, payload from public.page_events order by id"
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

	firstRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","MaxResults":2}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first GetStatementResult status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstPage struct {
		NextToken    string
		Records      [][]dataAPIResultField
		TotalNumRows int
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if firstPage.NextToken != "2" || firstPage.TotalNumRows != 3 || len(firstPage.Records) != 2 || firstPage.Records[0][1].StringValue == nil || *firstPage.Records[0][1].StringValue != "one" {
		t.Fatalf("first page = %#v", firstPage)
	}

	nextRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","MaxResults":2,"NextToken":"2"}`)
	if nextRec.Code != http.StatusOK {
		t.Fatalf("next GetStatementResult status = %d, body = %s", nextRec.Code, nextRec.Body.String())
	}
	var nextPage struct {
		NextToken string
		Records   [][]dataAPIResultField
	}
	if err := json.NewDecoder(nextRec.Body).Decode(&nextPage); err != nil {
		t.Fatalf("decode next page: %v", err)
	}
	if nextPage.NextToken != "" || len(nextPage.Records) != 1 || nextPage.Records[0][1].StringValue == nil || *nextPage.Records[0][1].StringValue != "three" {
		t.Fatalf("next page = %#v", nextPage)
	}

	invalidRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`","NextToken":"not-a-token"}`)
	if invalidRec.Code != http.StatusBadRequest || !strings.Contains(invalidRec.Body.String(), "NextToken is invalid") {
		t.Fatalf("invalid NextToken status = %d, body = %s", invalidRec.Code, invalidRec.Body.String())
	}
}

func TestDataAPIBatchExecuteStatementRunsStatementsAndIsIdempotent(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sqls":[
			"create schema if not exists batch",
			"create table batch.events(id integer, payload varchar(64))",
			"insert into batch.events values (1, 'created')",
			"select id, payload from batch.events where id = 1"
		],
		"ClientToken":"batch-token-1"
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}
	if executeResponse.ID == "" {
		t.Fatal("BatchExecuteStatement returned empty Id")
	}

	retryRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sqls":["select 1"],
		"ClientToken":"batch-token-1"
	}`)
	var retryResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(retryRec.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ID != executeResponse.ID {
		t.Fatalf("idempotent Id = %q, want %q", retryResponse.ID, executeResponse.ID)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"FINISHED"`) {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	resultRec := redshiftDataAPIRequest(t, server, "GetStatementResult", `{"Id":"`+executeResponse.ID+`"}`)
	if resultRec.Code != http.StatusOK || !strings.Contains(resultRec.Body.String(), "created") {
		t.Fatalf("GetStatementResult status = %d, body = %s", resultRec.Code, resultRec.Body.String())
	}
}

func TestDataAPIBatchExecuteStatementRollsBackOnFailure(t *testing.T) {
	server := NewServer(Config{})

	executeRec := redshiftDataAPIRequest(t, server, "BatchExecuteStatement", `{
		"Sqls":[
			"create schema if not exists batch_fail",
			"create table batch_fail.events(id integer)",
			"insert into batch_fail.events values (1, 'extra')"
		]
	}`)
	if executeRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", executeRec.Code, executeRec.Body.String())
	}
	var executeResponse struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(executeRec.Body).Decode(&executeResponse); err != nil {
		t.Fatalf("decode execute response: %v", err)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"`+executeResponse.ID+`"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"FAILED"`) {
		t.Fatalf("DescribeStatement status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	if _, err := server.executeSQL("select * from batch_fail.events"); err == nil {
		t.Fatal("batch failure left table behind")
	}
}

func TestDataAPICancelStatementAndListStatusFilter(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	createdAt := time.Now().UTC()
	server.statements["running"] = &statement{
		ID:                "running",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		DbUser:            "dev",
		QueryString:       "select 1",
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "STARTED",
	}
	server.statements["finished"] = &statement{
		ID:                "finished",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		DbUser:            "dev",
		QueryString:       "select 2",
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            queryResult{fields: []pgField{{Name: "?column?", TypeOID: pgTypeInt4OID, TypeSize: 4}}, rows: [][]string{{"2"}}, tag: "SELECT 1"},
		HasResultSet:      true,
	}

	cancelRec := redshiftDataAPIRequest(t, server, "CancelStatement", `{"Id":"running"}`)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("CancelStatement status = %d, body = %s", cancelRec.Code, cancelRec.Body.String())
	}
	if !strings.Contains(cancelRec.Body.String(), `"Status":true`) {
		t.Fatalf("CancelStatement body = %s", cancelRec.Body.String())
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeStatement", `{"Id":"running"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"Status":"ABORTED"`) {
		t.Fatalf("DescribeStatement after cancel status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}

	finishedCancelRec := redshiftDataAPIRequest(t, server, "CancelStatement", `{"Id":"finished"}`)
	if finishedCancelRec.Code != http.StatusOK || !strings.Contains(finishedCancelRec.Body.String(), `"Status":false`) {
		t.Fatalf("CancelStatement finished body = %d %s", finishedCancelRec.Code, finishedCancelRec.Body.String())
	}

	listRec := redshiftDataAPIRequest(t, server, "ListStatements", `{"Status":"ABORTED"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListStatements status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	body := listRec.Body.String()
	if !strings.Contains(body, `"Id":"running"`) || strings.Contains(body, `"Id":"finished"`) {
		t.Fatalf("ListStatements filter body = %s", body)
	}
}

func TestDataAPIMetadataListsUseCatalog(t *testing.T) {
	server := NewServer(Config{Database: "dev"})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer, payload varchar(64))",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	databasesRec := redshiftDataAPIRequest(t, server, "ListDatabases", `{"Database":"dev"}`)
	if databasesRec.Code != http.StatusOK || !strings.Contains(databasesRec.Body.String(), `"dev"`) {
		t.Fatalf("ListDatabases status = %d, body = %s", databasesRec.Code, databasesRec.Body.String())
	}

	schemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev"}`)
	if schemasRec.Code != http.StatusOK || !strings.Contains(schemasRec.Body.String(), `"loop"`) {
		t.Fatalf("ListSchemas status = %d, body = %s", schemasRec.Code, schemasRec.Body.String())
	}

	tablesRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","Schema":"loop"}`)
	if tablesRec.Code != http.StatusOK || !strings.Contains(tablesRec.Body.String(), `"events"`) {
		t.Fatalf("ListTables status = %d, body = %s", tablesRec.Code, tablesRec.Body.String())
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeTable", `{"Database":"dev","Schema":"loop","Table":"events"}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"ColumnList"`) {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
}

func TestDataAPIMetadataListsSupportPatternFiltersAndPagination(t *testing.T) {
	server := NewServer(Config{Database: "dev"})
	for _, statement := range []string{
		"create schema if not exists alpha",
		"create schema if not exists loop",
		"create table alpha.metrics(id integer)",
		"create table loop.events(id integer, payload varchar(64))",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	firstSchemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev","SchemaPattern":"%","MaxResults":1}`)
	if firstSchemasRec.Code != http.StatusOK || !strings.Contains(firstSchemasRec.Body.String(), `"NextToken":"1"`) {
		t.Fatalf("first ListSchemas status = %d, body = %s", firstSchemasRec.Code, firstSchemasRec.Body.String())
	}
	nextSchemasRec := redshiftDataAPIRequest(t, server, "ListSchemas", `{"Database":"dev","SchemaPattern":"%","MaxResults":1,"NextToken":"1"}`)
	if nextSchemasRec.Code != http.StatusOK || !strings.Contains(nextSchemasRec.Body.String(), `"loop"`) {
		t.Fatalf("next ListSchemas status = %d, body = %s", nextSchemasRec.Code, nextSchemasRec.Body.String())
	}

	tablesRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","SchemaPattern":"lo%","TablePattern":"ev%"}`)
	body := tablesRec.Body.String()
	if tablesRec.Code != http.StatusOK || !strings.Contains(body, `"events"`) || strings.Contains(body, `"metrics"`) {
		t.Fatalf("ListTables filtered status = %d, body = %s", tablesRec.Code, body)
	}

	describeRec := redshiftDataAPIRequest(t, server, "DescribeTable", `{"Database":"dev","Schema":"loop","Table":"events","MaxResults":1}`)
	if describeRec.Code != http.StatusOK || !strings.Contains(describeRec.Body.String(), `"NextToken":"1"`) || !strings.Contains(describeRec.Body.String(), `"TableName":"events"`) {
		t.Fatalf("DescribeTable paged status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}

	invalidRec := redshiftDataAPIRequest(t, server, "ListTables", `{"Database":"dev","NextToken":"not-a-token"}`)
	if invalidRec.Code != http.StatusBadRequest || !strings.Contains(invalidRec.Body.String(), "NextToken is invalid") {
		t.Fatalf("invalid NextToken status = %d, body = %s", invalidRec.Code, invalidRec.Body.String())
	}
}

func TestDataAPIStatementMetadataRedactsSensitiveSQL(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})

	executeRec := redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv"
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

	for operation, payload := range map[string]string{
		"DescribeStatement": `{"Id":"` + executeResponse.ID + `"}`,
		"ListStatements":    `{}`,
	} {
		rec := redshiftDataAPIRequest(t, server, operation, payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s", operation, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if !strings.Contains(body, `"QueryString":"[redacted]"`) {
			t.Fatalf("%s did not redact QueryString: %s", operation, body)
		}
		if strings.Contains(body, "secret-role") || strings.Contains(body, "s3://bucket/events.csv") {
			t.Fatalf("%s leaked sensitive SQL: %s", operation, body)
		}
	}
}
