package dynamodb

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestExecuteStatementSelectsItemsWithParametersAndProjection(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"settings"},"name":{"S":"Ada Settings"},"active":{"BOOL":false}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":true}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	selectRec := dynamodbRequest(t, server, "ExecuteStatement", `{
		"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
		"Parameters":[{"S":"user#1"},{"S":"profile"}],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if selectRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", selectRec.Code, selectRec.Body.String())
	}
	var response struct {
		Items            []item
		ConsumedCapacity map[string]any
	}
	if err := json.NewDecoder(selectRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode ExecuteStatement: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("Items = %#v, want one result", response.Items)
	}
	if got := response.Items[0]["name"]["S"]; got != "Ada" {
		t.Fatalf("name = %#v, want Ada", got)
	}
	if _, ok := response.Items[0]["active"]; ok {
		t.Fatalf("projection included active: %#v", response.Items[0])
	}
	if response.ConsumedCapacity["TableName"] != "Demo" {
		t.Fatalf("ConsumedCapacity = %#v, want Demo table", response.ConsumedCapacity)
	}
}

func TestExecuteStatementRejectsUnsupportedPartiQL(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "ExecuteStatement", `{
		"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
		"Parameters":[{"S":"Ada"},{"S":"user#1"}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ExecuteStatement status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestBatchExecuteStatementSelectsSingleItemsInRequestOrder(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	batchRec := dynamodbRequest(t, server, "BatchExecuteStatement", `{
		"Statements":[
			{
				"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#2"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"missing"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var response struct {
		Responses        []batchStatementResponse
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode BatchExecuteStatement: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if got := response.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := response.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", response.Responses[0].Item)
	}
	if len(response.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want no item", response.Responses[1].Item)
	}
	if got := response.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
	if len(response.ConsumedCapacity) != 3 {
		t.Fatalf("ConsumedCapacity = %#v, want one entry per statement", response.ConsumedCapacity)
	}
}

func TestBatchExecuteStatementReturnsStatementErrorsWithoutFailingWholeBatch(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	batchRec := dynamodbRequest(t, server, "BatchExecuteStatement", `{
		"Statements":[
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ?",
				"Parameters":[{"S":"user#1"}]
			},
			{
				"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
				"Parameters":[{"S":"Ada"},{"S":"user#1"}]
			},
			{
				"Statement":"SELECT * FROM Missing WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		]
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var response struct {
		Responses []batchStatementResponse
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode BatchExecuteStatement: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if response.Responses[0].Error == nil || response.Responses[0].Error.Code != "ValidationError" {
		t.Fatalf("first error = %#v, want ValidationError", response.Responses[0].Error)
	}
	if response.Responses[1].Error == nil || response.Responses[1].Error.Code != "ValidationError" {
		t.Fatalf("second error = %#v, want ValidationError", response.Responses[1].Error)
	}
	if response.Responses[2].Error == nil || response.Responses[2].Error.Code != "ResourceNotFound" {
		t.Fatalf("third error = %#v, want ResourceNotFound", response.Responses[2].Error)
	}
}

func TestExecuteTransactionSelectsSingleItemsInRequestOrder(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	transactionRec := dynamodbRequest(t, server, "ExecuteTransaction", `{
		"TransactStatements":[
			{
				"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#2"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"missing"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if transactionRec.Code != http.StatusOK {
		t.Fatalf("ExecuteTransaction status = %d, body = %s", transactionRec.Code, transactionRec.Body.String())
	}
	var response struct {
		Responses        []batchStatementResponse
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(transactionRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode ExecuteTransaction: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if got := response.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := response.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", response.Responses[0].Item)
	}
	if len(response.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want no item", response.Responses[1].Item)
	}
	if got := response.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
	if len(response.ConsumedCapacity) != 3 {
		t.Fatalf("ConsumedCapacity = %#v, want one entry per statement", response.ConsumedCapacity)
	}
}

func TestExecuteTransactionRejectsUnsupportedPartiQLAtomically(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "ExecuteTransaction", `{
		"TransactStatements":[
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			},
			{
				"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
				"Parameters":[{"S":"Ada"},{"S":"user#1"}]
			}
		]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ExecuteTransaction status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

