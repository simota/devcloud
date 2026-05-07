package dynamodb

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestBatchWriteAndBatchGetItems(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}
	var writeResponse struct {
		UnprocessedItems map[string][]writeRequest
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(writeRec.Body).Decode(&writeResponse); err != nil {
		t.Fatalf("decode BatchWriteItem: %v", err)
	}
	if len(writeResponse.UnprocessedItems) != 0 {
		t.Fatalf("UnprocessedItems = %#v, want empty", writeResponse.UnprocessedItems)
	}
	if len(writeResponse.ConsumedCapacity) != 1 || writeResponse.ConsumedCapacity[0]["TableName"] != "Demo" {
		t.Fatalf("BatchWriteItem ConsumedCapacity = %#v, want Demo table capacity", writeResponse.ConsumedCapacity)
	}

	getRec := dynamodbRequest(t, server, "BatchGetItem", `{
		"RequestItems":{
			"Demo":{
				"Keys":[
					{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
					{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
					{"pk":{"S":"missing"},"sk":{"S":"profile"}}
				],
				"ProjectionExpression":"pk, name"
			}
		},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("BatchGetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Responses        map[string][]item
		UnprocessedKeys  map[string]batchGetTableRequest
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode BatchGetItem: %v", err)
	}
	if len(getResponse.UnprocessedKeys) != 0 {
		t.Fatalf("UnprocessedKeys = %#v, want empty", getResponse.UnprocessedKeys)
	}
	if len(getResponse.ConsumedCapacity) != 1 || getResponse.ConsumedCapacity[0]["TableName"] != "Demo" {
		t.Fatalf("BatchGetItem ConsumedCapacity = %#v, want Demo table capacity", getResponse.ConsumedCapacity)
	}
	if len(getResponse.Responses["Demo"]) != 2 {
		t.Fatalf("Responses[Demo] = %#v, want two items", getResponse.Responses["Demo"])
	}
	for _, got := range getResponse.Responses["Demo"] {
		if _, ok := got["active"]; ok {
			t.Fatalf("BatchGetItem projection included active: %#v", got)
		}
		if _, ok := got["pk"]; !ok {
			t.Fatalf("BatchGetItem projection missing pk: %#v", got)
		}
		if _, ok := got["name"]; !ok {
			t.Fatalf("BatchGetItem projection missing name: %#v", got)
		}
	}

	deleteRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"DeleteRequest":{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}}
			]
		}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.ItemCount != 1 {
		t.Fatalf("ItemCount = %d, want 1", describeResponse.Table.ItemCount)
	}
}

func TestBatchWriteValidationFailureDoesNotPartiallyApply(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"name":{"S":"Grace"}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchWriteItem status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("item was partially applied after failed BatchWriteItem: %#v", getResponse.Item)
	}
}

func TestBatchOperationsRejectInvalidReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}}}
			]
		},
		"ReturnConsumedCapacity":"BROKEN"
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchWriteItem status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}
	if got := writeRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("BatchWriteItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getAfterRejectedWrite := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getAfterRejectedWriteResponse struct {
		Item item
	}
	if err := json.NewDecoder(getAfterRejectedWrite.Body).Decode(&getAfterRejectedWriteResponse); err != nil {
		t.Fatalf("decode GetItem after rejected BatchWriteItem: %v", err)
	}
	if len(getAfterRejectedWriteResponse.Item) != 0 {
		t.Fatalf("rejected BatchWriteItem mutated item: %#v", getAfterRejectedWriteResponse.Item)
	}

	getRec := dynamodbRequest(t, server, "BatchGetItem", `{
		"RequestItems":{
			"Demo":{"Keys":[{"pk":{"S":"user#1"},"sk":{"S":"profile"}}]}
		},
		"ReturnConsumedCapacity":"BROKEN"
	}`)
	if getRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchGetItem status = %d, want %d, body = %s", getRec.Code, http.StatusBadRequest, getRec.Body.String())
	}
	if got := getRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("BatchGetItem X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestTransactGetItemsReturnsProjectedItemsInRequestOrder(t *testing.T) {
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

	getRec := dynamodbRequest(t, server, "TransactGetItems", `{
		"TransactItems":[
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
				"ProjectionExpression":"pk, #n",
				"ExpressionAttributeNames":{"#n":"name"}
			}},
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"missing"},"sk":{"S":"profile"}}
			}},
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ProjectionExpression":"name"
			}}
		]
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("TransactGetItems status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Responses []transactGetItemResponse
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode TransactGetItems: %v", err)
	}
	if len(getResponse.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", getResponse.Responses)
	}
	if got := getResponse.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := getResponse.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", getResponse.Responses[0].Item)
	}
	if len(getResponse.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want empty item response", getResponse.Responses[1].Item)
	}
	if got := getResponse.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
}

func TestTransactWriteItemsAppliesPutUpdateDeleteAtomically(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"visits":{"N":"1"},"status":{"S":"active"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	writeRec := dynamodbRequest(t, server, "TransactWriteItems", `{
		"TransactItems":[
			{"ConditionCheck":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ConditionExpression":"#s = :active",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":active":{"S":"active"}}
			}},
			{"Update":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET visits = visits + :one, #s = :updated",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":one":{"N":"1"},":updated":{"S":"updated"}}
			}},
			{"Put":{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
				"ConditionExpression":"attribute_not_exists(pk)"
			}},
			{"Delete":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"missing"},"sk":{"S":"profile"}}
			}}
		]
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("TransactWriteItems status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	getUpdatedRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getUpdatedRec.Code != http.StatusOK {
		t.Fatalf("GetItem updated status = %d, body = %s", getUpdatedRec.Code, getUpdatedRec.Body.String())
	}
	var updatedResponse struct {
		Item item
	}
	if err := json.NewDecoder(getUpdatedRec.Body).Decode(&updatedResponse); err != nil {
		t.Fatalf("decode updated item: %v", err)
	}
	if got := updatedResponse.Item["visits"]["N"]; got != "2" {
		t.Fatalf("visits = %#v, want 2", got)
	}
	if got := updatedResponse.Item["status"]["S"]; got != "updated" {
		t.Fatalf("status = %#v, want updated", got)
	}

	getPutRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}}
	}`)
	if getPutRec.Code != http.StatusOK {
		t.Fatalf("GetItem put status = %d, body = %s", getPutRec.Code, getPutRec.Body.String())
	}
	var putResponse struct {
		Item item
	}
	if err := json.NewDecoder(getPutRec.Body).Decode(&putResponse); err != nil {
		t.Fatalf("decode put item: %v", err)
	}
	if got := putResponse.Item["name"]["S"]; got != "Grace" {
		t.Fatalf("put item name = %#v, want Grace", got)
	}
}

func TestTransactWriteItemsConditionFailureDoesNotPartiallyApply(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"status":{"S":"active"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	writeRec := dynamodbRequest(t, server, "TransactWriteItems", `{
		"TransactItems":[
			{"Put":{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}}
			}},
			{"ConditionCheck":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ConditionExpression":"#s = :inactive",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":inactive":{"S":"inactive"}}
			}}
		]
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("TransactWriteItems status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}
	if got := writeRec.Header().Get("X-Amzn-Errortype"); got != "TransactionCanceledException" {
		t.Fatalf("X-Amzn-Errortype = %q, want TransactionCanceledException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("item was partially applied after failed TransactWriteItems: %#v", getResponse.Item)
	}
}

