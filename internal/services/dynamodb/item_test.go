package dynamodb

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestItemAndReadOperationsRequireTableName(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	cases := []struct {
		name      string
		operation string
		body      string
	}{
		{
			name:      "GetItem",
			operation: "GetItem",
			body:      `{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}`,
		},
		{
			name:      "DeleteItem",
			operation: "DeleteItem",
			body:      `{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}`,
		},
		{
			name:      "UpdateItem",
			operation: "UpdateItem",
			body: `{
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Ada"}}
			}`,
		},
		{
			name:      "Query",
			operation: "Query",
			body: `{
				"KeyConditionExpression":"pk = :pk",
				"ExpressionAttributeValues":{":pk":{"S":"user#1"}}
			}`,
		},
		{
			name:      "Scan",
			operation: "Scan",
			body:      `{}`,
		},
	}

	for _, tc := range cases {
		rec := dynamodbRequest(t, server, tc.operation, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", tc.name, got)
		}
		if !strings.Contains(rec.Body.String(), "table name is required") {
			t.Fatalf("%s body missing table-name validation message: %s", tc.name, rec.Body.String())
		}
	}
}

func TestItemCRUD(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"age":{"N":"37"},
			"active":{"BOOL":true},
			"tags":{"SS":["engineer","admin"]},
			"meta":{"M":{"team":{"S":"core"}}},
			"scores":{"L":[{"N":"1"},{"N":"2"}]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
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
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name = %#v, want Ada", got)
	}
	if got := getResponse.Item["active"]["BOOL"]; got != true {
		t.Fatalf("active = %#v, want true", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	getRec = dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var emptyGetResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&emptyGetResponse); err != nil {
		t.Fatalf("decode empty GetItem: %v", err)
	}
	if len(emptyGetResponse.Item) != 0 {
		t.Fatalf("Item after delete = %#v, want empty", emptyGetResponse.Item)
	}
}

func TestPutAndDeleteItemRejectInvalidReturnValuesWithoutMutation(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	invalidPutRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
		"ReturnValues":"ALL_NEW"
	}`)
	if invalidPutRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid PutItem status = %d, want %d, body = %s", invalidPutRec.Code, http.StatusBadRequest, invalidPutRec.Body.String())
	}
	if got := invalidPutRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("invalid PutItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected PutItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected PutItem = %#v, want Ada", got)
	}

	invalidDeleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ReturnValues":"UPDATED_OLD"
	}`)
	if invalidDeleteRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid DeleteItem status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusBadRequest, invalidDeleteRec.Body.String())
	}
	if got := invalidDeleteRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("invalid DeleteItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec = dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	getResponse.Item = nil
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected DeleteItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected DeleteItem = %#v, want Ada", got)
	}
}

func TestItemWritesRejectOversizedItems(t *testing.T) {
	server := NewServer(Config{MaxItemBytes: 120})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"payload":{"S":"this value intentionally exceeds the configured local item byte limit"}
		}
	}`)
	if putRec.Code != http.StatusBadRequest {
		t.Fatalf("oversized PutItem status = %d, want %d, body = %s", putRec.Code, http.StatusBadRequest, putRec.Body.String())
	}
	if got := putRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	smallPut := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if smallPut.Code != http.StatusOK {
		t.Fatalf("small PutItem status = %d, body = %s", smallPut.Code, smallPut.Body.String())
	}
	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET payload = :payload",
		"ExpressionAttributeValues":{
			":payload":{"S":"this update also exceeds the configured local item byte limit"}
		}
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("oversized UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("UpdateItem X-Amzn-Errortype = %q, want ValidationException", got)
	}
	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected update: %v", err)
	}
	if _, ok := getResponse.Item["payload"]; ok {
		t.Fatalf("rejected oversized update mutated item: %#v", getResponse.Item)
	}
}

func TestItemWritesRejectInvalidAttributeValueShapes(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	for name, body := range map[string]string{
		"multiple types": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"broken":{"S":"x","N":"1"}}
		}`,
		"invalid number": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"broken":{"N":"not-a-number"}}
		}`,
		"empty set": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"broken":{"SS":[]}}
		}`,
		"duplicate string set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#4"},"sk":{"S":"profile"},"broken":{"SS":["red","red"]}}
		}`,
		"duplicate number set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#5"},"sk":{"S":"profile"},"broken":{"NS":["1","1"]}}
		}`,
		"invalid binary value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#6"},"sk":{"S":"profile"},"broken":{"B":"not-base64"}}
		}`,
		"invalid binary set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#7"},"sk":{"S":"profile"},"broken":{"BS":["YQ==","not-base64"]}}
		}`,
		"nested invalid value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#8"},"sk":{"S":"profile"},"meta":{"M":{"broken":{"NULL":false}}}}
		}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s PutItem status = %d, want %d, body = %s", name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", name, got)
		}
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.ItemCount != 0 {
		t.Fatalf("ItemCount after rejected invalid writes = %d, want 0", describeResponse.Table.ItemCount)
	}
}

func TestKeyOperationsRejectInvalidAttributeValueShapes(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	cases := []struct {
		name      string
		operation string
		body      string
	}{
		{
			name:      "GetItem",
			operation: "GetItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1","N":"1"},"sk":{"S":"profile"}}
			}`,
		},
		{
			name:      "DeleteItem",
			operation: "DeleteItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"NULL":false}}
			}`,
		},
		{
			name:      "UpdateItem",
			operation: "UpdateItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"N":"not-a-number"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Grace"}}
			}`,
		},
		{
			name:      "BatchGetItem",
			operation: "BatchGetItem",
			body: `{
				"RequestItems":{
					"Demo":{"Keys":[{"pk":{"S":"user#1"},"sk":{"SS":[]}}]}
				}
			}`,
		},
		{
			name:      "BatchWriteItem",
			operation: "BatchWriteItem",
			body: `{
				"RequestItems":{
					"Demo":[{"DeleteRequest":{"Key":{"pk":{"S":"user#1"},"sk":{"M":{"bad":{"NULL":false}}}}}}]
				}
			}`,
		},
	}

	for _, tc := range cases {
		rec := dynamodbRequest(t, server, tc.operation, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", tc.name, got)
		}
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem after rejected key operations status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected key operations: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected key operations = %#v, want Ada", got)
	}
}

func TestGetItemSupportsProjectionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"active":{"BOOL":true}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ProjectionExpression":"pk, #n",
		"ExpressionAttributeNames":{"#n":"name"},
		"ConsistentRead":true
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var response struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if _, ok := response.Item["pk"]; !ok {
		t.Fatalf("projected item missing pk: %#v", response.Item)
	}
	if got := response.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("projected name = %#v, want Ada", got)
	}
	if _, ok := response.Item["active"]; ok {
		t.Fatalf("projected item included unrequested active attribute: %#v", response.Item)
	}
}

func TestConditionalPutRejectsExistingItem(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	payload := `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_not_exists(pk)"
	}`
	first := dynamodbRequest(t, server, "PutItem", payload)
	if first.Code != http.StatusOK {
		t.Fatalf("first PutItem status = %d, body = %s", first.Code, first.Body.String())
	}
	second := dynamodbRequest(t, server, "PutItem", payload)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second PutItem status = %d, want %d", second.Code, http.StatusBadRequest)
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionalPutSupportsAttributeValueComparison(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	conditionalRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"2"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if conditionalRec.Code != http.StatusOK {
		t.Fatalf("conditional PutItem status = %d, body = %s", conditionalRec.Code, conditionalRec.Body.String())
	}

	rejectRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"3"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("rejected PutItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestItemWritesReturnOldItemOnConditionCheckFailure(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"version = :expected",
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":expected":{"N":"2"},
			":name":{"S":"Ada Lovelace"}
		},
		"ReturnValuesOnConditionCheckFailure":"ALL_OLD"
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
	var updateResponse struct {
		Item item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem conditional failure: %v", err)
	}
	if got := updateResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("conditional failure Item name = %#v, want Ada", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_not_exists(pk)",
		"ReturnValuesOnConditionCheckFailure":"ALL_OLD"
	}`)
	if deleteRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", deleteRec.Code, http.StatusBadRequest, deleteRec.Body.String())
	}
	var deleteResponse struct {
		Item item
	}
	if err := json.NewDecoder(deleteRec.Body).Decode(&deleteResponse); err != nil {
		t.Fatalf("decode DeleteItem conditional failure: %v", err)
	}
	if got := deleteResponse.Item["version"]["N"]; got != "1" {
		t.Fatalf("conditional failure Item version = %#v, want 1", got)
	}
}

