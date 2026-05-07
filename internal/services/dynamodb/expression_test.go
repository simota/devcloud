package dynamodb

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestScanAndConditionSupportComparisonOperators(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"age":{"N":"37"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"age":{"N":"42"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"age > :age",
		"ExpressionAttributeValues":{":age":{"N":"40"}}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#2" {
		t.Fatalf("Scan response = %#v, want user#2", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"age <> :age",
		"UpdateExpression":"SET ok = :ok",
		"ExpressionAttributeValues":{
			":age":{"N":"37"},
			":ok":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
}

func TestConditionAndFilterExpressionsSupportInPredicate(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"status":{"S":"open"},
			"priority":{"N":"2"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	conditionalRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#s IN (:open, :pending) AND priority IN (:one, :two)",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":open":{"S":"open"},
			":pending":{"S":"pending"},
			":one":{"N":"1"},
			":two":{"N":"2"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if conditionalRec.Code != http.StatusOK {
		t.Fatalf("conditional UpdateItem status = %d, body = %s", conditionalRec.Code, conditionalRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"#s IN (:closed, :open)",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":closed":{"S":"closed"},
			":open":{"S":"open"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if response.Count != 1 || len(response.Items) != 1 {
		t.Fatalf("Scan response = %#v, want one matching item", response)
	}
}

func TestConditionAndFilterExpressionsSupportAndPredicates(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"age":{"N":"37"},"active":{"BOOL":true},"version":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"age":{"N":"42"},"active":{"BOOL":true},"version":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"age":{"N":"45"},"active":{"BOOL":false},"version":{"N":"1"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"attribute_exists(age) AND active = :active AND age > :age",
		"ExpressionAttributeValues":{
			":active":{"BOOL":true},
			":age":{"N":"40"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#2" {
		t.Fatalf("Scan response = %#v, want user#2 only", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_exists(version) AND #v = :expected AND active = :active",
		"UpdateExpression":"SET #v = :next",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{
			":expected":{"N":"1"},
			":active":{"BOOL":true},
			":next":{"N":"2"}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["version"]["N"]; got != "2" {
		t.Fatalf("version = %#v, want 2", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_exists(version) AND #v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportOrAndNotPredicates(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"status":{"S":"open"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"status":{"S":"pending"},"active":{"BOOL":false},"deletedAt":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"status":{"S":"closed"},"active":{"BOOL":false}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active OR #s = :pending",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":active":{"BOOL":true},
			":pending":{"S":"pending"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 2 {
		t.Fatalf("OR Scan count = %d, want 2; items=%#v", scanResponse.Count, scanResponse.Items)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"NOT attribute_exists(deletedAt) OR #s = :closed",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":closed":{"S":"closed"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"NOT attribute_exists(deletedAt) OR active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportContains(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"bio":{"S":"Ada writes systems"},"tags":{"SS":["engineer","admin"]},"scores":{"L":[{"N":"1"},{"N":"2"}]}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"bio":{"S":"Grace documents systems"},"tags":{"SS":["engineer"]},"scores":{"L":[{"N":"3"}]}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"contains(#bio, :needle) AND contains(tags, :tag) AND contains(scores, :score)",
		"ExpressionAttributeNames":{"#bio":"bio"},
		"ExpressionAttributeValues":{
			":needle":{"S":"Ada"},
			":tag":{"S":"admin"},
			":score":{"N":"2"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#1" {
		t.Fatalf("Scan response = %#v, want user#1", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"contains(tags, :tag)",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeValues":{
			":tag":{"S":"admin"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"contains(tags, :tag)",
		"ExpressionAttributeValues":{":tag":{"S":"admin"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportAttributeTypeAndSize(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"active":{"BOOL":true},
			"tags":{"SS":["engineer","admin"]},
			"meta":{"M":{"team":{"S":"core"}}}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"attribute_type(meta, :mapType) AND size(tags) >= :tagCount AND size(#n) = :nameLength",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":mapType":{"S":"M"},
			":tagCount":{"N":"2"},
			":nameLength":{"N":"3"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#1" {
		t.Fatalf("Scan response = %#v, want user#1", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_type(active, :boolType) AND size(tags) <> :wrongCount",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeValues":{
			":boolType":{"S":"BOOL"},
			":wrongCount":{"N":"1"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}
}

