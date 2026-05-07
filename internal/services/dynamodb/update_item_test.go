package dynamodb

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestUpdateItemSetReturnsAllNew(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name, visits = :one",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":name":{"S":"Ada Lovelace"},
			":one":{"N":"1"}
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
	if got := updateResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("updated name = %#v, want Ada Lovelace", got)
	}
	if got := updateResponse.Attributes["visits"]["N"]; got != "1" {
		t.Fatalf("visits = %#v, want 1", got)
	}
}

func TestUpdateItemSupportsReturnValuesVariants(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"visits":{"N":"1"},"obsolete":{"S":"old"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updatedNewRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name, visits = visits + :one REMOVE obsolete",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":name":{"S":"Ada Lovelace"},
			":one":{"N":"1"}
		},
		"ReturnValues":"UPDATED_NEW"
	}`)
	if updatedNewRec.Code != http.StatusOK {
		t.Fatalf("UPDATED_NEW status = %d, body = %s", updatedNewRec.Code, updatedNewRec.Body.String())
	}
	var updatedNewResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updatedNewRec.Body).Decode(&updatedNewResponse); err != nil {
		t.Fatalf("decode UPDATED_NEW response: %v", err)
	}
	if got := updatedNewResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("UPDATED_NEW name = %#v, want Ada Lovelace", got)
	}
	if got := updatedNewResponse.Attributes["visits"]["N"]; got != "2" {
		t.Fatalf("UPDATED_NEW visits = %#v, want 2", got)
	}
	if _, ok := updatedNewResponse.Attributes["obsolete"]; ok {
		t.Fatalf("UPDATED_NEW included removed attribute: %#v", updatedNewResponse.Attributes)
	}
	if _, ok := updatedNewResponse.Attributes["pk"]; ok {
		t.Fatalf("UPDATED_NEW included unchanged key: %#v", updatedNewResponse.Attributes)
	}

	updatedOldRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name REMOVE visits",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Ada Byron"}},
		"ReturnValues":"UPDATED_OLD"
	}`)
	if updatedOldRec.Code != http.StatusOK {
		t.Fatalf("UPDATED_OLD status = %d, body = %s", updatedOldRec.Code, updatedOldRec.Body.String())
	}
	var updatedOldResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updatedOldRec.Body).Decode(&updatedOldResponse); err != nil {
		t.Fatalf("decode UPDATED_OLD response: %v", err)
	}
	if got := updatedOldResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("UPDATED_OLD name = %#v, want Ada Lovelace", got)
	}
	if got := updatedOldResponse.Attributes["visits"]["N"]; got != "2" {
		t.Fatalf("UPDATED_OLD visits = %#v, want 2", got)
	}
	if _, ok := updatedOldResponse.Attributes["pk"]; ok {
		t.Fatalf("UPDATED_OLD included unchanged key: %#v", updatedOldResponse.Attributes)
	}

	allOldRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Countess Lovelace"}},
		"ReturnValues":"ALL_OLD"
	}`)
	if allOldRec.Code != http.StatusOK {
		t.Fatalf("ALL_OLD status = %d, body = %s", allOldRec.Code, allOldRec.Body.String())
	}
	var allOldResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(allOldRec.Body).Decode(&allOldResponse); err != nil {
		t.Fatalf("decode ALL_OLD response: %v", err)
	}
	if got := allOldResponse.Attributes["name"]["S"]; got != "Ada Byron" {
		t.Fatalf("ALL_OLD name = %#v, want Ada Byron", got)
	}

	noneRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Ada"}},
		"ReturnValues":"NONE"
	}`)
	if noneRec.Code != http.StatusOK {
		t.Fatalf("NONE status = %d, body = %s", noneRec.Code, noneRec.Body.String())
	}
	var noneResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(noneRec.Body).Decode(&noneResponse); err != nil {
		t.Fatalf("decode NONE response: %v", err)
	}
	if len(noneResponse.Attributes) != 0 {
		t.Fatalf("NONE Attributes = %#v, want empty", noneResponse.Attributes)
	}
}

func TestUpdateItemRejectsInvalidReturnValuesWithoutMutation(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}},
		"ReturnValues":"BROKEN"
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected update = %#v, want Ada", got)
	}
}

func TestUpdateItemSupportsSetAndRemove(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"obsolete":{"S":"remove-me"},
			"ttl":{"N":"123"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name REMOVE obsolete, #ttl",
		"ExpressionAttributeNames":{"#n":"name","#ttl":"ttl"},
		"ExpressionAttributeValues":{":name":{"S":"Ada Lovelace"}},
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
	if got := updateResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("updated name = %#v, want Ada Lovelace", got)
	}
	if _, ok := updateResponse.Attributes["obsolete"]; ok {
		t.Fatalf("obsolete attribute was not removed: %#v", updateResponse.Attributes)
	}
	if _, ok := updateResponse.Attributes["ttl"]; ok {
		t.Fatalf("ttl attribute was not removed: %#v", updateResponse.Attributes)
	}
}

func TestUpdateItemSupportsSetArithmeticAndIfNotExists(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"visits":{"N":"2"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET visits = visits + :one, remaining = :ten - :three, created = if_not_exists(created, :created)",
		"ExpressionAttributeValues":{
			":one":{"N":"1"},
			":ten":{"N":"10"},
			":three":{"N":"3"},
			":created":{"S":"initial"}
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
	if got := updateResponse.Attributes["visits"]["N"]; got != "3" {
		t.Fatalf("visits = %#v, want 3", got)
	}
	if got := updateResponse.Attributes["remaining"]["N"]; got != "7" {
		t.Fatalf("remaining = %#v, want 7", got)
	}
	if got := updateResponse.Attributes["created"]["S"]; got != "initial" {
		t.Fatalf("created = %#v, want initial", got)
	}

	secondRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET created = if_not_exists(created, :replacement)",
		"ExpressionAttributeValues":{":replacement":{"S":"replacement"}},
		"ReturnValues":"ALL_NEW"
	}`)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second UpdateItem status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second UpdateItem: %v", err)
	}
	if got := secondResponse.Attributes["created"]["S"]; got != "initial" {
		t.Fatalf("created after if_not_exists = %#v, want initial", got)
	}
}

func TestUpdateItemSupportsListAppend(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"events":{"L":[{"S":"created"}]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET events = list_append(events, :tail), prefixed = list_append(:head, events)",
		"ExpressionAttributeValues":{
			":tail":{"L":[{"S":"updated"},{"N":"2"}]},
			":head":{"L":[{"S":"first"}]}
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
	events := attributeValueList(updateResponse.Attributes["events"]["L"])
	if len(events) != 3 || events[0]["S"] != "created" || events[1]["S"] != "updated" || events[2]["N"] != "2" {
		t.Fatalf("events = %#v, want appended list", updateResponse.Attributes["events"])
	}
	prefixed := attributeValueList(updateResponse.Attributes["prefixed"]["L"])
	if len(prefixed) != 4 || prefixed[0]["S"] != "first" || prefixed[1]["S"] != "created" || prefixed[2]["S"] != "updated" || prefixed[3]["N"] != "2" {
		t.Fatalf("prefixed = %#v, want prepended current events", updateResponse.Attributes["prefixed"])
	}
}

func TestUpdateItemSupportsAddNumberAndSetUnion(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"visits":{"N":"2"},
			"tags":{"SS":["engineer"]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"ADD visits :one, tags :tags, scores :score",
		"ExpressionAttributeValues":{
			":one":{"N":"1"},
			":tags":{"SS":["admin","engineer"]},
			":score":{"NS":["10"]}
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
	if got := updateResponse.Attributes["visits"]["N"]; got != "3" {
		t.Fatalf("visits = %#v, want 3", got)
	}
	if got := updateResponse.Attributes["tags"]["SS"]; !reflect.DeepEqual(got, []any{"engineer", "admin"}) {
		t.Fatalf("tags = %#v, want engineer/admin union", got)
	}
	if got := updateResponse.Attributes["scores"]["NS"]; !reflect.DeepEqual(got, []any{"10"}) {
		t.Fatalf("scores = %#v, want new number set", got)
	}
}

func TestUpdateItemSupportsDeleteSetValues(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"tags":{"SS":["engineer","admin","owner"]},
			"scores":{"NS":["10"]},
			"bins":{"BS":["YQ==","Yg=="]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"DELETE tags :tags, scores :scores, bins :bins, missing :missing",
		"ExpressionAttributeValues":{
			":tags":{"SS":["admin"]},
			":scores":{"NS":["10"]},
			":bins":{"BS":["YQ=="]},
			":missing":{"SS":["unused"]}
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
	if got := updateResponse.Attributes["tags"]["SS"]; !reflect.DeepEqual(got, []any{"engineer", "owner"}) {
		t.Fatalf("tags = %#v, want admin removed", got)
	}
	if _, ok := updateResponse.Attributes["scores"]; ok {
		t.Fatalf("scores should be removed after deleting all members: %#v", updateResponse.Attributes)
	}
	if got := updateResponse.Attributes["bins"]["BS"]; !reflect.DeepEqual(got, []any{"Yg=="}) {
		t.Fatalf("bins = %#v, want remaining binary set member", got)
	}
	if _, ok := updateResponse.Attributes["missing"]; ok {
		t.Fatalf("missing attribute should remain absent: %#v", updateResponse.Attributes)
	}
}

func TestUpdateAndDeleteItemHonorConditionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	rejectedUpdate := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#v = :expected",
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#v":"version","#n":"name"},
		"ExpressionAttributeValues":{
			":expected":{"N":"2"},
			":name":{"S":"Grace"}
		}
	}`)
	if rejectedUpdate.Code != http.StatusBadRequest {
		t.Fatalf("rejected UpdateItem status = %d, want %d, body = %s", rejectedUpdate.Code, http.StatusBadRequest, rejectedUpdate.Body.String())
	}
	if got := rejectedUpdate.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("Item after conditional delete = %#v, want empty", getResponse.Item)
	}
}

