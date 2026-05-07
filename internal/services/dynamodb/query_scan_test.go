package dynamodb

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestQuerySupportsKeyConditionLimitAndLastEvaluatedKey(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#001"},"kind":{"S":"event"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#002"},"kind":{"S":"event"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"event#001"},"kind":{"S":"event"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk AND begins_with(sk, :prefix)",
		"ExpressionAttributeValues":{
			":pk":{"S":"user#1"},
			":prefix":{"S":"event#"}
		},
		"Limit":1
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items            []item
		Count            int
		LastEvaluatedKey item
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if response.Count != 1 || len(response.Items) != 1 {
		t.Fatalf("Query count/items = %d/%d, want 1/1", response.Count, len(response.Items))
	}
	if got := response.Items[0]["pk"]["S"]; got != "user#1" {
		t.Fatalf("Query pk = %#v, want user#1", got)
	}
	if len(response.LastEvaluatedKey) == 0 {
		t.Fatalf("LastEvaluatedKey missing from limited Query response")
	}
}

func TestQueryOrdersNumericSortKeysByDynamoDBTypeSemantics(t *testing.T) {
	server := NewServer(Config{})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"NumericSort",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"score","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"score","KeyType":"RANGE"}
		]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"10"},"label":{"S":"ten"}}}`,
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"2"},"label":{"S":"two"}}}`,
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"1.5"},"label":{"S":"one-point-five"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"NumericSort",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items []item
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	got := []any{response.Items[0]["score"]["N"], response.Items[1]["score"]["N"], response.Items[2]["score"]["N"]}
	if !reflect.DeepEqual(got, []any{"1.5", "2", "10"}) {
		t.Fatalf("ascending scores = %#v, want numeric order 1.5, 2, 10", got)
	}

	descendingRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"NumericSort",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"group#1"}},
		"ScanIndexForward":false
	}`)
	if descendingRec.Code != http.StatusOK {
		t.Fatalf("descending Query status = %d, body = %s", descendingRec.Code, descendingRec.Body.String())
	}
	var descendingResponse struct {
		Items []item
	}
	if err := json.NewDecoder(descendingRec.Body).Decode(&descendingResponse); err != nil {
		t.Fatalf("decode descending Query: %v", err)
	}
	got = []any{descendingResponse.Items[0]["score"]["N"], descendingResponse.Items[1]["score"]["N"], descendingResponse.Items[2]["score"]["N"]}
	if !reflect.DeepEqual(got, []any{"10", "2", "1.5"}) {
		t.Fatalf("descending scores = %#v, want numeric order 10, 2, 1.5", got)
	}
}

func TestQuerySupportsSortKeyComparisonAndBetween(t *testing.T) {
	server := NewServer(Config{})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Scores",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"score","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"score","KeyType":"RANGE"}
		]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"1"},"label":{"S":"one"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"2"},"label":{"S":"two"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"3"},"label":{"S":"three"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#2"},"score":{"N":"2"},"label":{"S":"other"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Scores",
		"KeyConditionExpression":"pk = :pk AND score >= :min",
		"ExpressionAttributeValues":{
			":pk":{"S":"group#1"},
			":min":{"N":"2"}
		}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("comparison Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var comparisonResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&comparisonResponse); err != nil {
		t.Fatalf("decode comparison Query: %v", err)
	}
	if comparisonResponse.Count != 2 {
		t.Fatalf("comparison Query count = %d, want 2; items=%#v", comparisonResponse.Count, comparisonResponse.Items)
	}

	betweenRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Scores",
		"KeyConditionExpression":"pk = :pk AND score BETWEEN :low AND :high",
		"ExpressionAttributeValues":{
			":pk":{"S":"group#1"},
			":low":{"N":"2"},
			":high":{"N":"2"}
		}
	}`)
	if betweenRec.Code != http.StatusOK {
		t.Fatalf("BETWEEN Query status = %d, body = %s", betweenRec.Code, betweenRec.Body.String())
	}
	var betweenResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(betweenRec.Body).Decode(&betweenResponse); err != nil {
		t.Fatalf("decode BETWEEN Query: %v", err)
	}
	if betweenResponse.Count != 1 || betweenResponse.Items[0]["label"]["S"] != "two" {
		t.Fatalf("BETWEEN Query response = %#v, want score 2 item", betweenResponse)
	}
}

func TestScanSupportsFilterAndProjection(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true},"name":{"S":"Ada"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":false},"name":{"S":"Grace"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"ProjectionExpression":"pk, sk, active"
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
		t.Fatalf("Scan count/items = %d/%d, want 1/1", response.Count, len(response.Items))
	}
	if _, ok := response.Items[0]["name"]; ok {
		t.Fatalf("Scan projection included unrequested name attribute: %#v", response.Items[0])
	}
	if got := response.Items[0]["active"]["BOOL"]; got != true {
		t.Fatalf("Scan active = %#v, want true", got)
	}
}

func TestScanLimitAppliesBeforeFilterExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":false}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":true}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	firstRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"Limit":1
	}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first Scan status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstResponse struct {
		Items            []item
		Count            int
		ScannedCount     int
		LastEvaluatedKey item
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstResponse); err != nil {
		t.Fatalf("decode first Scan: %v", err)
	}
	if firstResponse.Count != 0 || len(firstResponse.Items) != 0 || firstResponse.ScannedCount != 1 {
		t.Fatalf("first Scan response = %#v, want no returned items after evaluating one filtered item", firstResponse)
	}
	if len(firstResponse.LastEvaluatedKey) == 0 {
		t.Fatalf("first Scan LastEvaluatedKey missing")
	}

	secondPayload, err := json.Marshal(map[string]any{
		"TableName":                 "Demo",
		"FilterExpression":          "active = :active",
		"ExpressionAttributeValues": map[string]attributeValue{":active": {"BOOL": true}},
		"ExclusiveStartKey":         firstResponse.LastEvaluatedKey,
		"Limit":                     1,
	})
	if err != nil {
		t.Fatalf("marshal second Scan payload: %v", err)
	}
	secondRec := dynamodbRequest(t, server, "Scan", string(secondPayload))
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second Scan status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		Items        []item
		Count        int
		ScannedCount int
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second Scan: %v", err)
	}
	if secondResponse.Count != 1 || len(secondResponse.Items) != 1 || secondResponse.ScannedCount != 1 {
		t.Fatalf("second Scan response = %#v, want one matching item after resume", secondResponse)
	}
	if got := secondResponse.Items[0]["pk"]["S"]; got != "user#2" {
		t.Fatalf("second Scan pk = %#v, want user#2", got)
	}
}

func TestQueryAndScanSupportSelectCount(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#001"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":false}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"Select":"COUNT"
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query COUNT status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query COUNT: %v", err)
	}
	if queryResponse.Count != 2 || queryResponse.Items != nil {
		t.Fatalf("Query COUNT response = %#v, want count only", queryResponse)
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"Select":"COUNT"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan COUNT status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan COUNT: %v", err)
	}
	if scanResponse.Count != 2 || scanResponse.Items != nil {
		t.Fatalf("Scan COUNT response = %#v, want count only", scanResponse)
	}
}

func TestSelectCountRejectsProjectionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"Select":"COUNT",
		"ProjectionExpression":"pk"
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("Scan COUNT with projection status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestQueryScanAndItemOperationsReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	assertConsumedCapacity(t, putRec, "Demo")

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	assertConsumedCapacity(t, getRec, "Demo")

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	assertConsumedCapacity(t, queryRec, "Demo")

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	assertConsumedCapacity(t, scanRec, "Demo")
}

func TestItemQueryAndScanRejectInvalidReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	for name, tc := range map[string]struct {
		operation string
		payload   string
	}{
		"PutItem": {
			operation: "PutItem",
			payload: `{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"GetItem": {
			operation: "GetItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"UpdateItem": {
			operation: "UpdateItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Grace"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"DeleteItem": {
			operation: "DeleteItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"Query": {
			operation: "Query",
			payload: `{
				"TableName":"Demo",
				"KeyConditionExpression":"pk = :pk",
				"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"Scan": {
			operation: "Scan",
			payload: `{
				"TableName":"Demo",
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
	} {
		rec := dynamodbRequest(t, server, tc.operation, tc.payload)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", name, got)
		}
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem after rejected requests status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected requests: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("item after rejected requests = %#v, want Ada", getResponse.Item)
	}
}

