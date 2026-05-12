package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dynamodbsvc "devcloud/internal/services/dynamodb"
)

func TestDynamoDBDashboardPageAndAPIExposeTables(t *testing.T) {
	dynamo := dynamodbsvc.NewServer(dynamodbsvc.Config{Region: "us-east-1"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"}
		],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rec := httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"gpk":{"S":"group#1"},"name":{"S":"Ada"}}
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	rec = httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTimeToLive")
	rec = httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", rec.Code, rec.Body.String())
	}

	server := NewServer(Config{
		DynamoDBEndpoint:    "http://127.0.0.1:8000",
		DynamoDBRegion:      "us-east-1",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
	}, newDashboardStore(nil, nil))
	server.SetDynamoDB(dynamo)
	routes := server.routes()

	legacy := performRequest(routes, http.MethodGet, "/dynamodb")
	if legacy.Code != http.StatusMovedPermanently {
		t.Fatalf("legacy /dynamodb status = %d, want %d", legacy.Code, http.StatusMovedPermanently)
	}
	if got := legacy.Header().Get("Location"); got != "/dashboard/dynamodb" {
		t.Fatalf("legacy /dynamodb redirect target = %q, want /dashboard/dynamodb", got)
	}

	dashboardPage := performRequest(routes, http.MethodGet, "/dashboard/dynamodb")
	if dashboardPage.Code != http.StatusOK || !strings.Contains(dashboardPage.Body.String(), "devcloud Dashboard") {
		t.Fatalf("DynamoDB dashboard route changed: status=%d body=%s", dashboardPage.Code, dashboardPage.Body.String())
	}

	status := performRequest(routes, http.MethodGet, "/api/dynamodb/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":true`) {
		t.Fatalf("DynamoDB status = %d body=%s", status.Code, status.Body.String())
	}

	tables := performRequest(routes, http.MethodGet, "/api/dynamodb/tables")
	if tables.Code != http.StatusOK || !strings.Contains(tables.Body.String(), `"tableName":"Demo"`) {
		t.Fatalf("DynamoDB tables = %d body=%s", tables.Code, tables.Body.String())
	}

	items := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/items?limit=1")
	if items.Code != http.StatusOK || !strings.Contains(items.Body.String(), `"tableName":"Demo"`) || !strings.Contains(items.Body.String(), `"Ada"`) {
		t.Fatalf("DynamoDB table items = %d body=%s", items.Code, items.Body.String())
	}

	detail := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"tableName":"Demo"`) {
		t.Fatalf("DynamoDB table detail = %d body=%s", detail.Code, detail.Body.String())
	}

	indexes := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/indexes")
	if indexes.Code != http.StatusOK || !strings.Contains(indexes.Body.String(), `"IndexName":"gsi1"`) {
		t.Fatalf("DynamoDB table indexes = %d body=%s", indexes.Code, indexes.Body.String())
	}

	ttl := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/ttl")
	if ttl.Code != http.StatusOK || !strings.Contains(ttl.Body.String(), `"AttributeName":"expiresAt"`) {
		t.Fatalf("DynamoDB table ttl = %d body=%s", ttl.Code, ttl.Body.String())
	}

	streams := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/streams")
	if streams.Code != http.StatusOK || !strings.Contains(streams.Body.String(), `"streamEnabled":false`) {
		t.Fatalf("DynamoDB table streams = %d body=%s", streams.Code, streams.Body.String())
	}

	missing := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Missing/items")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing DynamoDB table items status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestDynamoDBDashboardManagementAPIsForwardThroughService(t *testing.T) {
	dynamo := dynamodbsvc.NewServer(dynamodbsvc.Config{Region: "us-east-1"})
	server := NewServer(Config{
		DynamoDBEndpoint:    "http://127.0.0.1:8000",
		DynamoDBRegion:      "us-east-1",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
	}, newDashboardStore(nil, nil))
	server.SetDynamoDB(dynamo)
	routes := server.routes()

	create := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables", `{"input":{
		"TableName":"Managed",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"BillingMode":"PAY_PER_REQUEST"
	}}`)
	if create.Code != http.StatusOK || !strings.Contains(create.Body.String(), `"TableName":"Managed"`) {
		t.Fatalf("dashboard CreateTable = %d body=%s", create.Code, create.Body.String())
	}

	put := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/items", `{"input":{
		"Item":{"pk":{"S":"user#1"},"name":{"S":"Ada"}}
	}}`)
	if put.Code != http.StatusOK {
		t.Fatalf("dashboard PutItem = %d body=%s", put.Code, put.Body.String())
	}

	update := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/items/update", `{"input":{
		"Key":{"pk":{"S":"user#1"}},
		"UpdateExpression":"SET #name = :name",
		"ExpressionAttributeNames":{"#name":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}}
	}}`)
	if update.Code != http.StatusOK {
		t.Fatalf("dashboard UpdateItem = %d body=%s", update.Code, update.Body.String())
	}

	ttl := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/ttl", `{"input":{
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}}`)
	if ttl.Code != http.StatusOK || !strings.Contains(ttl.Body.String(), `"AttributeName":"expiresAt"`) {
		t.Fatalf("dashboard UpdateTimeToLive = %d body=%s", ttl.Code, ttl.Body.String())
	}

	items := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Managed/items?limit=1")
	if items.Code != http.StatusOK || !strings.Contains(items.Body.String(), `"Grace"`) {
		t.Fatalf("dashboard table items after update = %d body=%s", items.Code, items.Body.String())
	}

	query := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/query", `{"input":{
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"Limit":1
	}}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"Count":1`) || !strings.Contains(query.Body.String(), `"Grace"`) {
		t.Fatalf("dashboard Query = %d body=%s", query.Code, query.Body.String())
	}

	scan := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/scan", `{"input":{
		"FilterExpression":"#name = :name",
		"ExpressionAttributeNames":{"#name":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}},
		"Limit":5
	}}`)
	if scan.Code != http.StatusOK || !strings.Contains(scan.Body.String(), `"ScannedCount":1`) || !strings.Contains(scan.Body.String(), `"Grace"`) {
		t.Fatalf("dashboard Scan = %d body=%s", scan.Code, scan.Body.String())
	}

	rejectedQuery := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/query", `{"input":{
		"TableName":"Other",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}}
	}}`)
	if rejectedQuery.Code != http.StatusBadRequest {
		t.Fatalf("dashboard Query table mismatch = %d, want %d", rejectedQuery.Code, http.StatusBadRequest)
	}

	rejectedDeleteItem := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/items/delete", `{"input":{
		"Key":{"pk":{"S":"user#1"}}
	},"confirmation":"wrong"}`)
	if rejectedDeleteItem.Code != http.StatusBadRequest {
		t.Fatalf("dashboard DeleteItem without confirmation = %d, want %d", rejectedDeleteItem.Code, http.StatusBadRequest)
	}

	deleteItem := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/items/delete", `{"input":{
		"Key":{"pk":{"S":"user#1"}}
	},"confirmation":"Managed"}`)
	if deleteItem.Code != http.StatusOK {
		t.Fatalf("dashboard DeleteItem = %d body=%s", deleteItem.Code, deleteItem.Body.String())
	}

	rejectedDeleteTable := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/delete", `{"input":{},"confirmation":""}`)
	if rejectedDeleteTable.Code != http.StatusBadRequest {
		t.Fatalf("dashboard DeleteTable without confirmation = %d, want %d", rejectedDeleteTable.Code, http.StatusBadRequest)
	}

	deleteTable := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Managed/delete", `{"input":{},"confirmation":"Managed"}`)
	if deleteTable.Code != http.StatusOK {
		t.Fatalf("dashboard DeleteTable = %d body=%s", deleteTable.Code, deleteTable.Body.String())
	}
}

func TestDynamoDBDashboardQueryScanAPIsForwardThroughService(t *testing.T) {
	dynamo := dynamodbsvc.NewServer(dynamodbsvc.Config{Region: "us-east-1"})
	server := NewServer(Config{
		DynamoDBEndpoint:    "http://127.0.0.1:8000",
		DynamoDBRegion:      "us-east-1",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
	}, newDashboardStore(nil, nil))
	server.SetDynamoDB(dynamo)
	routes := server.routes()

	create := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables", `{"input":{
		"TableName":"ReadModel",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"BillingMode":"PAY_PER_REQUEST"
	}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("dashboard CreateTable for Query/Scan = %d body=%s", create.Code, create.Body.String())
	}
	put := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/ReadModel/items", `{"input":{
		"Item":{"pk":{"S":"user#1"},"name":{"S":"Ada"}}
	}}`)
	if put.Code != http.StatusOK {
		t.Fatalf("dashboard PutItem for Query/Scan = %d body=%s", put.Code, put.Body.String())
	}

	query := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/ReadModel/query", `{"input":{
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"Limit":1
	}}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"Count":1`) || !strings.Contains(query.Body.String(), `"Ada"`) {
		t.Fatalf("dashboard Query = %d body=%s", query.Code, query.Body.String())
	}

	scan := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/ReadModel/scan", `{"input":{
		"FilterExpression":"#name = :name",
		"ExpressionAttributeNames":{"#name":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Ada"}},
		"Limit":5
	}}`)
	if scan.Code != http.StatusOK || !strings.Contains(scan.Body.String(), `"ScannedCount":1`) || !strings.Contains(scan.Body.String(), `"Ada"`) {
		t.Fatalf("dashboard Scan = %d body=%s", scan.Code, scan.Body.String())
	}
}

func TestDynamoDBDashboardQueryPaginationForwardsExclusiveStartKey(t *testing.T) {
	dynamo := dynamodbsvc.NewServer(dynamodbsvc.Config{Region: "us-east-1"})
	server := NewServer(Config{
		DynamoDBEndpoint:    "http://127.0.0.1:8000",
		DynamoDBRegion:      "us-east-1",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
	}, newDashboardStore(nil, nil))
	server.SetDynamoDB(dynamo)
	routes := server.routes()

	create := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables", `{"input":{
		"TableName":"Paginated",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"BillingMode":"PAY_PER_REQUEST"
	}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("dashboard CreateTable for pagination = %d body=%s", create.Code, create.Body.String())
	}
	for _, body := range []string{
		`{"input":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"001"},"name":{"S":"Ada"}}}}`,
		`{"input":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"002"},"name":{"S":"Grace"}}}}`,
	} {
		put := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Paginated/items", body)
		if put.Code != http.StatusOK {
			t.Fatalf("dashboard PutItem for pagination = %d body=%s", put.Code, put.Body.String())
		}
	}

	firstPage := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Paginated/query", `{"input":{
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"Limit":1
	}}`)
	if firstPage.Code != http.StatusOK || !strings.Contains(firstPage.Body.String(), `"Count":1`) || !strings.Contains(firstPage.Body.String(), `"LastEvaluatedKey"`) {
		t.Fatalf("dashboard Query first page = %d body=%s", firstPage.Code, firstPage.Body.String())
	}
	var firstPayload struct {
		LastEvaluatedKey map[string]any `json:"LastEvaluatedKey"`
	}
	if err := json.Unmarshal(firstPage.Body.Bytes(), &firstPayload); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPayload.LastEvaluatedKey) == 0 {
		t.Fatalf("first page did not include LastEvaluatedKey: %s", firstPage.Body.String())
	}

	secondInput := map[string]any{
		"input": map[string]any{
			"KeyConditionExpression":    "pk = :pk",
			"ExpressionAttributeValues": map[string]any{":pk": map[string]any{"S": "user#1"}},
			"ExclusiveStartKey":         firstPayload.LastEvaluatedKey,
			"Limit":                     float64(1),
		},
	}
	secondBody, err := json.Marshal(secondInput)
	if err != nil {
		t.Fatalf("encode second page: %v", err)
	}
	secondPage := performRequestWithBody(routes, http.MethodPost, "/api/dynamodb/tables/Paginated/query", string(secondBody))
	if secondPage.Code != http.StatusOK || !strings.Contains(secondPage.Body.String(), `"Count":1`) || !strings.Contains(secondPage.Body.String(), `"Grace"`) {
		t.Fatalf("dashboard Query second page = %d body=%s", secondPage.Code, secondPage.Body.String())
	}
}
