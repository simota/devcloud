package dynamodb

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestCreateTableStoresGSIAndQueryAcceptsIndexName(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if len(createResponse.TableDescription.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("GlobalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.GlobalSecondaryIndexes)
	}
	if got := createResponse.TableDescription.GlobalSecondaryIndexes[0].IndexStatus; got != "ACTIVE" {
		t.Fatalf("IndexStatus = %q, want ACTIVE", got)
	}

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoIndex",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["pk"]["S"] != "item#1" {
		t.Fatalf("Query response = %#v, want indexed item", queryResponse)
	}
}

func TestGSIQueryHonorsKeysOnlyProjection(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoKeysOnlyIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"KEYS_ONLY"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoKeysOnlyIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"},
			"name":{"S":"Ada"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoKeysOnlyIndex",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if response.Count != 1 {
		t.Fatalf("Count = %d, want 1", response.Count)
	}
	for _, name := range []string{"pk", "sk", "gpk", "gsk"} {
		if _, ok := response.Items[0][name]; !ok {
			t.Fatalf("KEYS_ONLY projection missing %s: %#v", name, response.Items[0])
		}
	}
	if _, ok := response.Items[0]["name"]; ok {
		t.Fatalf("KEYS_ONLY projection included non-key attribute: %#v", response.Items[0])
	}
}

func TestGSIScanHonorsIncludeProjection(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIncludeIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
			"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["name"]}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoIncludeIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"name":{"S":"Ada"},
			"secret":{"S":"hidden"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"DemoIncludeIndex",
		"IndexName":"gsi1"
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
	if response.Count != 1 || response.Items[0]["name"]["S"] != "Ada" {
		t.Fatalf("INCLUDE projection response = %#v, want projected name", response)
	}
	if _, ok := response.Items[0]["secret"]; ok {
		t.Fatalf("INCLUDE projection leaked non-projected attribute: %#v", response.Items[0])
	}
}

func TestCreateTableStoresLSIAndQueryAcceptsIndexName(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoLocalIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"status","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"LocalSecondaryIndexes":[{
			"IndexName":"lsi1",
			"KeySchema":[
				{"AttributeName":"pk","KeyType":"HASH"},
				{"AttributeName":"status","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if len(createResponse.TableDescription.LocalSecondaryIndexes) != 1 {
		t.Fatalf("LocalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.LocalSecondaryIndexes)
	}
	if got := createResponse.TableDescription.LocalSecondaryIndexes[0].IndexName; got != "lsi1" {
		t.Fatalf("IndexName = %q, want lsi1", got)
	}

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoLocalIndex",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"status":{"S":"active"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoLocalIndex",
		"IndexName":"lsi1",
		"KeyConditionExpression":"pk = :pk AND status = :status",
		"ExpressionAttributeValues":{
			":pk":{"S":"user#1"},
			":status":{"S":"active"}
		}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["sk"]["S"] != "profile" {
		t.Fatalf("Query response = %#v, want local-indexed item", queryResponse)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"DemoLocalIndex"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.LocalSecondaryIndexes[0].ItemCount != 1 {
		t.Fatalf("LSI ItemCount = %d, want 1", describeResponse.Table.LocalSecondaryIndexes[0].ItemCount)
	}
}

func TestScanWithIndexNameReturnsOnlyIndexedItems(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIndexScan",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#1"},"sk":{"S":"v1"},"gpk":{"S":"group#1"},"gsk":{"N":"2"},"name":{"S":"indexed-two"}}}`,
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#2"},"sk":{"S":"v1"},"gpk":{"S":"group#1"},"gsk":{"N":"1"},"name":{"S":"indexed-one"}}}`,
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#3"},"sk":{"S":"v1"},"name":{"S":"not-indexed"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"DemoIndexScan",
		"IndexName":"gsi1",
		"ProjectionExpression":"pk, gsk"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode indexed Scan: %v", err)
	}
	if response.Count != 2 || len(response.Items) != 2 {
		t.Fatalf("indexed Scan count/items = %d/%d, want 2/2: %#v", response.Count, len(response.Items), response.Items)
	}
	got := []any{response.Items[0]["gsk"]["N"], response.Items[1]["gsk"]["N"]}
	if !reflect.DeepEqual(got, []any{"1", "2"}) {
		t.Fatalf("indexed Scan order = %#v, want numeric index order 1, 2", got)
	}
	for _, item := range response.Items {
		if _, ok := item["name"]; ok {
			t.Fatalf("indexed Scan projection included unrequested name: %#v", item)
		}
	}
}

