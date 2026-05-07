package dynamodb

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestTableLifecycle(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"BillingMode":"PAY_PER_REQUEST"
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
	if createResponse.TableDescription.TableName != "Demo" || createResponse.TableDescription.TableStatus != "ACTIVE" {
		t.Fatalf("TableDescription = %#v", createResponse.TableDescription)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.TableName != "Demo" || describeResponse.Table.TableStatus != "ACTIVE" {
		t.Fatalf("Table = %#v", describeResponse.Table)
	}

	listRec := dynamodbRequest(t, server, "ListTables", `{}`)
	var listResponse struct {
		TableNames []string
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTables: %v", err)
	}
	if !reflect.DeepEqual(listResponse.TableNames, []string{"Demo"}) {
		t.Fatalf("TableNames = %#v, want Demo", listResponse.TableNames)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteTable", `{"TableName":"Demo"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteTable status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	listRec = dynamodbRequest(t, server, "ListTables", `{}`)
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTables after delete: %v", err)
	}
	if len(listResponse.TableNames) != 0 {
		t.Fatalf("TableNames after delete = %#v, want empty", listResponse.TableNames)
	}
}

func TestListTablesPaginatesWithExclusiveStartTableName(t *testing.T) {
	server := NewServer(Config{})
	for _, tableName := range []string{"Bravo", "Alpha", "Charlie"} {
		rec := dynamodbRequest(t, server, "CreateTable", `{
			"TableName":"`+tableName+`",
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
		}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("CreateTable(%s) status = %d, body = %s", tableName, rec.Code, rec.Body.String())
		}
	}

	firstRec := dynamodbRequest(t, server, "ListTables", `{"Limit":2}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first ListTables status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstResponse struct {
		TableNames             []string
		LastEvaluatedTableName string
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstResponse); err != nil {
		t.Fatalf("decode first ListTables: %v", err)
	}
	if !reflect.DeepEqual(firstResponse.TableNames, []string{"Alpha", "Bravo"}) {
		t.Fatalf("first TableNames = %#v, want Alpha, Bravo", firstResponse.TableNames)
	}
	if firstResponse.LastEvaluatedTableName != "Bravo" {
		t.Fatalf("LastEvaluatedTableName = %q, want Bravo", firstResponse.LastEvaluatedTableName)
	}

	secondRec := dynamodbRequest(t, server, "ListTables", `{"ExclusiveStartTableName":"Bravo","Limit":2}`)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second ListTables status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		TableNames             []string
		LastEvaluatedTableName string
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second ListTables: %v", err)
	}
	if !reflect.DeepEqual(secondResponse.TableNames, []string{"Charlie"}) {
		t.Fatalf("second TableNames = %#v, want Charlie", secondResponse.TableNames)
	}
	if secondResponse.LastEvaluatedTableName != "" {
		t.Fatalf("second LastEvaluatedTableName = %q, want empty", secondResponse.LastEvaluatedTableName)
	}
}

func TestCreateTableRejectsDuplicate(t *testing.T) {
	server := NewServer(Config{})
	payload := `{
		"TableName":"Demo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`
	first := dynamodbRequest(t, server, "CreateTable", payload)
	if first.Code != http.StatusOK {
		t.Fatalf("first CreateTable status = %d, body = %s", first.Code, first.Body.String())
	}
	second := dynamodbRequest(t, server, "CreateTable", payload)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second CreateTable status = %d, want %d", second.Code, http.StatusBadRequest)
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "ResourceInUseException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceInUseException", got)
	}
}

func TestCreateTableRejectsWhenMaxTablesExceeded(t *testing.T) {
	server := NewServer(Config{MaxTables: 1})
	first := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"First",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first CreateTable status = %d, body = %s", first.Code, first.Body.String())
	}

	second := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Second",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second CreateTable status = %d, want %d, body = %s", second.Code, http.StatusBadRequest, second.Body.String())
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "LimitExceededException" {
		t.Fatalf("X-Amzn-Errortype = %q, want LimitExceededException", got)
	}
}

func TestUpdateTableUpdatesBillingModeSummary(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"BillingMode":"PROVISIONED"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateTable: %v", err)
	}
	if updateResponse.TableDescription.TableName != "Demo" {
		t.Fatalf("TableName = %q, want Demo", updateResponse.TableDescription.TableName)
	}
	if updateResponse.TableDescription.BillingModeSummary == nil || updateResponse.TableDescription.BillingModeSummary.BillingMode != "PROVISIONED" {
		t.Fatalf("BillingModeSummary = %#v, want PROVISIONED", updateResponse.TableDescription.BillingModeSummary)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.BillingModeSummary == nil || describeResponse.Table.BillingModeSummary.BillingMode != "PROVISIONED" {
		t.Fatalf("described BillingModeSummary = %#v, want PROVISIONED", describeResponse.Table.BillingModeSummary)
	}
}

func TestUpdateTableCanCreateAndDeleteGlobalSecondaryIndex(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"},
			"name":{"S":"Ada"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	createIndexRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"GlobalSecondaryIndexUpdates":[{
			"Create":{
				"IndexName":"gsi1",
				"KeySchema":[
					{"AttributeName":"gpk","KeyType":"HASH"},
					{"AttributeName":"gsk","KeyType":"RANGE"}
				],
				"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["name"]}
			}
		}]
	}`)
	if createIndexRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable create GSI status = %d, body = %s", createIndexRec.Code, createIndexRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createIndexRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode create GSI UpdateTable: %v", err)
	}
	if len(createResponse.TableDescription.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("GlobalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.GlobalSecondaryIndexes)
	}
	if createResponse.TableDescription.GlobalSecondaryIndexes[0].ItemCount != 1 {
		t.Fatalf("GSI ItemCount = %d, want 1", createResponse.TableDescription.GlobalSecondaryIndexes[0].ItemCount)
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("GSI Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode GSI Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["name"]["S"] != "Ada" {
		t.Fatalf("GSI Query response = %#v, want projected item", queryResponse)
	}

	deleteIndexRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"gsi1"}}]
	}`)
	if deleteIndexRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable delete GSI status = %d, body = %s", deleteIndexRec.Code, deleteIndexRec.Body.String())
	}
	var deleteResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(deleteIndexRec.Body).Decode(&deleteResponse); err != nil {
		t.Fatalf("decode delete GSI UpdateTable: %v", err)
	}
	if len(deleteResponse.TableDescription.GlobalSecondaryIndexes) != 0 {
		t.Fatalf("GlobalSecondaryIndexes after delete = %#v, want none", deleteResponse.TableDescription.GlobalSecondaryIndexes)
	}

	missingQueryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if missingQueryRec.Code != http.StatusBadRequest {
		t.Fatalf("Query deleted GSI status = %d, want %d, body = %s", missingQueryRec.Code, http.StatusBadRequest, missingQueryRec.Body.String())
	}
}

func TestUpdateTableRejectsInvalidGlobalSecondaryIndexDefinition(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[{"AttributeName":"gpk","AttributeType":"BOOL"}],
		"GlobalSecondaryIndexUpdates":[{
			"Create":{
				"IndexName":"gsi1",
				"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
				"Projection":{"ProjectionType":"ALL"}
			}
		}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateTable invalid GSI status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var response struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if len(response.Table.GlobalSecondaryIndexes) != 0 {
		t.Fatalf("rejected GSI update mutated table: %#v", response.Table.GlobalSecondaryIndexes)
	}
}

func TestStreamsMetadataCanBeCreatedListedDescribedUpdatedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"StreamDemo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
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
	if createResponse.TableDescription.LatestStreamArn == "" || createResponse.TableDescription.LatestStreamLabel == "" {
		t.Fatalf("stream identifiers were not set: %#v", createResponse.TableDescription)
	}
	if createResponse.TableDescription.StreamSpecification == nil || createResponse.TableDescription.StreamSpecification.StreamViewType != "NEW_AND_OLD_IMAGES" {
		t.Fatalf("StreamSpecification = %#v, want NEW_AND_OLD_IMAGES", createResponse.TableDescription.StreamSpecification)
	}

	listRec := dynamodbRequest(t, server, "ListStreams", `{"TableName":"StreamDemo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListStreams status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listResponse struct {
		Streams []streamSummary
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListStreams: %v", err)
	}
	if len(listResponse.Streams) != 1 || listResponse.Streams[0].StreamArn != createResponse.TableDescription.LatestStreamArn {
		t.Fatalf("ListStreams = %#v, want created stream", listResponse.Streams)
	}

	describeRec := dynamodbRequest(t, server, "DescribeStream", `{"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStream status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		StreamDescription streamDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeStream: %v", err)
	}
	if describeResponse.StreamDescription.TableName != "StreamDemo" || describeResponse.StreamDescription.StreamStatus != "ENABLED" {
		t.Fatalf("StreamDescription = %#v", describeResponse.StreamDescription)
	}
	if len(describeResponse.StreamDescription.Shards) != 1 {
		t.Fatalf("Shards = %#v, want one local shard", describeResponse.StreamDescription.Shards)
	}
	shardID := describeResponse.StreamDescription.Shards[0].ShardID
	iteratorRec := dynamodbRequest(t, server, "GetShardIterator", `{
		"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`",
		"ShardId":"`+shardID+`",
		"ShardIteratorType":"TRIM_HORIZON"
	}`)
	if iteratorRec.Code != http.StatusOK {
		t.Fatalf("GetShardIterator status = %d, body = %s", iteratorRec.Code, iteratorRec.Body.String())
	}
	var iteratorResponse struct {
		ShardIterator string
	}
	if err := json.NewDecoder(iteratorRec.Body).Decode(&iteratorResponse); err != nil {
		t.Fatalf("decode GetShardIterator: %v", err)
	}
	if iteratorResponse.ShardIterator == "" {
		t.Fatal("ShardIterator is empty")
	}

	recordsRec := dynamodbRequest(t, server, "GetRecords", `{"ShardIterator":"`+iteratorResponse.ShardIterator+`","Limit":10}`)
	if recordsRec.Code != http.StatusOK {
		t.Fatalf("GetRecords status = %d, body = %s", recordsRec.Code, recordsRec.Body.String())
	}
	var recordsResponse struct {
		NextShardIterator string
		Records           []map[string]any
	}
	if err := json.NewDecoder(recordsRec.Body).Decode(&recordsResponse); err != nil {
		t.Fatalf("decode GetRecords: %v", err)
	}
	if recordsResponse.NextShardIterator == "" {
		t.Fatal("NextShardIterator is empty")
	}
	if len(recordsResponse.Records) != 0 {
		t.Fatalf("Records = %#v, want empty stream records before mutations", recordsResponse.Records)
	}

	updateRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"StreamDemo",
		"StreamSpecification":{"StreamEnabled":false}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable stream disable status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	listRec = dynamodbRequest(t, reloaded, "ListStreams", `{"TableName":"StreamDemo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("reloaded ListStreams status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	listResponse.Streams = nil
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode reloaded ListStreams: %v", err)
	}
	if len(listResponse.Streams) != 0 {
		t.Fatalf("ListStreams after disable = %#v, want empty", listResponse.Streams)
	}
}

func TestStreamRecordsCaptureItemMutationsAndPersist(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"StreamDemo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
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

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"StreamDemo",
		"Item":{"pk":{"S":"user#1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"StreamDemo",
		"Key":{"pk":{"S":"user#1"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"StreamDemo",
		"Key":{"pk":{"S":"user#1"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	describeRec := dynamodbRequest(t, reloaded, "DescribeStream", `{"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStream status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		StreamDescription streamDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeStream: %v", err)
	}
	iteratorRec := dynamodbRequest(t, reloaded, "GetShardIterator", `{
		"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`",
		"ShardId":"`+describeResponse.StreamDescription.Shards[0].ShardID+`",
		"ShardIteratorType":"TRIM_HORIZON"
	}`)
	if iteratorRec.Code != http.StatusOK {
		t.Fatalf("GetShardIterator status = %d, body = %s", iteratorRec.Code, iteratorRec.Body.String())
	}
	var iteratorResponse struct {
		ShardIterator string
	}
	if err := json.NewDecoder(iteratorRec.Body).Decode(&iteratorResponse); err != nil {
		t.Fatalf("decode GetShardIterator: %v", err)
	}
	recordsRec := dynamodbRequest(t, reloaded, "GetRecords", `{"ShardIterator":"`+iteratorResponse.ShardIterator+`","Limit":2}`)
	if recordsRec.Code != http.StatusOK {
		t.Fatalf("GetRecords status = %d, body = %s", recordsRec.Code, recordsRec.Body.String())
	}
	var recordsResponse struct {
		NextShardIterator string
		Records           []streamRecord
	}
	if err := json.NewDecoder(recordsRec.Body).Decode(&recordsResponse); err != nil {
		t.Fatalf("decode GetRecords: %v", err)
	}
	if len(recordsResponse.Records) != 2 {
		t.Fatalf("first Records len = %d, want 2: %#v", len(recordsResponse.Records), recordsResponse.Records)
	}
	if recordsResponse.Records[0].EventName != "INSERT" || recordsResponse.Records[0].DynamoDB.NewImage["name"]["S"] != "Ada" {
		t.Fatalf("insert stream record = %#v", recordsResponse.Records[0])
	}
	if recordsResponse.Records[1].EventName != "MODIFY" || recordsResponse.Records[1].DynamoDB.OldImage["name"]["S"] != "Ada" || recordsResponse.Records[1].DynamoDB.NewImage["name"]["S"] != "Grace" {
		t.Fatalf("modify stream record = %#v", recordsResponse.Records[1])
	}

	nextRec := dynamodbRequest(t, reloaded, "GetRecords", `{"ShardIterator":"`+recordsResponse.NextShardIterator+`","Limit":2}`)
	if nextRec.Code != http.StatusOK {
		t.Fatalf("next GetRecords status = %d, body = %s", nextRec.Code, nextRec.Body.String())
	}
	var nextResponse struct {
		Records []streamRecord
	}
	if err := json.NewDecoder(nextRec.Body).Decode(&nextResponse); err != nil {
		t.Fatalf("decode next GetRecords: %v", err)
	}
	if len(nextResponse.Records) != 1 || nextResponse.Records[0].EventName != "REMOVE" || nextResponse.Records[0].DynamoDB.OldImage["name"]["S"] != "Grace" {
		t.Fatalf("remove stream record = %#v", nextResponse.Records)
	}
}

func TestCreateTableRejectsInvalidStreamSpecification(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"BadStream",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"ALL_IMAGES"}
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CreateTable status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestDescribeTableMissingReturnsResourceNotFound(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Missing"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

