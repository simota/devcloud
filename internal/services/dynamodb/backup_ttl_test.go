package dynamodb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestDescribeLimitsReturnsLocalCapacityEnvelope(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "DescribeLimits", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeLimits status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		AccountMaxReadCapacityUnits  int
		AccountMaxWriteCapacityUnits int
		TableMaxReadCapacityUnits    int
		TableMaxWriteCapacityUnits   int
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeLimits: %v", err)
	}
	if response.AccountMaxReadCapacityUnits <= 0 || response.AccountMaxWriteCapacityUnits <= 0 || response.TableMaxReadCapacityUnits <= 0 || response.TableMaxWriteCapacityUnits <= 0 {
		t.Fatalf("DescribeLimits response has non-positive capacity: %#v", response)
	}
}

func TestDescribeEndpointsReturnsLocalEndpointDiscoveryMetadata(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:8010"})
	rec := dynamodbRequest(t, server, "DescribeEndpoints", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeEndpoints status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Endpoints []struct {
			Address              string
			CachePeriodInMinutes int64
		}
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeEndpoints: %v", err)
	}
	if len(response.Endpoints) != 1 {
		t.Fatalf("Endpoints = %#v, want one local endpoint", response.Endpoints)
	}
	if response.Endpoints[0].Address != "127.0.0.1:8010" || response.Endpoints[0].CachePeriodInMinutes <= 0 {
		t.Fatalf("endpoint metadata = %#v, want configured address and positive cache period", response.Endpoints[0])
	}
}

func TestDescribeContinuousBackupsReturnsLocalMetadata(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "DescribeContinuousBackups", `{"TableName":"Demo"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeContinuousBackups status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		ContinuousBackupsDescription continuousBackupsDescription
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeContinuousBackups: %v", err)
	}
	if response.ContinuousBackupsDescription.ContinuousBackupsStatus != "ENABLED" {
		t.Fatalf("ContinuousBackupsStatus = %q, want ENABLED", response.ContinuousBackupsDescription.ContinuousBackupsStatus)
	}
	if response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != "DISABLED" {
		t.Fatalf("PointInTimeRecoveryStatus = %q, want DISABLED", response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
	}
}

func TestUpdateContinuousBackupsPersistsPointInTimeRecoveryMetadata(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateContinuousBackups", `{
		"TableName":"Demo",
		"PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateContinuousBackups status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	describeRec := dynamodbRequest(t, reloaded, "DescribeContinuousBackups", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeContinuousBackups status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var response struct {
		ContinuousBackupsDescription continuousBackupsDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeContinuousBackups: %v", err)
	}
	if response.ContinuousBackupsDescription.ContinuousBackupsStatus != "ENABLED" {
		t.Fatalf("ContinuousBackupsStatus = %q, want ENABLED", response.ContinuousBackupsDescription.ContinuousBackupsStatus)
	}
	if response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != "ENABLED" {
		t.Fatalf("PointInTimeRecoveryStatus = %q, want ENABLED", response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
	}
}

func TestDescribeContinuousBackupsRequiresExistingTable(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "DescribeContinuousBackups", `{"TableName":"Missing"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DescribeContinuousBackups status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestBackupMetadataCanBeCreatedListedDescribedDeletedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)

	createRec := dynamodbRequest(t, server, "CreateBackup", `{
		"TableName":"Demo",
		"BackupName":"snapshot-1"
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateBackup status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		BackupDetails backupDetails
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateBackup: %v", err)
	}
	if createResponse.BackupDetails.BackupName != "snapshot-1" || createResponse.BackupDetails.BackupStatus != "AVAILABLE" {
		t.Fatalf("BackupDetails = %#v, want available snapshot-1", createResponse.BackupDetails)
	}
	if createResponse.BackupDetails.BackupArn == "" {
		t.Fatalf("BackupArn is empty")
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	listRec := dynamodbRequest(t, reloaded, "ListBackups", `{"TableName":"Demo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListBackups status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listResponse struct {
		BackupSummaries []backupSummary
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListBackups: %v", err)
	}
	if len(listResponse.BackupSummaries) != 1 || listResponse.BackupSummaries[0].BackupArn != createResponse.BackupDetails.BackupArn {
		t.Fatalf("BackupSummaries = %#v, want created backup", listResponse.BackupSummaries)
	}

	describeRec := dynamodbRequest(t, reloaded, "DescribeBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeBackup status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		BackupDescription backupDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeBackup: %v", err)
	}
	if describeResponse.BackupDescription.SourceTableDetails.TableName != "Demo" {
		t.Fatalf("SourceTableDetails = %#v, want Demo table", describeResponse.BackupDescription.SourceTableDetails)
	}

	deleteRec := dynamodbRequest(t, reloaded, "DeleteBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteBackup status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missingRec := dynamodbRequest(t, reloaded, "DescribeBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if missingRec.Code != http.StatusBadRequest {
		t.Fatalf("DescribeBackup after delete status = %d, want %d, body = %s", missingRec.Code, http.StatusBadRequest, missingRec.Body.String())
	}
	if got := missingRec.Header().Get("X-Amzn-Errortype"); got != "BackupNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want BackupNotFoundException", got)
	}
}

func TestCreateBackupRequiresExistingTable(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "CreateBackup", `{"TableName":"Missing","BackupName":"snapshot-1"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CreateBackup status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestRestoreTableFromBackupRestoresSchemaAndItems(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	createRec := dynamodbRequest(t, server, "CreateBackup", `{
		"TableName":"Demo",
		"BackupName":"snapshot-restore"
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateBackup status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		BackupDetails backupDetails
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateBackup: %v", err)
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	restoreRec := dynamodbRequest(t, reloaded, "RestoreTableFromBackup", fmt.Sprintf(`{
		"BackupArn":%q,
		"TargetTableName":"DemoRestored"
	}`, createResponse.BackupDetails.BackupArn))
	if restoreRec.Code != http.StatusOK {
		t.Fatalf("RestoreTableFromBackup status = %d, body = %s", restoreRec.Code, restoreRec.Body.String())
	}
	var restoreResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(restoreRec.Body).Decode(&restoreResponse); err != nil {
		t.Fatalf("decode RestoreTableFromBackup: %v", err)
	}
	if restoreResponse.TableDescription.TableName != "DemoRestored" || restoreResponse.TableDescription.TableStatus != "ACTIVE" {
		t.Fatalf("restored TableDescription = %#v, want active DemoRestored", restoreResponse.TableDescription)
	}
	if len(restoreResponse.TableDescription.KeySchema) != 2 || restoreResponse.TableDescription.ItemCount != 1 {
		t.Fatalf("restored schema/count = %#v, want key schema and one item", restoreResponse.TableDescription)
	}

	afterRestore := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	getRec := dynamodbRequest(t, afterRestore, "GetItem", `{
		"TableName":"DemoRestored",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem restored status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode restored GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("restored item name = %#v, want Ada", got)
	}
}

func TestRestoreTableFromBackupRejectsMissingBackup(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "RestoreTableFromBackup", `{
		"BackupArn":"arn:aws:dynamodb:us-east-1:000000000000:table/Missing/backup/1-snapshot",
		"TargetTableName":"Restored"
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("RestoreTableFromBackup status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "BackupNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want BackupNotFoundException", got)
	}
}

func TestTimeToLiveMetadataCanBeDescribedUpdatedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	describeRec := dynamodbRequest(t, server, "DescribeTimeToLive", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTimeToLive status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var disabledResponse struct {
		TimeToLiveDescription timeToLiveDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&disabledResponse); err != nil {
		t.Fatalf("decode disabled DescribeTimeToLive: %v", err)
	}
	if disabledResponse.TimeToLiveDescription.TimeToLiveStatus != "DISABLED" {
		t.Fatalf("initial TTL status = %q, want DISABLED", disabledResponse.TimeToLiveDescription.TimeToLiveStatus)
	}

	updateRec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		TimeToLiveSpecification timeToLiveSpecification
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateTimeToLive: %v", err)
	}
	if !updateResponse.TimeToLiveSpecification.Enabled || updateResponse.TimeToLiveSpecification.AttributeName != "expiresAt" {
		t.Fatalf("TimeToLiveSpecification = %#v, want enabled expiresAt", updateResponse.TimeToLiveSpecification)
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	describeRec = dynamodbRequest(t, reloaded, "DescribeTimeToLive", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("reloaded DescribeTimeToLive status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var enabledResponse struct {
		TimeToLiveDescription timeToLiveDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&enabledResponse); err != nil {
		t.Fatalf("decode enabled DescribeTimeToLive: %v", err)
	}
	if enabledResponse.TimeToLiveDescription.TimeToLiveStatus != "ENABLED" || enabledResponse.TimeToLiveDescription.AttributeName != "expiresAt" {
		t.Fatalf("persisted TTL description = %#v, want enabled expiresAt", enabledResponse.TimeToLiveDescription)
	}
}

func TestUpdateTimeToLiveRequiresAttributeNameWhenEnabled(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true}
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateTimeToLive status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestTimeToLiveExpiresItemsAndPersistsCleanup(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}

	now := time.Now().Unix()
	putExpiredRec := dynamodbRequest(t, server, "PutItem", fmt.Sprintf(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"expired"},"expiresAt":{"N":"%d"}}
	}`, now-1))
	if putExpiredRec.Code != http.StatusOK {
		t.Fatalf("expired PutItem status = %d, body = %s", putExpiredRec.Code, putExpiredRec.Body.String())
	}
	putLiveRec := dynamodbRequest(t, server, "PutItem", fmt.Sprintf(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"live"},"expiresAt":{"N":"%d"}}
	}`, now+3600))
	if putLiveRec.Code != http.StatusOK {
		t.Fatalf("live PutItem status = %d, body = %s", putLiveRec.Code, putLiveRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{"TableName":"Demo"}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if len(scanResponse.Items) != 1 || scanResponse.Items[0]["sk"]["S"] != "live" {
		t.Fatalf("Scan Items = %#v, want only live item", scanResponse.Items)
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	getExpiredRec := dynamodbRequest(t, reloaded, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"expired"}}
	}`)
	var getExpiredResponse struct {
		Item item
	}
	if err := json.NewDecoder(getExpiredRec.Body).Decode(&getExpiredResponse); err != nil {
		t.Fatalf("decode expired GetItem: %v", err)
	}
	if len(getExpiredResponse.Item) != 0 {
		t.Fatalf("expired item persisted after TTL cleanup: %#v", getExpiredResponse.Item)
	}
}

