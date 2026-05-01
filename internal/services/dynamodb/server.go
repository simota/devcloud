package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Addr            string
	Region          string
	AuthMode        string
	AccessKeyID     string
	SecretAccessKey string
	StoragePath     string
	MaxItemBytes    int64
	MaxTables       int
}

type Server struct {
	config       Config
	mu           sync.Mutex
	tables       map[string]*tableState
	backups      map[string]backupDescription
	backupTables map[string]tableDescription
	backupItems  map[string]map[string]item
	loadErr      error
}

func NewServer(cfg Config) *Server {
	server := &Server{
		config:       cfg,
		tables:       map[string]*tableState{},
		backups:      map[string]backupDescription{},
		backupTables: map[string]tableDescription{},
		backupItems:  map[string]map[string]item{},
	}
	if cfg.StoragePath != "" {
		server.loadErr = server.load()
	}
	return server
}

type DashboardSnapshot struct {
	Running bool                     `json:"running"`
	Status  string                   `json:"status"`
	Region  string                   `json:"region"`
	Tables  []DashboardTableSnapshot `json:"tables"`
}

type DashboardTableSnapshot struct {
	TableName              string                            `json:"tableName"`
	TableStatus            string                            `json:"tableStatus"`
	ItemCount              int                               `json:"itemCount"`
	KeySchema              []keySchemaElement                `json:"keySchema,omitempty"`
	GlobalSecondaryIndexes []globalSecondaryIndexDescription `json:"globalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []localSecondaryIndexDescription  `json:"localSecondaryIndexes,omitempty"`
	LatestStreamArn        string                            `json:"latestStreamArn,omitempty"`
	LatestStreamLabel      string                            `json:"latestStreamLabel,omitempty"`
	StreamSpecification    *streamSpecification              `json:"streamSpecification,omitempty"`
	TimeToLiveDescription  timeToLiveDescription             `json:"timeToLiveDescription"`
}

type DashboardItemSnapshot struct {
	Key  map[string]any `json:"key"`
	Item map[string]any `json:"item"`
}

type attributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

type keySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}

type createTableRequest struct {
	TableName              string                        `json:"TableName"`
	AttributeDefinitions   []attributeDefinition         `json:"AttributeDefinitions"`
	KeySchema              []keySchemaElement            `json:"KeySchema"`
	GlobalSecondaryIndexes []globalSecondaryIndexRequest `json:"GlobalSecondaryIndexes"`
	LocalSecondaryIndexes  []localSecondaryIndexRequest  `json:"LocalSecondaryIndexes"`
	BillingMode            string                        `json:"BillingMode"`
	StreamSpecification    streamSpecification           `json:"StreamSpecification"`
}

type tableNameRequest struct {
	TableName string `json:"TableName"`
}

type updateContinuousBackupsRequest struct {
	TableName                        string                           `json:"TableName"`
	PointInTimeRecoverySpecification pointInTimeRecoverySpecification `json:"PointInTimeRecoverySpecification"`
}

type createBackupRequest struct {
	TableName  string `json:"TableName"`
	BackupName string `json:"BackupName"`
}

type describeBackupRequest struct {
	BackupARN string `json:"BackupArn"`
}

type deleteBackupRequest struct {
	BackupARN string `json:"BackupArn"`
}

type restoreTableFromBackupRequest struct {
	BackupARN       string `json:"BackupArn"`
	TargetTableName string `json:"TargetTableName"`
}

type listBackupsRequest struct {
	TableName               string `json:"TableName"`
	ExclusiveStartBackupARN string `json:"ExclusiveStartBackupArn"`
	Limit                   int    `json:"Limit"`
}

type pointInTimeRecoverySpecification struct {
	PointInTimeRecoveryEnabled bool `json:"PointInTimeRecoveryEnabled"`
}

type tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type tagResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []tag  `json:"Tags"`
}

type putResourcePolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
}

type getResourcePolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

type deleteResourcePolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsOfResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
}

type untagResourceRequest struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type listTablesRequest struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
	Limit                   int    `json:"Limit"`
}

type updateTableRequest struct {
	TableName                   string                       `json:"TableName"`
	AttributeDefinitions        []attributeDefinition        `json:"AttributeDefinitions"`
	BillingMode                 string                       `json:"BillingMode"`
	GlobalSecondaryIndexUpdates []globalSecondaryIndexUpdate `json:"GlobalSecondaryIndexUpdates"`
	StreamSpecification         *streamSpecification         `json:"StreamSpecification"`
}

type globalSecondaryIndexUpdate struct {
	Create *globalSecondaryIndexRequest `json:"Create,omitempty"`
	Delete *deleteGlobalSecondaryIndex  `json:"Delete,omitempty"`
	Update *updateGlobalSecondaryIndex  `json:"Update,omitempty"`
}

type deleteGlobalSecondaryIndex struct {
	IndexName string `json:"IndexName"`
}

type updateGlobalSecondaryIndex struct {
	IndexName string `json:"IndexName"`
}

type timeToLiveRequest struct {
	TableName               string                  `json:"TableName"`
	TimeToLiveSpecification timeToLiveSpecification `json:"TimeToLiveSpecification"`
}

type timeToLiveSpecification struct {
	AttributeName string `json:"AttributeName,omitempty"`
	Enabled       bool   `json:"Enabled"`
}

type timeToLiveDescription struct {
	AttributeName    string `json:"AttributeName,omitempty"`
	TimeToLiveStatus string `json:"TimeToLiveStatus"`
}

type continuousBackupsDescription struct {
	ContinuousBackupsStatus        string                         `json:"ContinuousBackupsStatus"`
	PointInTimeRecoveryDescription pointInTimeRecoveryDescription `json:"PointInTimeRecoveryDescription"`
}

type backupDescription struct {
	BackupDetails      backupDetails      `json:"BackupDetails"`
	SourceTableDetails sourceTableDetails `json:"SourceTableDetails"`
}

type backupDetails struct {
	BackupArn              string `json:"BackupArn"`
	BackupCreationDateTime int64  `json:"BackupCreationDateTime"`
	BackupName             string `json:"BackupName"`
	BackupSizeBytes        int    `json:"BackupSizeBytes"`
	BackupStatus           string `json:"BackupStatus"`
	BackupType             string `json:"BackupType"`
}

type backupSummary struct {
	BackupArn              string `json:"BackupArn"`
	BackupCreationDateTime int64  `json:"BackupCreationDateTime"`
	BackupName             string `json:"BackupName"`
	BackupSizeBytes        int    `json:"BackupSizeBytes"`
	BackupStatus           string `json:"BackupStatus"`
	BackupType             string `json:"BackupType"`
	TableArn               string `json:"TableArn"`
	TableName              string `json:"TableName"`
}

type sourceTableDetails struct {
	AttributeDefinitions  []attributeDefinition `json:"AttributeDefinitions,omitempty"`
	BillingMode           string                `json:"BillingMode,omitempty"`
	ItemCount             int                   `json:"ItemCount"`
	KeySchema             []keySchemaElement    `json:"KeySchema,omitempty"`
	TableArn              string                `json:"TableArn"`
	TableCreationDateTime int64                 `json:"TableCreationDateTime"`
	TableID               string                `json:"TableId"`
	TableName             string                `json:"TableName"`
	TableSizeBytes        int                   `json:"TableSizeBytes"`
}

type pointInTimeRecoveryDescription struct {
	PointInTimeRecoveryStatus string `json:"PointInTimeRecoveryStatus"`
}

type streamSpecification struct {
	StreamEnabled  bool   `json:"StreamEnabled"`
	StreamViewType string `json:"StreamViewType,omitempty"`
}

type listStreamsRequest struct {
	TableName               string `json:"TableName"`
	ExclusiveStartStreamArn string `json:"ExclusiveStartStreamArn"`
	Limit                   int    `json:"Limit"`
}

type describeStreamRequest struct {
	StreamArn             string `json:"StreamArn"`
	ExclusiveStartShardID string `json:"ExclusiveStartShardId"`
	Limit                 int    `json:"Limit"`
}

type getShardIteratorRequest struct {
	StreamArn         string `json:"StreamArn"`
	ShardID           string `json:"ShardId"`
	ShardIteratorType string `json:"ShardIteratorType"`
	SequenceNumber    string `json:"SequenceNumber"`
}

type getRecordsRequest struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

type streamIterator struct {
	StreamArn string `json:"streamArn"`
	ShardID   string `json:"shardId"`
	Position  int    `json:"position"`
}

type streamRecord struct {
	EventID      string            `json:"eventID"`
	EventName    string            `json:"eventName"`
	EventSource  string            `json:"eventSource"`
	EventVersion string            `json:"eventVersion"`
	AWSRegion    string            `json:"awsRegion"`
	DynamoDB     streamRecordImage `json:"dynamodb"`
}

type streamRecordImage struct {
	ApproximateCreationDateTime int64  `json:"ApproximateCreationDateTime"`
	Keys                        item   `json:"Keys"`
	NewImage                    item   `json:"NewImage,omitempty"`
	OldImage                    item   `json:"OldImage,omitempty"`
	SequenceNumber              string `json:"SequenceNumber"`
	SizeBytes                   int    `json:"SizeBytes"`
	StreamViewType              string `json:"StreamViewType"`
}

type streamSummary struct {
	StreamArn   string `json:"StreamArn"`
	StreamLabel string `json:"StreamLabel"`
	TableName   string `json:"TableName"`
}

type streamDescription struct {
	CreationRequestDateTime int64              `json:"CreationRequestDateTime"`
	KeySchema               []keySchemaElement `json:"KeySchema,omitempty"`
	LastEvaluatedShardID    string             `json:"LastEvaluatedShardId,omitempty"`
	Shards                  []shardDescription `json:"Shards"`
	StreamArn               string             `json:"StreamArn"`
	StreamLabel             string             `json:"StreamLabel"`
	StreamStatus            string             `json:"StreamStatus"`
	StreamViewType          string             `json:"StreamViewType"`
	TableName               string             `json:"TableName"`
}

type shardDescription struct {
	SequenceNumberRange sequenceNumberRange `json:"SequenceNumberRange"`
	ShardID             string              `json:"ShardId"`
}

type sequenceNumberRange struct {
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
}

type attributeValue map[string]any

type item map[string]attributeValue

type tableState struct {
	description            tableDescription
	items                  map[string]item
	streamRecords          []streamRecord
	tags                   map[string]string
	continuousBackups      *continuousBackupsDescription
	resourcePolicy         string
	resourcePolicyRevision string
}

type persistedState struct {
	Tables       map[string]persistedTable    `json:"tables"`
	Backups      map[string]backupDescription `json:"backups,omitempty"`
	BackupTables map[string]tableDescription  `json:"backupTables,omitempty"`
	BackupItems  map[string]map[string]item   `json:"backupItems,omitempty"`
}

type persistedTable struct {
	Description            tableDescription              `json:"description"`
	Items                  map[string]item               `json:"items"`
	StreamRecords          []streamRecord                `json:"streamRecords,omitempty"`
	Tags                   map[string]string             `json:"tags,omitempty"`
	ContinuousBackups      *continuousBackupsDescription `json:"continuousBackups,omitempty"`
	ResourcePolicy         string                        `json:"resourcePolicy,omitempty"`
	ResourcePolicyRevision string                        `json:"resourcePolicyRevision,omitempty"`
}

type itemBackup struct {
	item   item
	exists bool
}

type tableDescription struct {
	AttributeDefinitions   []attributeDefinition             `json:"AttributeDefinitions,omitempty"`
	BillingModeSummary     *billingModeSummary               `json:"BillingModeSummary,omitempty"`
	CreationDateTime       int64                             `json:"CreationDateTime"`
	GlobalSecondaryIndexes []globalSecondaryIndexDescription `json:"GlobalSecondaryIndexes,omitempty"`
	ItemCount              int                               `json:"ItemCount"`
	KeySchema              []keySchemaElement                `json:"KeySchema,omitempty"`
	LatestStreamArn        string                            `json:"LatestStreamArn,omitempty"`
	LatestStreamLabel      string                            `json:"LatestStreamLabel,omitempty"`
	LocalSecondaryIndexes  []localSecondaryIndexDescription  `json:"LocalSecondaryIndexes,omitempty"`
	StreamSpecification    *streamSpecification              `json:"StreamSpecification,omitempty"`
	TableArn               string                            `json:"TableArn"`
	TableName              string                            `json:"TableName"`
	TableSizeBytes         int                               `json:"TableSizeBytes"`
	TableStatus            string                            `json:"TableStatus"`
	TimeToLiveDescription  *timeToLiveDescription            `json:"TimeToLiveDescription,omitempty"`
}

type billingModeSummary struct {
	BillingMode string `json:"BillingMode"`
}

type globalSecondaryIndexRequest struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []keySchemaElement `json:"KeySchema"`
	Projection indexProjection    `json:"Projection"`
}

type localSecondaryIndexRequest struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []keySchemaElement `json:"KeySchema"`
	Projection indexProjection    `json:"Projection"`
}

type indexProjection struct {
	ProjectionType   string   `json:"ProjectionType"`
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}

type globalSecondaryIndexDescription struct {
	IndexArn              string             `json:"IndexArn"`
	IndexName             string             `json:"IndexName"`
	IndexSizeBytes        int                `json:"IndexSizeBytes"`
	IndexStatus           string             `json:"IndexStatus"`
	ItemCount             int                `json:"ItemCount"`
	KeySchema             []keySchemaElement `json:"KeySchema"`
	Projection            indexProjection    `json:"Projection"`
	ProvisionedThroughput map[string]int     `json:"ProvisionedThroughput,omitempty"`
}

type localSecondaryIndexDescription struct {
	IndexArn       string             `json:"IndexArn"`
	IndexName      string             `json:"IndexName"`
	IndexSizeBytes int                `json:"IndexSizeBytes"`
	ItemCount      int                `json:"ItemCount"`
	KeySchema      []keySchemaElement `json:"KeySchema"`
	Projection     indexProjection    `json:"Projection"`
}

type putItemRequest struct {
	TableName                           string                    `json:"TableName"`
	Item                                item                      `json:"Item"`
	ConditionExpression                 string                    `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues                        string                    `json:"ReturnValues"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity"`
}

type getItemRequest struct {
	TableName                string            `json:"TableName"`
	Key                      item              `json:"Key"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
	ConsistentRead           bool              `json:"ConsistentRead"`
	ReturnConsumedCapacity   string            `json:"ReturnConsumedCapacity"`
}

type deleteItemRequest struct {
	TableName                           string                    `json:"TableName"`
	Key                                 item                      `json:"Key"`
	ConditionExpression                 string                    `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues                        string                    `json:"ReturnValues"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity"`
}

type updateItemRequest struct {
	TableName                           string                    `json:"TableName"`
	Key                                 item                      `json:"Key"`
	ConditionExpression                 string                    `json:"ConditionExpression"`
	UpdateExpression                    string                    `json:"UpdateExpression"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues                        string                    `json:"ReturnValues"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure"`
	ReturnConsumedCapacity              string                    `json:"ReturnConsumedCapacity"`
}

type queryRequest struct {
	TableName                 string                    `json:"TableName"`
	IndexName                 string                    `json:"IndexName"`
	KeyConditionExpression    string                    `json:"KeyConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ProjectionExpression      string                    `json:"ProjectionExpression"`
	Select                    string                    `json:"Select"`
	ExclusiveStartKey         item                      `json:"ExclusiveStartKey"`
	Limit                     int                       `json:"Limit"`
	ScanIndexForward          *bool                     `json:"ScanIndexForward"`
	ReturnConsumedCapacity    string                    `json:"ReturnConsumedCapacity"`
}

type scanRequest struct {
	TableName                 string                    `json:"TableName"`
	IndexName                 string                    `json:"IndexName"`
	FilterExpression          string                    `json:"FilterExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ProjectionExpression      string                    `json:"ProjectionExpression"`
	Select                    string                    `json:"Select"`
	ExclusiveStartKey         item                      `json:"ExclusiveStartKey"`
	Limit                     int                       `json:"Limit"`
	ReturnConsumedCapacity    string                    `json:"ReturnConsumedCapacity"`
}

type executeStatementRequest struct {
	Statement              string           `json:"Statement"`
	Parameters             []attributeValue `json:"Parameters"`
	ConsistentRead         bool             `json:"ConsistentRead"`
	Limit                  int              `json:"Limit"`
	ReturnConsumedCapacity string           `json:"ReturnConsumedCapacity"`
}

type batchExecuteStatementRequest struct {
	Statements             []batchStatementRequest `json:"Statements"`
	ReturnConsumedCapacity string                  `json:"ReturnConsumedCapacity"`
}

type executeTransactionRequest struct {
	TransactStatements     []batchStatementRequest `json:"TransactStatements"`
	ReturnConsumedCapacity string                  `json:"ReturnConsumedCapacity"`
}

type batchStatementRequest struct {
	Statement                           string           `json:"Statement"`
	Parameters                          []attributeValue `json:"Parameters"`
	ConsistentRead                      bool             `json:"ConsistentRead"`
	ReturnValuesOnConditionCheckFailure string           `json:"ReturnValuesOnConditionCheckFailure"`
}

type batchStatementResponse struct {
	Error     *batchStatementError `json:"Error,omitempty"`
	Item      item                 `json:"Item,omitempty"`
	TableName string               `json:"TableName,omitempty"`
}

type batchStatementError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

type batchGetItemRequest struct {
	RequestItems           map[string]batchGetTableRequest `json:"RequestItems"`
	ReturnConsumedCapacity string                          `json:"ReturnConsumedCapacity"`
}

type batchGetTableRequest struct {
	Keys                     []item            `json:"Keys"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
	ConsistentRead           bool              `json:"ConsistentRead"`
}

type batchWriteItemRequest struct {
	RequestItems           map[string][]writeRequest `json:"RequestItems"`
	ReturnConsumedCapacity string                    `json:"ReturnConsumedCapacity"`
}

type transactGetItemsRequest struct {
	TransactItems []transactGetItem `json:"TransactItems"`
}

type transactGetItem struct {
	Get *transactGet `json:"Get,omitempty"`
}

type transactGet struct {
	TableName                string            `json:"TableName"`
	Key                      item              `json:"Key"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
}

type transactGetItemResponse struct {
	Item item `json:"Item,omitempty"`
}

type transactWriteItemsRequest struct {
	TransactItems []transactWriteItem `json:"TransactItems"`
}

type transactWriteItem struct {
	Put            *transactPut            `json:"Put,omitempty"`
	Update         *transactUpdate         `json:"Update,omitempty"`
	Delete         *transactDelete         `json:"Delete,omitempty"`
	ConditionCheck *transactConditionCheck `json:"ConditionCheck,omitempty"`
}

type transactPut struct {
	TableName                 string                    `json:"TableName"`
	Item                      item                      `json:"Item"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactUpdate struct {
	TableName                 string                    `json:"TableName"`
	Key                       item                      `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactDelete struct {
	TableName                 string                    `json:"TableName"`
	Key                       item                      `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactConditionCheck struct {
	TableName                 string                    `json:"TableName"`
	Key                       item                      `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type writeRequest struct {
	PutRequest    *putRequest    `json:"PutRequest,omitempty"`
	DeleteRequest *deleteRequest `json:"DeleteRequest,omitempty"`
}

type putRequest struct {
	Item item `json:"Item"`
}

type deleteRequest struct {
	Key item `json:"Key"`
}

type validatedWrite struct {
	state  *tableState
	key    string
	put    item
	delete bool
}

func (s *Server) Run(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) routes() http.Handler {
	return http.HandlerFunc(s.handle)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handle(w, r)
}

func (s *Server) Snapshot() DashboardSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := make([]DashboardTableSnapshot, 0, len(s.tables))
	for _, state := range s.tables {
		tables = append(tables, DashboardTableSnapshot{
			TableName:              state.description.TableName,
			TableStatus:            state.description.TableStatus,
			ItemCount:              state.description.ItemCount,
			KeySchema:              append([]keySchemaElement(nil), state.description.KeySchema...),
			GlobalSecondaryIndexes: append([]globalSecondaryIndexDescription(nil), state.description.GlobalSecondaryIndexes...),
			LocalSecondaryIndexes:  append([]localSecondaryIndexDescription(nil), state.description.LocalSecondaryIndexes...),
			LatestStreamArn:        state.description.LatestStreamArn,
			LatestStreamLabel:      state.description.LatestStreamLabel,
			StreamSpecification:    cloneStreamSpecification(state.description.StreamSpecification),
			TimeToLiveDescription:  ttlDescription(state.description),
		})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})
	return DashboardSnapshot{
		Running: true,
		Status:  "running",
		Region:  defaultString(s.config.Region, "us-east-1"),
		Tables:  tables,
	}
}

func (s *Server) TableSnapshot(tableName string) (DashboardTableSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[tableName]
	if !ok {
		return DashboardTableSnapshot{}, false
	}
	return DashboardTableSnapshot{
		TableName:              state.description.TableName,
		TableStatus:            state.description.TableStatus,
		ItemCount:              state.description.ItemCount,
		KeySchema:              append([]keySchemaElement(nil), state.description.KeySchema...),
		GlobalSecondaryIndexes: append([]globalSecondaryIndexDescription(nil), state.description.GlobalSecondaryIndexes...),
		LocalSecondaryIndexes:  append([]localSecondaryIndexDescription(nil), state.description.LocalSecondaryIndexes...),
		LatestStreamArn:        state.description.LatestStreamArn,
		LatestStreamLabel:      state.description.LatestStreamLabel,
		StreamSpecification:    cloneStreamSpecification(state.description.StreamSpecification),
		TimeToLiveDescription:  ttlDescription(state.description),
	}, true
}

func (s *Server) TableItems(tableName string, limit int) ([]DashboardItemSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[tableName]
	if !ok {
		return nil, false
	}
	source := sortedItems(state)
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	items := make([]DashboardItemSnapshot, 0, minInt(limit, len(source)))
	for _, candidate := range source {
		if len(items) == limit {
			break
		}
		key, err := extractKey(state.description, candidate.value)
		if err != nil {
			continue
		}
		items = append(items, DashboardItemSnapshot{
			Key:  dashboardItemPayload(key),
			Item: dashboardItemPayload(candidate.value),
		})
	}
	return items, true
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "devcloud-dynamodb")
	if s.loadErr != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to load dynamodb state")
		return
	}
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "ResourceNotFoundException", "not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "ValidationException", "method not allowed")
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" && !strings.HasPrefix(contentType, "application/x-amz-json-1.0") {
		writeError(w, http.StatusBadRequest, "ValidationException", "unsupported content type")
		return
	}
	if err := s.verifySignature(r); err != nil {
		writeSignatureError(w, err)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	const prefix = "DynamoDB_20120810."
	if !strings.HasPrefix(target, prefix) {
		writeError(w, http.StatusBadRequest, "UnknownOperationException", "unknown operation")
		return
	}
	if err := s.expireTTLItems(time.Now()); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	switch strings.TrimPrefix(target, prefix) {
	case "ListTables":
		s.handleListTables(w, r)
	case "CreateTable":
		s.handleCreateTable(w, r)
	case "DescribeTable":
		s.handleDescribeTable(w, r)
	case "DeleteTable":
		s.handleDeleteTable(w, r)
	case "UpdateTable":
		s.handleUpdateTable(w, r)
	case "DescribeLimits":
		s.handleDescribeLimits(w)
	case "DescribeEndpoints":
		s.handleDescribeEndpoints(w)
	case "DescribeTimeToLive":
		s.handleDescribeTimeToLive(w, r)
	case "UpdateTimeToLive":
		s.handleUpdateTimeToLive(w, r)
	case "DescribeContinuousBackups":
		s.handleDescribeContinuousBackups(w, r)
	case "UpdateContinuousBackups":
		s.handleUpdateContinuousBackups(w, r)
	case "CreateBackup":
		s.handleCreateBackup(w, r)
	case "DescribeBackup":
		s.handleDescribeBackup(w, r)
	case "ListBackups":
		s.handleListBackups(w, r)
	case "DeleteBackup":
		s.handleDeleteBackup(w, r)
	case "RestoreTableFromBackup":
		s.handleRestoreTableFromBackup(w, r)
	case "ListStreams":
		s.handleListStreams(w, r)
	case "DescribeStream":
		s.handleDescribeStream(w, r)
	case "GetShardIterator":
		s.handleGetShardIterator(w, r)
	case "GetRecords":
		s.handleGetRecords(w, r)
	case "PutItem":
		s.handlePutItem(w, r)
	case "GetItem":
		s.handleGetItem(w, r)
	case "DeleteItem":
		s.handleDeleteItem(w, r)
	case "UpdateItem":
		s.handleUpdateItem(w, r)
	case "Query":
		s.handleQuery(w, r)
	case "Scan":
		s.handleScan(w, r)
	case "BatchGetItem":
		s.handleBatchGetItem(w, r)
	case "BatchWriteItem":
		s.handleBatchWriteItem(w, r)
	case "ExecuteStatement":
		s.handleExecuteStatement(w, r)
	case "BatchExecuteStatement":
		s.handleBatchExecuteStatement(w, r)
	case "ExecuteTransaction":
		s.handleExecuteTransaction(w, r)
	case "TransactGetItems":
		s.handleTransactGetItems(w, r)
	case "TransactWriteItems":
		s.handleTransactWriteItems(w, r)
	case "TagResource":
		s.handleTagResource(w, r)
	case "ListTagsOfResource":
		s.handleListTagsOfResource(w, r)
	case "UntagResource":
		s.handleUntagResource(w, r)
	case "PutResourcePolicy":
		s.handlePutResourcePolicy(w, r)
	case "GetResourcePolicy":
		s.handleGetResourcePolicy(w, r)
	case "DeleteResourcePolicy":
		s.handleDeleteResourcePolicy(w, r)
	default:
		writeError(w, http.StatusBadRequest, "UnknownOperationException", "unknown operation")
	}
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	var request listTablesRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	s.mu.Unlock()

	sort.Strings(names)
	start := 0
	if request.ExclusiveStartTableName != "" {
		found := false
		for i, name := range names {
			if name == request.ExclusiveStartTableName {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start table name does not exist")
			return
		}
	}
	if start > len(names) {
		start = len(names)
	}
	end := len(names)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"TableNames": names[start:end]}
	if end < len(names) {
		response["LastEvaluatedTableName"] = names[end-1]
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	var request createTableRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if err := validateCreateTableRequest(request); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	createdAt := time.Now().Unix()
	description := tableDescription{
		AttributeDefinitions:   append([]attributeDefinition(nil), request.AttributeDefinitions...),
		BillingModeSummary:     &billingModeSummary{BillingMode: billingMode(request.BillingMode)},
		CreationDateTime:       createdAt,
		GlobalSecondaryIndexes: gsiDescriptions(defaultString(s.config.Region, "us-east-1"), request.TableName, request.GlobalSecondaryIndexes),
		ItemCount:              0,
		KeySchema:              append([]keySchemaElement(nil), request.KeySchema...),
		LocalSecondaryIndexes:  lsiDescriptions(defaultString(s.config.Region, "us-east-1"), request.TableName, request.LocalSecondaryIndexes),
		TableArn:               "arn:aws:dynamodb:" + defaultString(s.config.Region, "us-east-1") + ":000000000000:table/" + request.TableName,
		TableName:              request.TableName,
		TableSizeBytes:         0,
		TableStatus:            "ACTIVE",
	}
	if request.StreamSpecification.StreamEnabled {
		if err := validateStreamSpecification(request.StreamSpecification); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		enableStreamDescription(&description, defaultString(s.config.Region, "us-east-1"), request.StreamSpecification)
	}

	s.mu.Lock()
	if _, exists := s.tables[request.TableName]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "ResourceInUseException", "table already exists")
		return
	}
	if len(s.tables) >= s.maxTables() {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "LimitExceededException", "table limit exceeded")
		return
	}
	s.tables[request.TableName] = &tableState{
		description: description,
		items:       map[string]item{},
		tags:        map[string]string{},
	}
	if err := s.persistLocked(); err != nil {
		delete(s.tables, request.TableName)
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": description})
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	description, ok := s.table(request.TableName)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"Table": description})
}

func (s *Server) handleDeleteTable(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	if ok {
		delete(s.tables, request.TableName)
	}
	if ok {
		if err := s.persistLocked(); err != nil {
			s.tables[request.TableName] = state
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": state.description})
}

func (s *Server) handleUpdateTable(w http.ResponseWriter, r *http.Request) {
	var request updateTableRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.BillingMode != "" && request.BillingMode != "PAY_PER_REQUEST" && request.BillingMode != "PROVISIONED" {
		writeError(w, http.StatusBadRequest, "ValidationException", "billing mode must be PAY_PER_REQUEST or PROVISIONED")
		return
	}
	if request.StreamSpecification != nil {
		if err := validateStreamSpecification(*request.StreamSpecification); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous tableDescription
	if ok {
		previous = state.description
	}
	if ok && request.BillingMode != "" {
		state.description.BillingModeSummary = &billingModeSummary{BillingMode: request.BillingMode}
	}
	if ok && request.StreamSpecification != nil {
		if request.StreamSpecification.StreamEnabled {
			enableStreamDescription(&state.description, defaultString(s.config.Region, "us-east-1"), *request.StreamSpecification)
		} else {
			state.description.StreamSpecification = &streamSpecification{StreamEnabled: false}
			state.description.LatestStreamArn = ""
			state.description.LatestStreamLabel = ""
		}
	}
	if ok && len(request.GlobalSecondaryIndexUpdates) > 0 {
		if err := applyGlobalSecondaryIndexUpdates(&state.description, defaultString(s.config.Region, "us-east-1"), request.AttributeDefinitions, request.GlobalSecondaryIndexUpdates); err != nil {
			state.description = previous
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		updateIndexItemCounts(state)
	}
	var description tableDescription
	if ok {
		description = state.description
		if err := s.persistLocked(); err != nil {
			state.description = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": description})
}

func (s *Server) handleDescribeLimits(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]int{
		"AccountMaxReadCapacityUnits":  80000,
		"AccountMaxWriteCapacityUnits": 80000,
		"TableMaxReadCapacityUnits":    40000,
		"TableMaxWriteCapacityUnits":   40000,
	})
}

func (s *Server) handleDescribeEndpoints(w http.ResponseWriter) {
	address := defaultString(s.config.Addr, "127.0.0.1:8000")
	writeJSON(w, http.StatusOK, map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              address,
				"CachePeriodInMinutes": int64(1440),
			},
		},
	})
}

func (s *Server) handleDescribeContinuousBackups(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	description := continuousBackupsDescriptionForState(state)
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ContinuousBackupsDescription": description,
	})
}

func (s *Server) handleUpdateContinuousBackups(w http.ResponseWriter, r *http.Request) {
	var request updateContinuousBackupsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	status := "DISABLED"
	if request.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled {
		status = "ENABLED"
	}
	description := continuousBackupsDescription{
		ContinuousBackupsStatus: "ENABLED",
		PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{
			PointInTimeRecoveryStatus: status,
		},
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous *continuousBackupsDescription
	if ok {
		previous = cloneContinuousBackupsDescription(state.continuousBackups)
		state.continuousBackups = cloneContinuousBackupsDescription(&description)
		if err := s.persistLocked(); err != nil {
			state.continuousBackups = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ContinuousBackupsDescription": description})
}

func (s *Server) handleCreateBackup(w http.ResponseWriter, r *http.Request) {
	var request createBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.BackupName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	createdAt := time.Now().Unix()
	description := backupDescriptionForTable(state.description, request.BackupName, createdAt)
	if _, exists := s.backups[description.BackupDetails.BackupArn]; exists {
		writeError(w, http.StatusBadRequest, "BackupInUseException", "backup already exists")
		return
	}
	s.backups[description.BackupDetails.BackupArn] = description
	s.backupTables[description.BackupDetails.BackupArn] = cloneTableDescription(state.description)
	s.backupItems[description.BackupDetails.BackupArn] = cloneItems(state.items)
	if err := s.persistLocked(); err != nil {
		delete(s.backups, description.BackupDetails.BackupArn)
		delete(s.backupTables, description.BackupDetails.BackupArn)
		delete(s.backupItems, description.BackupDetails.BackupArn)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDetails": description.BackupDetails})
}

func (s *Server) handleDescribeBackup(w http.ResponseWriter, r *http.Request) {
	var request describeBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}

	s.mu.Lock()
	description, ok := s.backups[request.BackupARN]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDescription": description})
}

func (s *Server) handleListBackups(w http.ResponseWriter, r *http.Request) {
	var request listBackupsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	backups := make([]backupSummary, 0, len(s.backups))
	for _, description := range s.backups {
		if request.TableName != "" && description.SourceTableDetails.TableName != request.TableName {
			continue
		}
		backups = append(backups, backupSummaryForDescription(description))
	}
	s.mu.Unlock()

	sort.Slice(backups, func(i, j int) bool {
		if backups[i].TableName == backups[j].TableName {
			return backups[i].BackupArn < backups[j].BackupArn
		}
		return backups[i].TableName < backups[j].TableName
	})
	start := 0
	if request.ExclusiveStartBackupARN != "" {
		start = -1
		for i, backup := range backups {
			if backup.BackupArn == request.ExclusiveStartBackupARN {
				start = i + 1
				break
			}
		}
		if start == -1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start backup arn does not exist")
			return
		}
	}
	end := len(backups)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"BackupSummaries": backups[start:end]}
	if end < len(backups) {
		response["LastEvaluatedBackupArn"] = backups[end-1].BackupArn
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteBackup(w http.ResponseWriter, r *http.Request) {
	var request deleteBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}

	s.mu.Lock()
	description, ok := s.backups[request.BackupARN]
	tableDescriptionBackup := s.backupTables[request.BackupARN]
	items := s.backupItems[request.BackupARN]
	if ok {
		delete(s.backups, request.BackupARN)
		delete(s.backupTables, request.BackupARN)
		delete(s.backupItems, request.BackupARN)
		if err := s.persistLocked(); err != nil {
			s.backups[request.BackupARN] = description
			if tableDescriptionBackup.TableName != "" {
				s.backupTables[request.BackupARN] = tableDescriptionBackup
			}
			if items != nil {
				s.backupItems[request.BackupARN] = items
			}
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"BackupDescription": description})
}

func (s *Server) handleRestoreTableFromBackup(w http.ResponseWriter, r *http.Request) {
	var request restoreTableFromBackupRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.BackupARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "backup arn is required")
		return
	}
	if request.TargetTableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "target table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	backup, ok := s.backups[request.BackupARN]
	if !ok {
		writeError(w, http.StatusBadRequest, "BackupNotFoundException", "backup not found")
		return
	}
	if _, exists := s.tables[request.TargetTableName]; exists {
		writeError(w, http.StatusBadRequest, "ResourceInUseException", "table already exists")
		return
	}
	if len(s.tables) >= s.maxTables() {
		writeError(w, http.StatusBadRequest, "LimitExceededException", "table limit exceeded")
		return
	}

	description := s.restoredTableDescription(request.TargetTableName, backup, time.Now().Unix())
	items := cloneItems(s.backupItems[request.BackupARN])
	state := &tableState{
		description: description,
		items:       items,
		tags:        map[string]string{},
	}
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.tables[request.TargetTableName] = state
	if err := s.persistLocked(); err != nil {
		delete(s.tables, request.TargetTableName)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": state.description})
}

func (s *Server) handleDescribeTimeToLive(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var description timeToLiveDescription
	if ok {
		description = ttlDescription(state.description)
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TimeToLiveDescription": description})
}

func (s *Server) handleUpdateTimeToLive(w http.ResponseWriter, r *http.Request) {
	var request timeToLiveRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.TimeToLiveSpecification.Enabled && request.TimeToLiveSpecification.AttributeName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ttl attribute name is required when ttl is enabled")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous *timeToLiveDescription
	if ok {
		previous = cloneTTLDescription(state.description.TimeToLiveDescription)
		state.description.TimeToLiveDescription = ttlDescriptionFromSpecification(request.TimeToLiveSpecification)
		if err := s.persistLocked(); err != nil {
			state.description.TimeToLiveDescription = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TimeToLiveSpecification": request.TimeToLiveSpecification})
}

func (s *Server) handleListStreams(w http.ResponseWriter, r *http.Request) {
	var request listStreamsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	if request.TableName != "" {
		if _, ok := s.tables[request.TableName]; !ok {
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
	}
	streams := make([]streamSummary, 0, len(s.tables))
	for _, state := range s.tables {
		description := state.description
		if request.TableName != "" && description.TableName != request.TableName {
			continue
		}
		if description.LatestStreamArn == "" || description.StreamSpecification == nil || !description.StreamSpecification.StreamEnabled {
			continue
		}
		streams = append(streams, streamSummary{
			StreamArn:   description.LatestStreamArn,
			StreamLabel: description.LatestStreamLabel,
			TableName:   description.TableName,
		})
	}
	s.mu.Unlock()
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].TableName == streams[j].TableName {
			return streams[i].StreamArn < streams[j].StreamArn
		}
		return streams[i].TableName < streams[j].TableName
	})

	start := 0
	if request.ExclusiveStartStreamArn != "" {
		start = -1
		for i, stream := range streams {
			if stream.StreamArn == request.ExclusiveStartStreamArn {
				start = i + 1
				break
			}
		}
		if start == -1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start stream arn does not exist")
			return
		}
	}
	end := len(streams)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"Streams": streams[start:end]}
	if end < len(streams) {
		response["LastEvaluatedStreamArn"] = streams[end-1].StreamArn
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDescribeStream(w http.ResponseWriter, r *http.Request) {
	var request describeStreamRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.StreamArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "stream arn is required")
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}
	if request.ExclusiveStartShardID != "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start shard id does not exist")
		return
	}

	s.mu.Lock()
	var description tableDescription
	var ok bool
	for _, state := range s.tables {
		if state.description.LatestStreamArn == request.StreamArn && state.description.StreamSpecification != nil && state.description.StreamSpecification.StreamEnabled {
			description = state.description
			ok = true
			break
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"StreamDescription": streamDescriptionForTable(description)})
}

func (s *Server) handleGetShardIterator(w http.ResponseWriter, r *http.Request) {
	var request getShardIteratorRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.StreamArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "stream arn is required")
		return
	}
	if request.ShardID == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "shard id is required")
		return
	}
	switch request.ShardIteratorType {
	case "TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
	case "":
		writeError(w, http.StatusBadRequest, "ValidationException", "shard iterator type is required")
		return
	default:
		writeError(w, http.StatusBadRequest, "ValidationException", "unsupported shard iterator type")
		return
	}
	if (request.ShardIteratorType == "AT_SEQUENCE_NUMBER" || request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER") && request.SequenceNumber == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "sequence number is required")
		return
	}
	if !s.streamShardExists(request.StreamArn, request.ShardID) {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream shard not found")
		return
	}

	position := 0
	if request.ShardIteratorType == "LATEST" {
		position = s.streamRecordCount(request.StreamArn)
	}
	if request.ShardIteratorType == "AT_SEQUENCE_NUMBER" || request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" {
		var ok bool
		position, ok = s.streamPositionForSequence(request.StreamArn, request.SequenceNumber, request.ShardIteratorType == "AFTER_SEQUENCE_NUMBER")
		if !ok {
			writeError(w, http.StatusBadRequest, "TrimmedDataAccessException", "sequence number is invalid")
			return
		}
	}

	iterator, err := encodeStreamIterator(streamIterator{
		StreamArn: request.StreamArn,
		ShardID:   request.ShardID,
		Position:  position,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to create stream iterator")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ShardIterator": iterator})
}

func (s *Server) handleGetRecords(w http.ResponseWriter, r *http.Request) {
	var request getRecordsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ShardIterator == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "shard iterator is required")
		return
	}
	if request.Limit < 0 || request.Limit > 1000 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 1000")
		return
	}
	iterator, err := decodeStreamIterator(request.ShardIterator)
	if err != nil {
		writeError(w, http.StatusBadRequest, "TrimmedDataAccessException", "shard iterator is invalid")
		return
	}
	if !s.streamShardExists(iterator.StreamArn, iterator.ShardID) {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "stream shard not found")
		return
	}

	records := s.streamRecords(iterator.StreamArn, iterator.Position, request.Limit)
	nextIterator, err := encodeStreamIterator(streamIterator{
		StreamArn: iterator.StreamArn,
		ShardID:   iterator.ShardID,
		Position:  iterator.Position + len(records),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to create stream iterator")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"NextShardIterator": nextIterator,
		"Records":           records,
	})
}

func (s *Server) handlePutItem(w http.ResponseWriter, r *http.Request) {
	var request putItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if len(request.Item) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "item is required")
		return
	}
	if err := s.validateItemSize(request.Item); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validPutDeleteReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE or ALL_OLD")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Item)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}

	previousStreamLen := len(state.streamRecords)
	state.items[key] = cloneItem(request.Item)
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, streamEventName(existed, false), oldItem, request.Item, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		} else {
			delete(state.items, key)
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	response := map[string]any{}
	if returnValues == "ALL_OLD" && existed {
		response["Attributes"] = oldItem
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var request queryRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if strings.TrimSpace(request.KeyConditionExpression) == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "key condition expression is required")
		return
	}
	if err := validateSelect(request.Select, request.ProjectionExpression); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	if request.IndexName != "" && !tableHasIndex(state.description, request.IndexName) {
		writeError(w, http.StatusBadRequest, "ValidationException", "index not found")
		return
	}
	items := sortedItemsForQuery(state, request.IndexName)
	if request.ScanIndexForward != nil && !*request.ScanIndexForward {
		reverseItems(items)
	}
	startKey, err := startKeyString(state.description, request.ExclusiveStartKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := collectItems(state.description, request.IndexName, items, request.Limit, startKey, request.ProjectionExpression, request.ExpressionAttributeNames, false, func(candidate item) (bool, error) {
		return matchKeyCondition(request.KeyConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, candidate)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	applySelect(response, request.Select)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	var request scanRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if err := validateSelect(request.Select, request.ProjectionExpression); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	if request.IndexName != "" && !tableHasIndex(state.description, request.IndexName) {
		writeError(w, http.StatusBadRequest, "ValidationException", "index not found")
		return
	}
	startKey, err := startKeyString(state.description, request.ExclusiveStartKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := collectItems(state.description, request.IndexName, sortedItemsForScan(state, request.IndexName), request.Limit, startKey, request.ProjectionExpression, request.ExpressionAttributeNames, true, func(candidate item) (bool, error) {
		return matchFilter(request.FilterExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, candidate)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	applySelect(response, request.Select)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleBatchGetItem(w http.ResponseWriter, r *http.Request) {
	var request batchGetItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.RequestItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "request items are required")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := map[string][]item{}
	consumedCapacity := []map[string]any{}
	for tableName, tableRequest := range request.RequestItems {
		state, ok := s.tables[tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if len(tableRequest.Keys) == 0 {
			writeError(w, http.StatusBadRequest, "ValidationException", "keys are required")
			return
		}
		for _, keyValue := range tableRequest.Keys {
			key, err := itemKey(state.description, keyValue)
			if err != nil {
				writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
				return
			}
			found, ok := state.items[key]
			if !ok {
				continue
			}
			responses[tableName] = append(responses[tableName], projectItem(found, tableRequest.ProjectionExpression, tableRequest.ExpressionAttributeNames))
		}
		if _, ok := responses[tableName]; !ok {
			responses[tableName] = []item{}
		}
		appendBatchConsumedCapacity(&consumedCapacity, tableName, request.ReturnConsumedCapacity)
	}

	response := map[string]any{
		"Responses":       responses,
		"UnprocessedKeys": map[string]batchGetTableRequest{},
	}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleBatchWriteItem(w http.ResponseWriter, r *http.Request) {
	var request batchWriteItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.RequestItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "request items are required")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	writesToApply := []validatedWrite{}
	backups := map[*tableState]map[string]itemBackup{}
	touched := map[*tableState]bool{}
	consumedCapacity := []map[string]any{}
	for tableName, writes := range request.RequestItems {
		state, ok := s.tables[tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if len(writes) == 0 {
			writeError(w, http.StatusBadRequest, "ValidationException", "write requests are required")
			return
		}
		for _, write := range writes {
			if (write.PutRequest == nil) == (write.DeleteRequest == nil) {
				writeError(w, http.StatusBadRequest, "ValidationException", "each write request must contain exactly one operation")
				return
			}
			if write.PutRequest != nil {
				if len(write.PutRequest.Item) == 0 {
					writeError(w, http.StatusBadRequest, "ValidationException", "put item is required")
					return
				}
				if err := s.validateItemSize(write.PutRequest.Item); err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				key, err := itemKey(state.description, write.PutRequest.Item)
				if err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				rememberItemBackup(backups, state, key)
				writesToApply = append(writesToApply, validatedWrite{state: state, key: key, put: cloneItem(write.PutRequest.Item)})
			}
			if write.DeleteRequest != nil {
				key, err := itemKey(state.description, write.DeleteRequest.Key)
				if err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				rememberItemBackup(backups, state, key)
				writesToApply = append(writesToApply, validatedWrite{state: state, key: key, delete: true})
			}
		}
		appendBatchConsumedCapacity(&consumedCapacity, tableName, request.ReturnConsumedCapacity)
	}

	for _, write := range writesToApply {
		if write.delete {
			delete(write.state.items, write.key)
		} else {
			write.state.items[write.key] = write.put
		}
		touched[write.state] = true
	}
	for state := range touched {
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
	}
	if err := s.persistLocked(); err != nil {
		restoreBackups(backups)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{
		"UnprocessedItems": map[string][]writeRequest{},
	}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request executeStatementRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	statement, err := parsePartiQLSelect(request.Statement, request.Parameters)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[statement.tableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	source := sortedItemsForQuery(state, "")
	items := []item{}
	for _, candidate := range source {
		if !partiQLConditionsMatch(candidate.value, statement.conditions) {
			continue
		}
		items = append(items, projectPartiQLItem(candidate.value, statement.projections))
		if request.Limit > 0 && len(items) == request.Limit {
			break
		}
	}
	response := map[string]any{"Items": items}
	addConsumedCapacity(response, statement.tableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleBatchExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request batchExecuteStatementRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.Statements) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "statements are required")
		return
	}
	if len(request.Statements) > 25 {
		writeError(w, http.StatusBadRequest, "ValidationException", "statements must contain 25 or fewer entries")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]batchStatementResponse, 0, len(request.Statements))
	consumedCapacity := make([]map[string]any, 0, len(request.Statements))
	for _, statementRequest := range request.Statements {
		statement, err := parsePartiQLSelect(statementRequest.Statement, statementRequest.Parameters)
		if err != nil {
			responses = append(responses, batchStatementResponse{
				Error: &batchStatementError{Code: "ValidationError", Message: err.Error()},
			})
			continue
		}
		state, ok := s.tables[statement.tableName]
		if !ok {
			responses = append(responses, batchStatementResponse{
				TableName: statement.tableName,
				Error:     &batchStatementError{Code: "ResourceNotFound", Message: "table not found"},
			})
			appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
			continue
		}
		if !partiQLConditionsCoverKey(state.description, statement.conditions) {
			responses = append(responses, batchStatementResponse{
				TableName: statement.tableName,
				Error:     &batchStatementError{Code: "ValidationError", Message: "SELECT statement must include equality conditions for all key attributes"},
			})
			appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
			continue
		}

		response := batchStatementResponse{TableName: statement.tableName}
		for _, candidate := range sortedItemsForQuery(state, "") {
			if partiQLConditionsMatch(candidate.value, statement.conditions) {
				response.Item = projectPartiQLItem(candidate.value, statement.projections)
				break
			}
		}
		responses = append(responses, response)
		appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
	}

	response := map[string]any{"Responses": responses}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleExecuteTransaction(w http.ResponseWriter, r *http.Request) {
	var request executeTransactionRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactStatements) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction statements are required")
		return
	}
	if len(request.TransactStatements) > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction statements must contain 100 or fewer entries")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]batchStatementResponse, 0, len(request.TransactStatements))
	consumedCapacity := make([]map[string]any, 0, len(request.TransactStatements))
	for _, statementRequest := range request.TransactStatements {
		statement, err := parsePartiQLSelect(statementRequest.Statement, statementRequest.Parameters)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		state, ok := s.tables[statement.tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if !partiQLConditionsCoverKey(state.description, statement.conditions) {
			writeError(w, http.StatusBadRequest, "ValidationException", "SELECT statement must include equality conditions for all key attributes")
			return
		}

		response := batchStatementResponse{TableName: statement.tableName}
		for _, candidate := range sortedItemsForQuery(state, "") {
			if partiQLConditionsMatch(candidate.value, statement.conditions) {
				response.Item = projectPartiQLItem(candidate.value, statement.projections)
				break
			}
		}
		responses = append(responses, response)
		appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
	}

	response := map[string]any{"Responses": responses}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleTransactGetItems(w http.ResponseWriter, r *http.Request) {
	var request transactGetItemsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction items are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]transactGetItemResponse, 0, len(request.TransactItems))
	for _, transactionItem := range request.TransactItems {
		if transactionItem.Get == nil {
			writeError(w, http.StatusBadRequest, "ValidationException", "each transaction item must contain a Get operation")
			return
		}
		get := transactionItem.Get
		if get.TableName == "" {
			writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
			return
		}
		state, ok := s.tables[get.TableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		key, err := itemKey(state.description, get.Key)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		found, ok := state.items[key]
		if !ok {
			responses = append(responses, transactGetItemResponse{})
			continue
		}
		responses = append(responses, transactGetItemResponse{
			Item: projectItem(found, get.ProjectionExpression, get.ExpressionAttributeNames),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"Responses": responses})
}

func (s *Server) handleTransactWriteItems(w http.ResponseWriter, r *http.Request) {
	var request transactWriteItemsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction items are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	writesToApply := []validatedWrite{}
	backups := map[*tableState]map[string]itemBackup{}
	touched := map[*tableState]bool{}
	for _, transactionItem := range request.TransactItems {
		operationCount := countTransactWriteOperations(transactionItem)
		if operationCount != 1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "each transaction item must contain exactly one operation")
			return
		}
		if transactionItem.Put != nil {
			write, err := s.validateTransactPut(transactionItem.Put, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.Update != nil {
			write, err := s.validateTransactUpdate(transactionItem.Update, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.Delete != nil {
			write, err := s.validateTransactDelete(transactionItem.Delete, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.ConditionCheck != nil {
			if err := s.validateTransactConditionCheck(transactionItem.ConditionCheck); err != nil {
				writeTransactError(w, err)
				return
			}
		}
	}

	for _, write := range writesToApply {
		if write.delete {
			delete(write.state.items, write.key)
		} else {
			write.state.items[write.key] = write.put
		}
		touched[write.state] = true
	}
	for state := range touched {
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
	}
	if err := s.persistLocked(); err != nil {
		restoreBackups(backups)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleTagResource(w http.ResponseWriter, r *http.Request) {
	var request tagResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if len(request.Tags) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tags are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	if state.tags == nil {
		state.tags = map[string]string{}
	}
	projected := cloneTags(state.tags)
	for _, tag := range request.Tags {
		if tag.Key == "" {
			writeError(w, http.StatusBadRequest, "ValidationException", "tag key is required")
			return
		}
		projected[tag.Key] = tag.Value
	}
	if len(projected) > 50 {
		writeError(w, http.StatusBadRequest, "LimitExceededException", "tag limit exceeded")
		return
	}
	previous := cloneTags(state.tags)
	state.tags = projected
	if err := s.persistLocked(); err != nil {
		state.tags = previous
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleListTagsOfResource(w http.ResponseWriter, r *http.Request) {
	var request listTagsOfResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if request.NextToken != "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "next token is invalid")
		return
	}

	s.mu.Lock()
	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	tags := []tag{}
	if ok {
		for key, value := range state.tags {
			tags = append(tags, tag{Key: key, Value: value})
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})
	writeJSON(w, http.StatusOK, map[string]any{"Tags": tags})
}

func (s *Server) handleUntagResource(w http.ResponseWriter, r *http.Request) {
	var request untagResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if len(request.TagKeys) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tag keys are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previous := cloneTags(state.tags)
	for _, key := range request.TagKeys {
		delete(state.tags, key)
	}
	if err := s.persistLocked(); err != nil {
		state.tags = previous
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handlePutResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request putResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if strings.TrimSpace(request.Policy) == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "policy is required")
		return
	}
	if !json.Valid([]byte(request.Policy)) {
		writeError(w, http.StatusBadRequest, "ValidationException", "policy must be valid JSON")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previousPolicy := state.resourcePolicy
	previousRevision := state.resourcePolicyRevision
	state.resourcePolicy = request.Policy
	state.resourcePolicyRevision = resourcePolicyRevision(request.Policy)
	if err := s.persistLocked(); err != nil {
		state.resourcePolicy = previousPolicy
		state.resourcePolicyRevision = previousRevision
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"RevisionId": state.resourcePolicyRevision})
}

func (s *Server) handleGetResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request getResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	policy := ""
	revision := ""
	if ok {
		policy = state.resourcePolicy
		revision = state.resourcePolicyRevision
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	if policy == "" {
		writeError(w, http.StatusBadRequest, "PolicyNotFoundException", "resource policy not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"Policy": policy, "RevisionId": revision})
}

func (s *Server) handleDeleteResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request deleteResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previousPolicy := state.resourcePolicy
	previousRevision := state.resourcePolicyRevision
	state.resourcePolicy = ""
	state.resourcePolicyRevision = ""
	if err := s.persistLocked(); err != nil {
		state.resourcePolicy = previousPolicy
		state.resourcePolicyRevision = previousRevision
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleGetItem(w http.ResponseWriter, r *http.Request) {
	var request getItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	found, ok := state.items[key]
	response := map[string]any{}
	if !ok {
		addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
		writeJSON(w, http.StatusOK, response)
		return
	}
	response["Item"] = projectItem(found, request.ProjectionExpression, request.ExpressionAttributeNames)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, r *http.Request) {
	var request deleteItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validPutDeleteReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE or ALL_OLD")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}
	previousStreamLen := len(state.streamRecords)
	delete(state.items, key)
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, "REMOVE", oldItem, nil, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{}
	if returnValues == "ALL_OLD" && existed {
		response["Attributes"] = oldItem
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func validPutDeleteReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD":
		return true
	default:
		return false
	}
}

func validConditionFailureReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD":
		return true
	default:
		return false
	}
}

func (s *Server) handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	var request updateItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validUpdateReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, or UPDATED_NEW")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}
	updated := cloneItem(request.Key)
	oldItem, existed := state.items[key]
	if existing, ok := state.items[key]; ok {
		updated = cloneItem(existing)
	}
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}
	if err := applyUpdateExpression(updated, request.UpdateExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := s.validateItemSize(updated); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	previousStreamLen := len(state.streamRecords)
	state.items[key] = updated
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, streamEventName(existed, false), oldItem, updated, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		} else {
			delete(state.items, key)
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{}
	switch returnValues {
	case "NONE":
	case "ALL_NEW":
		response["Attributes"] = cloneItem(updated)
	case "ALL_OLD":
		if existed {
			response["Attributes"] = cloneItem(oldItem)
		}
	case "UPDATED_NEW":
		if attributes := updatedAttributes(oldItem, updated); len(attributes) > 0 {
			response["Attributes"] = attributes
		}
	case "UPDATED_OLD":
		if attributes := updatedOldAttributes(oldItem, updated); len(attributes) > 0 {
			response["Attributes"] = attributes
		}
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func validUpdateReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW":
		return true
	default:
		return false
	}
}

func updatedAttributes(oldValue item, newValue item) item {
	result := item{}
	for name, newAttr := range newValue {
		oldAttr, existed := oldValue[name]
		if !existed || !attributeValuesEqual(oldAttr, newAttr) {
			result[name] = cloneAttributeValue(newAttr)
		}
	}
	return result
}

func updatedOldAttributes(oldValue item, newValue item) item {
	result := item{}
	for name, oldAttr := range oldValue {
		newAttr, existsNow := newValue[name]
		if !existsNow || !attributeValuesEqual(oldAttr, newAttr) {
			result[name] = cloneAttributeValue(oldAttr)
		}
	}
	return result
}

func (s *Server) table(name string) (tableDescription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[name]
	if !ok {
		return tableDescription{}, false
	}
	return state.description, true
}

func (s *Server) load() error {
	path := filepath.Join(s.config.StoragePath, "state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	if persisted.Tables == nil {
		persisted.Tables = map[string]persistedTable{}
	}
	for name, table := range persisted.Tables {
		items := table.Items
		if items == nil {
			items = map[string]item{}
		}
		state := &tableState{
			description:            table.Description,
			items:                  items,
			streamRecords:          cloneStreamRecords(table.StreamRecords),
			tags:                   cloneTags(table.Tags),
			continuousBackups:      cloneContinuousBackupsDescription(table.ContinuousBackups),
			resourcePolicy:         table.ResourcePolicy,
			resourcePolicyRevision: table.ResourcePolicyRevision,
		}
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		s.tables[name] = state
	}
	for arn, backup := range persisted.Backups {
		if arn == "" {
			continue
		}
		s.backups[arn] = backup
	}
	for arn, description := range persisted.BackupTables {
		if arn == "" {
			continue
		}
		s.backupTables[arn] = cloneTableDescription(description)
	}
	for arn, items := range persisted.BackupItems {
		if arn == "" {
			continue
		}
		s.backupItems[arn] = cloneItems(items)
	}
	return nil
}

func (s *Server) persistLocked() error {
	if s.config.StoragePath == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
		return err
	}
	persisted := persistedState{
		Tables:       map[string]persistedTable{},
		Backups:      map[string]backupDescription{},
		BackupTables: map[string]tableDescription{},
		BackupItems:  map[string]map[string]item{},
	}
	for name, state := range s.tables {
		items := make(map[string]item, len(state.items))
		for key, value := range state.items {
			items[key] = cloneItem(value)
		}
		persisted.Tables[name] = persistedTable{
			Description:            state.description,
			Items:                  items,
			StreamRecords:          cloneStreamRecords(state.streamRecords),
			Tags:                   cloneTags(state.tags),
			ContinuousBackups:      cloneContinuousBackupsDescription(state.continuousBackups),
			ResourcePolicy:         state.resourcePolicy,
			ResourcePolicyRevision: state.resourcePolicyRevision,
		}
	}
	for arn, backup := range s.backups {
		persisted.Backups[arn] = backup
	}
	for arn, description := range s.backupTables {
		persisted.BackupTables[arn] = cloneTableDescription(description)
	}
	for arn, items := range s.backupItems {
		persisted.BackupItems[arn] = cloneItems(items)
	}
	path := filepath.Join(s.config.StoragePath, "state.json")
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(persisted)
	closeErr := file.Close()
	if encodeErr != nil {
		os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, path)
}

func (s *Server) maxItemBytes() int64 {
	if s.config.MaxItemBytes > 0 {
		return s.config.MaxItemBytes
	}
	return 400000
}

func (s *Server) maxTables() int {
	if s.config.MaxTables > 0 {
		return s.config.MaxTables
	}
	return 256
}

func (s *Server) validateItemSize(value item) error {
	if err := validateItemAttributeValues(value); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}
	if int64(len(encoded)) > s.maxItemBytes() {
		return fmt.Errorf("item size exceeds maximum of %d bytes", s.maxItemBytes())
	}
	return nil
}

func validateItemAttributeValues(value item) error {
	for name, attr := range value {
		if name == "" {
			return errors.New("attribute name is required")
		}
		if err := validateAttributeValue(attr, name); err != nil {
			return err
		}
	}
	return nil
}

func validateAttributeValue(value attributeValue, path string) error {
	if len(value) != 1 {
		return fmt.Errorf("attribute %s must contain exactly one AttributeValue type", path)
	}
	for kind, raw := range value {
		switch kind {
		case "S":
			if _, ok := raw.(string); !ok {
				return fmt.Errorf("attribute %s %s value must be a string", path, kind)
			}
		case "B":
			binary, ok := raw.(string)
			if !ok {
				return fmt.Errorf("attribute %s B value must be a string", path)
			}
			if _, err := base64.StdEncoding.DecodeString(binary); err != nil {
				return fmt.Errorf("attribute %s B value must be base64 encoded", path)
			}
		case "N":
			number, ok := raw.(string)
			if !ok {
				return fmt.Errorf("attribute %s N value must be a string", path)
			}
			if _, ok := new(big.Rat).SetString(number); !ok {
				return fmt.Errorf("attribute %s N value must be a valid number", path)
			}
		case "BOOL":
			if _, ok := raw.(bool); !ok {
				return fmt.Errorf("attribute %s BOOL value must be a boolean", path)
			}
		case "NULL":
			isNull, ok := raw.(bool)
			if !ok || !isNull {
				return fmt.Errorf("attribute %s NULL value must be true", path)
			}
		case "M":
			entries, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("attribute %s M value must be a map", path)
			}
			for name, nested := range entries {
				nestedValue, ok := nested.(map[string]any)
				if !ok {
					return fmt.Errorf("attribute %s.%s must be an AttributeValue object", path, name)
				}
				if err := validateAttributeValue(attributeValue(nestedValue), path+"."+name); err != nil {
					return err
				}
			}
		case "L":
			entries, ok := raw.([]any)
			if !ok {
				return fmt.Errorf("attribute %s L value must be a list", path)
			}
			for index, nested := range entries {
				nestedValue, ok := nested.(map[string]any)
				if !ok {
					return fmt.Errorf("attribute %s[%d] must be an AttributeValue object", path, index)
				}
				if err := validateAttributeValue(attributeValue(nestedValue), fmt.Sprintf("%s[%d]", path, index)); err != nil {
					return err
				}
			}
		case "SS", "BS":
			values, ok := stringSliceAttribute(value, kind)
			if !ok {
				return fmt.Errorf("attribute %s %s value must be a string list", path, kind)
			}
			if len(values) == 0 {
				return fmt.Errorf("attribute %s %s value must not be empty", path, kind)
			}
			if hasDuplicateString(values) {
				return fmt.Errorf("attribute %s %s value must not contain duplicates", path, kind)
			}
			if kind == "BS" {
				for _, binary := range values {
					if _, err := base64.StdEncoding.DecodeString(binary); err != nil {
						return fmt.Errorf("attribute %s BS value must contain base64 encoded strings", path)
					}
				}
			}
		case "NS":
			values, ok := stringSliceAttribute(value, kind)
			if !ok {
				return fmt.Errorf("attribute %s NS value must be a string list", path)
			}
			if len(values) == 0 {
				return fmt.Errorf("attribute %s NS value must not be empty", path)
			}
			if hasDuplicateString(values) {
				return fmt.Errorf("attribute %s NS value must not contain duplicates", path)
			}
			for _, number := range values {
				if _, ok := new(big.Rat).SetString(number); !ok {
					return fmt.Errorf("attribute %s NS value must contain valid numbers", path)
				}
			}
		default:
			return fmt.Errorf("attribute %s has unsupported AttributeValue type %s", path, kind)
		}
	}
	return nil
}

func hasDuplicateString(values []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func (s *Server) tableStateForARNLocked(resourceARN string) (*tableState, bool) {
	for _, state := range s.tables {
		if state.description.TableArn == resourceARN {
			return state, true
		}
	}
	return nil, false
}

func rememberItemBackup(backups map[*tableState]map[string]itemBackup, state *tableState, key string) {
	if backups[state] == nil {
		backups[state] = map[string]itemBackup{}
	}
	if _, ok := backups[state][key]; ok {
		return
	}
	existing, exists := state.items[key]
	backups[state][key] = itemBackup{item: cloneItem(existing), exists: exists}
}

func restoreBackups(backups map[*tableState]map[string]itemBackup) {
	for state, tableBackups := range backups {
		for key, backup := range tableBackups {
			if backup.exists {
				state.items[key] = backup.item
			} else {
				delete(state.items, key)
			}
		}
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
	}
}

type transactValidationError struct {
	name    string
	message string
}

func (e transactValidationError) Error() string {
	return e.message
}

func newTransactValidationError(name string, message string) error {
	return transactValidationError{name: name, message: message}
}

func writeTransactError(w http.ResponseWriter, err error) {
	var transactionErr transactValidationError
	if errors.As(err, &transactionErr) {
		writeError(w, http.StatusBadRequest, transactionErr.name, transactionErr.message)
		return
	}
	writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
}

func countTransactWriteOperations(transactionItem transactWriteItem) int {
	count := 0
	if transactionItem.Put != nil {
		count++
	}
	if transactionItem.Update != nil {
		count++
	}
	if transactionItem.Delete != nil {
		count++
	}
	if transactionItem.ConditionCheck != nil {
		count++
	}
	return count
}

func (s *Server) validateTransactPut(request *transactPut, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	if len(request.Item) == 0 {
		return validatedWrite{}, errors.New("item is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Item)
	if err != nil {
		return validatedWrite{}, err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	if err := s.validateItemSize(request.Item); err != nil {
		return validatedWrite{}, err
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, put: cloneItem(request.Item)}, nil
}

func (s *Server) validateTransactUpdate(request *transactUpdate, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return validatedWrite{}, err
	}
	updated := cloneItem(request.Key)
	oldItem, existed := state.items[key]
	if existed {
		updated = cloneItem(oldItem)
	}
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	if err := applyUpdateExpression(updated, request.UpdateExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues); err != nil {
		return validatedWrite{}, err
	}
	if err := s.validateItemSize(updated); err != nil {
		return validatedWrite{}, err
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, put: updated}, nil
}

func (s *Server) validateTransactDelete(request *transactDelete, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return validatedWrite{}, err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, delete: true}, nil
}

func (s *Server) validateTransactConditionCheck(request *transactConditionCheck) error {
	if request.TableName == "" {
		return errors.New("table name is required")
	}
	if strings.TrimSpace(request.ConditionExpression) == "" {
		return errors.New("condition expression is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	return nil
}

func decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	if err := json.NewDecoder(r.Body).Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "SerializationException", "invalid json request")
		return false
	}
	return true
}

func validateCreateTableRequest(request createTableRequest) error {
	if request.TableName == "" {
		return errors.New("table name is required")
	}
	if len(request.KeySchema) == 0 {
		return errors.New("key schema is required")
	}
	hashKeys := 0
	rangeKeys := 0
	attributes := map[string]bool{}
	for _, definition := range request.AttributeDefinitions {
		if definition.AttributeName == "" {
			return errors.New("attribute name is required")
		}
		switch definition.AttributeType {
		case "S", "N", "B":
		default:
			return errors.New("attribute type must be S, N, or B")
		}
		attributes[definition.AttributeName] = true
	}
	for _, element := range request.KeySchema {
		if element.AttributeName == "" {
			return errors.New("key attribute name is required")
		}
		if !attributes[element.AttributeName] {
			return errors.New("key schema attributes must be defined")
		}
		switch element.KeyType {
		case "HASH":
			hashKeys++
		case "RANGE":
			rangeKeys++
		default:
			return errors.New("key type must be HASH or RANGE")
		}
	}
	if hashKeys != 1 || rangeKeys > 1 || len(request.KeySchema) > 2 {
		return errors.New("key schema must include one HASH key and at most one RANGE key")
	}
	if request.BillingMode != "" && request.BillingMode != "PAY_PER_REQUEST" && request.BillingMode != "PROVISIONED" {
		return errors.New("billing mode must be PAY_PER_REQUEST or PROVISIONED")
	}
	if request.StreamSpecification.StreamEnabled {
		if err := validateStreamSpecification(request.StreamSpecification); err != nil {
			return err
		}
	}
	indexNames := map[string]bool{}
	for _, index := range request.GlobalSecondaryIndexes {
		if index.IndexName == "" {
			return errors.New("global secondary index name is required")
		}
		if indexNames[index.IndexName] {
			return errors.New("global secondary index names must be unique")
		}
		indexNames[index.IndexName] = true
		if len(index.KeySchema) == 0 {
			return errors.New("global secondary index key schema is required")
		}
		indexHashKeys := 0
		indexRangeKeys := 0
		for _, element := range index.KeySchema {
			if !attributes[element.AttributeName] {
				return errors.New("global secondary index key schema attributes must be defined")
			}
			switch element.KeyType {
			case "HASH":
				indexHashKeys++
			case "RANGE":
				indexRangeKeys++
			default:
				return errors.New("global secondary index key type must be HASH or RANGE")
			}
		}
		if indexHashKeys != 1 || indexRangeKeys > 1 || len(index.KeySchema) > 2 {
			return errors.New("global secondary index key schema must include one HASH key and at most one RANGE key")
		}
		if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
			return errors.New("global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
		}
	}
	for _, index := range request.LocalSecondaryIndexes {
		if index.IndexName == "" {
			return errors.New("local secondary index name is required")
		}
		if indexNames[index.IndexName] {
			return errors.New("secondary index names must be unique")
		}
		indexNames[index.IndexName] = true
		if len(index.KeySchema) != 2 {
			return errors.New("local secondary index key schema must include table HASH key and one RANGE key")
		}
		if index.KeySchema[0].KeyType != "HASH" || index.KeySchema[0].AttributeName != tableHashKey(request.KeySchema) {
			return errors.New("local secondary index HASH key must match table HASH key")
		}
		rangeKeys := 0
		for _, element := range index.KeySchema {
			if !attributes[element.AttributeName] {
				return errors.New("local secondary index key schema attributes must be defined")
			}
			switch element.KeyType {
			case "HASH":
			case "RANGE":
				rangeKeys++
			default:
				return errors.New("local secondary index key type must be HASH or RANGE")
			}
		}
		if rangeKeys != 1 {
			return errors.New("local secondary index key schema must include one RANGE key")
		}
		if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
			return errors.New("local secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
		}
	}
	return nil
}

func tableHashKey(schema []keySchemaElement) string {
	for _, element := range schema {
		if element.KeyType == "HASH" {
			return element.AttributeName
		}
	}
	return ""
}

func tableHasIndex(description tableDescription, indexName string) bool {
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return true
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return true
		}
	}
	return false
}

func gsiDescriptions(region string, tableName string, indexes []globalSecondaryIndexRequest) []globalSecondaryIndexDescription {
	if len(indexes) == 0 {
		return nil
	}
	descriptions := make([]globalSecondaryIndexDescription, 0, len(indexes))
	for _, index := range indexes {
		projection := index.Projection
		if projection.ProjectionType == "" {
			projection.ProjectionType = "ALL"
		}
		descriptions = append(descriptions, globalSecondaryIndexDescription{
			IndexArn:       "arn:aws:dynamodb:" + region + ":000000000000:table/" + tableName + "/index/" + index.IndexName,
			IndexName:      index.IndexName,
			IndexSizeBytes: 0,
			IndexStatus:    "ACTIVE",
			ItemCount:      0,
			KeySchema:      append([]keySchemaElement(nil), index.KeySchema...),
			Projection:     projection,
		})
	}
	return descriptions
}

func lsiDescriptions(region string, tableName string, indexes []localSecondaryIndexRequest) []localSecondaryIndexDescription {
	if len(indexes) == 0 {
		return nil
	}
	descriptions := make([]localSecondaryIndexDescription, 0, len(indexes))
	for _, index := range indexes {
		projection := index.Projection
		if projection.ProjectionType == "" {
			projection.ProjectionType = "ALL"
		}
		descriptions = append(descriptions, localSecondaryIndexDescription{
			IndexArn:       "arn:aws:dynamodb:" + region + ":000000000000:table/" + tableName + "/index/" + index.IndexName,
			IndexName:      index.IndexName,
			IndexSizeBytes: 0,
			ItemCount:      0,
			KeySchema:      append([]keySchemaElement(nil), index.KeySchema...),
			Projection:     projection,
		})
	}
	return descriptions
}

func applyGlobalSecondaryIndexUpdates(description *tableDescription, region string, definitions []attributeDefinition, updates []globalSecondaryIndexUpdate) error {
	if err := validateAttributeDefinitionUpdates(description.AttributeDefinitions, definitions); err != nil {
		return err
	}
	attributes := attributeDefinitionSet(description.AttributeDefinitions, definitions)
	for _, update := range updates {
		actions := 0
		if update.Create != nil {
			actions++
		}
		if update.Delete != nil {
			actions++
		}
		if update.Update != nil {
			actions++
		}
		if actions != 1 {
			return errors.New("each global secondary index update must contain exactly one action")
		}
		if update.Update != nil {
			return errors.New("global secondary index throughput updates are not supported")
		}
		if update.Create != nil {
			if err := validateGlobalSecondaryIndexCreate(*update.Create, attributes, *description); err != nil {
				return err
			}
			description.AttributeDefinitions = mergeAttributeDefinitions(description.AttributeDefinitions, definitions)
			description.GlobalSecondaryIndexes = append(description.GlobalSecondaryIndexes, gsiDescriptions(region, description.TableName, []globalSecondaryIndexRequest{*update.Create})...)
			continue
		}
		if update.Delete != nil {
			index := indexOfGlobalSecondaryIndex(description.GlobalSecondaryIndexes, update.Delete.IndexName)
			if index < 0 {
				return errors.New("global secondary index does not exist")
			}
			description.GlobalSecondaryIndexes = append(description.GlobalSecondaryIndexes[:index], description.GlobalSecondaryIndexes[index+1:]...)
		}
	}
	return nil
}

func validateAttributeDefinitionUpdates(existing []attributeDefinition, updates []attributeDefinition) error {
	types := map[string]string{}
	for _, definition := range existing {
		types[definition.AttributeName] = definition.AttributeType
	}
	for _, definition := range updates {
		if definition.AttributeName == "" {
			return errors.New("attribute name is required")
		}
		switch definition.AttributeType {
		case "S", "N", "B":
		default:
			return errors.New("attribute type must be S, N, or B")
		}
		if existingType, ok := types[definition.AttributeName]; ok && existingType != definition.AttributeType {
			return errors.New("attribute definitions cannot change existing attribute type")
		}
		types[definition.AttributeName] = definition.AttributeType
	}
	return nil
}

func attributeDefinitionSet(existing []attributeDefinition, updates []attributeDefinition) map[string]bool {
	attributes := map[string]bool{}
	for _, definition := range existing {
		attributes[definition.AttributeName] = true
	}
	for _, definition := range updates {
		if definition.AttributeName != "" {
			attributes[definition.AttributeName] = true
		}
	}
	return attributes
}

func mergeAttributeDefinitions(existing []attributeDefinition, updates []attributeDefinition) []attributeDefinition {
	merged := append([]attributeDefinition(nil), existing...)
	seen := map[string]bool{}
	for _, definition := range existing {
		seen[definition.AttributeName] = true
	}
	for _, definition := range updates {
		if definition.AttributeName == "" || seen[definition.AttributeName] {
			continue
		}
		merged = append(merged, definition)
		seen[definition.AttributeName] = true
	}
	return merged
}

func validateGlobalSecondaryIndexCreate(index globalSecondaryIndexRequest, attributes map[string]bool, description tableDescription) error {
	if index.IndexName == "" {
		return errors.New("global secondary index name is required")
	}
	if tableHasIndex(description, index.IndexName) {
		return errors.New("secondary index name already exists")
	}
	if len(index.KeySchema) == 0 {
		return errors.New("global secondary index key schema is required")
	}
	hashKeys := 0
	rangeKeys := 0
	for _, element := range index.KeySchema {
		if !attributes[element.AttributeName] {
			return errors.New("global secondary index key schema attributes must be defined")
		}
		switch element.KeyType {
		case "HASH":
			hashKeys++
		case "RANGE":
			rangeKeys++
		default:
			return errors.New("global secondary index key type must be HASH or RANGE")
		}
	}
	if hashKeys != 1 || rangeKeys > 1 || len(index.KeySchema) > 2 {
		return errors.New("global secondary index key schema must include one HASH key and at most one RANGE key")
	}
	if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
		return errors.New("global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
	}
	return nil
}

func indexOfGlobalSecondaryIndex(indexes []globalSecondaryIndexDescription, indexName string) int {
	for i, index := range indexes {
		if index.IndexName == indexName {
			return i
		}
	}
	return -1
}

func updateIndexItemCounts(state *tableState) {
	for i := range state.description.GlobalSecondaryIndexes {
		count := 0
		for _, candidate := range state.items {
			if itemHasAllKeys(candidate, state.description.GlobalSecondaryIndexes[i].KeySchema) {
				count++
			}
		}
		state.description.GlobalSecondaryIndexes[i].ItemCount = count
	}
	for i := range state.description.LocalSecondaryIndexes {
		count := 0
		for _, candidate := range state.items {
			if itemHasAllKeys(candidate, state.description.LocalSecondaryIndexes[i].KeySchema) {
				count++
			}
		}
		state.description.LocalSecondaryIndexes[i].ItemCount = count
	}
}

func itemHasAllKeys(value item, schema []keySchemaElement) bool {
	for _, element := range schema {
		if _, ok := value[element.AttributeName]; !ok {
			return false
		}
	}
	return true
}

func billingMode(value string) string {
	if value == "" {
		return "PAY_PER_REQUEST"
	}
	return value
}

func validateStreamSpecification(specification streamSpecification) error {
	if !specification.StreamEnabled {
		return nil
	}
	switch specification.StreamViewType {
	case "KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES":
		return nil
	case "":
		return errors.New("stream view type is required when stream is enabled")
	default:
		return errors.New("stream view type must be KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, or NEW_AND_OLD_IMAGES")
	}
}

func enableStreamDescription(description *tableDescription, region string, specification streamSpecification) {
	label := description.LatestStreamLabel
	if label == "" {
		label = time.Now().UTC().Format("2006-01-02T15:04:05.000")
	}
	description.LatestStreamLabel = label
	description.LatestStreamArn = description.TableArn + "/stream/" + label
	if description.LatestStreamArn == "/stream/"+label {
		description.LatestStreamArn = "arn:aws:dynamodb:" + region + ":000000000000:table/" + description.TableName + "/stream/" + label
	}
	specification.StreamEnabled = true
	description.StreamSpecification = &streamSpecification{
		StreamEnabled:  true,
		StreamViewType: specification.StreamViewType,
	}
}

func streamDescriptionForTable(description tableDescription) streamDescription {
	streamViewType := ""
	if description.StreamSpecification != nil {
		streamViewType = description.StreamSpecification.StreamViewType
	}
	return streamDescription{
		CreationRequestDateTime: description.CreationDateTime,
		KeySchema:               append([]keySchemaElement(nil), description.KeySchema...),
		Shards: []shardDescription{{
			ShardID: "shardId-000000000000",
			SequenceNumberRange: sequenceNumberRange{
				StartingSequenceNumber: "0",
			},
		}},
		StreamArn:      description.LatestStreamArn,
		StreamLabel:    description.LatestStreamLabel,
		StreamStatus:   "ENABLED",
		StreamViewType: streamViewType,
		TableName:      description.TableName,
	}
}

func (s *Server) streamShardExists(streamArn string, shardID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.tables {
		description := state.description
		if description.LatestStreamArn != streamArn || description.StreamSpecification == nil || !description.StreamSpecification.StreamEnabled {
			continue
		}
		for _, shard := range streamDescriptionForTable(description).Shards {
			if shard.ShardID == shardID {
				return true
			}
		}
	}
	return false
}

func (s *Server) streamRecordCount(streamArn string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok {
		return 0
	}
	return len(state.streamRecords)
}

func (s *Server) streamRecords(streamArn string, position int, limit int) []streamRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok || position >= len(state.streamRecords) {
		return []streamRecord{}
	}
	if position < 0 {
		position = 0
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	end := position + limit
	if end > len(state.streamRecords) {
		end = len(state.streamRecords)
	}
	return cloneStreamRecords(state.streamRecords[position:end])
}

func (s *Server) streamPositionForSequence(streamArn string, sequenceNumber string, after bool) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tableStateForStreamARNLocked(streamArn)
	if !ok {
		return 0, false
	}
	for i, record := range state.streamRecords {
		if record.DynamoDB.SequenceNumber == sequenceNumber {
			if after {
				return i + 1, true
			}
			return i, true
		}
	}
	return 0, false
}

func (s *Server) tableStateForStreamARNLocked(streamArn string) (*tableState, bool) {
	for _, state := range s.tables {
		description := state.description
		if description.LatestStreamArn == streamArn && description.StreamSpecification != nil && description.StreamSpecification.StreamEnabled {
			return state, true
		}
	}
	return nil, false
}

func (s *Server) appendStreamRecordLocked(state *tableState, eventName string, oldItem item, newItem item, oldExisted bool) {
	if state.description.StreamSpecification == nil || !state.description.StreamSpecification.StreamEnabled {
		return
	}
	if eventName == "REMOVE" && !oldExisted {
		return
	}
	source := newItem
	if eventName == "REMOVE" {
		source = oldItem
	}
	keys, err := extractKey(state.description, source)
	if err != nil {
		return
	}
	sequence := fmt.Sprintf("%d", len(state.streamRecords)+1)
	image := streamRecordImage{
		ApproximateCreationDateTime: time.Now().Unix(),
		Keys:                        cloneItem(keys),
		SequenceNumber:              sequence,
		StreamViewType:              state.description.StreamSpecification.StreamViewType,
	}
	switch image.StreamViewType {
	case "NEW_IMAGE":
		if eventName != "REMOVE" {
			image.NewImage = cloneItem(newItem)
		}
	case "OLD_IMAGE":
		if oldExisted {
			image.OldImage = cloneItem(oldItem)
		}
	case "NEW_AND_OLD_IMAGES":
		if eventName != "REMOVE" {
			image.NewImage = cloneItem(newItem)
		}
		if oldExisted {
			image.OldImage = cloneItem(oldItem)
		}
	}
	if encoded, err := json.Marshal(image); err == nil {
		image.SizeBytes = len(encoded)
	}
	state.streamRecords = append(state.streamRecords, streamRecord{
		EventID:      state.description.TableName + ":" + sequence,
		EventName:    eventName,
		EventSource:  "aws:dynamodb",
		EventVersion: "1.1",
		AWSRegion:    defaultString(s.config.Region, "us-east-1"),
		DynamoDB:     image,
	})
}

func streamEventName(existed bool, delete bool) string {
	if delete {
		return "REMOVE"
	}
	if existed {
		return "MODIFY"
	}
	return "INSERT"
}

func encodeStreamIterator(iterator streamIterator) (string, error) {
	payload, err := json.Marshal(iterator)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeStreamIterator(value string) (streamIterator, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return streamIterator{}, err
	}
	var iterator streamIterator
	if err := json.Unmarshal(payload, &iterator); err != nil {
		return streamIterator{}, err
	}
	if iterator.StreamArn == "" || iterator.ShardID == "" {
		return streamIterator{}, errors.New("invalid stream iterator")
	}
	return iterator, nil
}

func cloneStreamSpecification(value *streamSpecification) *streamSpecification {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func ttlDescription(description tableDescription) timeToLiveDescription {
	if description.TimeToLiveDescription == nil {
		return timeToLiveDescription{TimeToLiveStatus: "DISABLED"}
	}
	return *cloneTTLDescription(description.TimeToLiveDescription)
}

func ttlDescriptionFromSpecification(specification timeToLiveSpecification) *timeToLiveDescription {
	status := "DISABLED"
	if specification.Enabled {
		status = "ENABLED"
	}
	return &timeToLiveDescription{
		AttributeName:    specification.AttributeName,
		TimeToLiveStatus: status,
	}
}

func cloneTTLDescription(value *timeToLiveDescription) *timeToLiveDescription {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func (s *Server) expireTTLItems(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	backups, changed := s.expireTTLItemsLocked(now)
	if !changed {
		return nil
	}
	if err := s.persistLocked(); err != nil {
		restoreBackups(backups)
		return err
	}
	return nil
}

func (s *Server) expireTTLItemsLocked(now time.Time) (map[*tableState]map[string]itemBackup, bool) {
	backups := map[*tableState]map[string]itemBackup{}
	changed := false
	for _, state := range s.tables {
		tableChanged := false
		ttl := ttlDescription(state.description)
		if ttl.TimeToLiveStatus != "ENABLED" || ttl.AttributeName == "" {
			continue
		}
		for key, candidate := range state.items {
			if !ttlItemExpired(candidate, ttl.AttributeName, now) {
				continue
			}
			rememberItemBackup(backups, state, key)
			delete(state.items, key)
			tableChanged = true
			changed = true
		}
		if tableChanged {
			state.description.ItemCount = len(state.items)
			updateIndexItemCounts(state)
		}
	}
	return backups, changed
}

func ttlItemExpired(value item, attributeName string, now time.Time) bool {
	attr, ok := value[attributeName]
	if !ok {
		return false
	}
	seconds, ok := attr["N"].(string)
	if !ok {
		return false
	}
	expiry, ok := new(big.Rat).SetString(seconds)
	if !ok {
		return false
	}
	return expiry.Cmp(big.NewRat(now.Unix(), 1)) <= 0
}

func itemKey(description tableDescription, values item) (string, error) {
	keyValues := make([]attributeValue, 0, len(description.KeySchema))
	for _, element := range description.KeySchema {
		value, ok := values[element.AttributeName]
		if !ok {
			return "", fmt.Errorf("missing key attribute %s", element.AttributeName)
		}
		if err := validateAttributeValue(value, element.AttributeName); err != nil {
			return "", err
		}
		keyValues = append(keyValues, value)
	}
	encoded, err := json.Marshal(keyValues)
	if err != nil {
		return "", fmt.Errorf("encode key: %w", err)
	}
	return string(encoded), nil
}

type keyedItem struct {
	key   string
	value item
}

func sortedItems(state *tableState) []keyedItem {
	items := make([]keyedItem, 0, len(state.items))
	for key, value := range state.items {
		items = append(items, keyedItem{key: key, value: cloneItem(value)})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})
	return items
}

func sortedItemsForQuery(state *tableState, indexName string) []keyedItem {
	items := make([]keyedItem, 0, len(state.items))
	for key, value := range state.items {
		items = append(items, keyedItem{key: key, value: cloneItem(value)})
	}
	schema := queryKeySchema(state.description, indexName)
	sort.Slice(items, func(i, j int) bool {
		if comparison := compareItemsBySchema(items[i].value, items[j].value, schema); comparison != 0 {
			return comparison < 0
		}
		return items[i].key < items[j].key
	})
	return items
}

func sortedItemsForScan(state *tableState, indexName string) []keyedItem {
	items := sortedItemsForQuery(state, indexName)
	if indexName == "" {
		return items
	}
	schema := queryKeySchema(state.description, indexName)
	filtered := items[:0]
	for _, candidate := range items {
		if itemHasAllKeys(candidate.value, schema) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func queryKeySchema(description tableDescription, indexName string) []keySchemaElement {
	if indexName == "" {
		return description.KeySchema
	}
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.KeySchema
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.KeySchema
		}
	}
	return description.KeySchema
}

func compareItemsBySchema(left item, right item, schema []keySchemaElement) int {
	for _, element := range schema {
		comparison := compareAttributeValues(left[element.AttributeName], right[element.AttributeName])
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareAttributeValues(left attributeValue, right attributeValue) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if leftNumber, ok := left["N"].(string); ok {
		rightNumber, ok := right["N"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		leftRat, leftOK := new(big.Rat).SetString(leftNumber)
		rightRat, rightOK := new(big.Rat).SetString(rightNumber)
		if leftOK && rightOK {
			return leftRat.Cmp(rightRat)
		}
		return strings.Compare(leftNumber, rightNumber)
	}
	if leftString, ok := left["S"].(string); ok {
		rightString, ok := right["S"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		return strings.Compare(leftString, rightString)
	}
	if leftBinary, ok := left["B"].(string); ok {
		rightBinary, ok := right["B"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		return strings.Compare(leftBinary, rightBinary)
	}
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return strings.Compare(string(leftJSON), string(rightJSON))
}

func attributeTypeName(value attributeValue) string {
	for _, name := range []string{"S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"} {
		if _, ok := value[name]; ok {
			return name
		}
	}
	return ""
}

func reverseItems(items []keyedItem) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}

func startKeyString(description tableDescription, start item) (string, error) {
	if len(start) == 0 {
		return "", nil
	}
	return itemKey(description, start)
}

func collectItems(description tableDescription, indexName string, source []keyedItem, limit int, startKey string, projection string, names map[string]string, limitCountsUnmatched bool, match func(item) (bool, error)) (map[string]any, error) {
	responseItems := []item{}
	scanned := 0
	started := startKey == ""
	for _, candidate := range source {
		if !started {
			started = candidate.key == startKey
			continue
		}
		matched, err := match(candidate.value)
		if err != nil {
			return nil, err
		}
		if matched || limitCountsUnmatched {
			scanned++
		}
		if !matched {
			if limitCountsUnmatched && limit > 0 && scanned == limit {
				return limitedItemsResponse(description, indexName, candidate, responseItems, scanned, hasMoreItems(source, candidate.key)), nil
			}
			continue
		}
		responseItems = append(responseItems, projectResultItem(description, indexName, candidate.value, projection, names))
		if limit > 0 && scanned == limit {
			hasMore := hasMoreItems(source, candidate.key)
			if !limitCountsUnmatched {
				hasMore = hasMoreMatches(source, candidate.key, match)
			}
			return limitedItemsResponse(description, indexName, candidate, responseItems, scanned, hasMore), nil
		}
	}
	return map[string]any{
		"Items":        responseItems,
		"Count":        len(responseItems),
		"ScannedCount": scanned,
	}, nil
}

func limitedItemsResponse(description tableDescription, indexName string, candidate keyedItem, responseItems []item, scanned int, hasMore bool) map[string]any {
	response := map[string]any{
		"Items":        responseItems,
		"Count":        len(responseItems),
		"ScannedCount": scanned,
	}
	if lastKey, err := extractKey(description, candidate.value); err == nil && hasMore {
		response["LastEvaluatedKey"] = lastKey
	}
	return response
}

func validateSelect(selectValue string, projectionExpression string) error {
	selectValue = strings.ToUpper(strings.TrimSpace(selectValue))
	switch selectValue {
	case "", "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES":
		return nil
	case "COUNT":
		if strings.TrimSpace(projectionExpression) != "" {
			return errors.New("select COUNT cannot be used with ProjectionExpression")
		}
		return nil
	default:
		return fmt.Errorf("unsupported select value %s", selectValue)
	}
}

func applySelect(response map[string]any, selectValue string) {
	if strings.EqualFold(strings.TrimSpace(selectValue), "COUNT") {
		delete(response, "Items")
	}
}

func hasMoreItems(source []keyedItem, afterKey string) bool {
	found := false
	for _, candidate := range source {
		if !found {
			found = candidate.key == afterKey
			continue
		}
		return true
	}
	return false
}

func hasMoreMatches(source []keyedItem, afterKey string, match func(item) (bool, error)) bool {
	found := false
	for _, candidate := range source {
		if !found {
			found = candidate.key == afterKey
			continue
		}
		matched, err := match(candidate.value)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func extractKey(description tableDescription, value item) (item, error) {
	key := item{}
	for _, element := range description.KeySchema {
		attr, ok := value[element.AttributeName]
		if !ok {
			return nil, fmt.Errorf("missing key attribute %s", element.AttributeName)
		}
		key[element.AttributeName] = cloneAttributeValue(attr)
	}
	return key, nil
}

func projectItem(value item, expression string, names map[string]string) item {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return cloneItem(value)
	}
	projected := item{}
	for _, token := range strings.Split(expression, ",") {
		name := resolveAttributeName(strings.TrimSpace(token), names)
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func projectResultItem(description tableDescription, indexName string, value item, expression string, names map[string]string) item {
	projected := projectIndexItem(description, indexName, value)
	return projectItem(projected, expression, names)
}

func projectIndexItem(description tableDescription, indexName string, value item) item {
	projection, schema, ok := indexProjectionForName(description, indexName)
	if !ok || projection.ProjectionType == "" || projection.ProjectionType == "ALL" {
		return cloneItem(value)
	}
	allowed := map[string]bool{}
	for _, element := range description.KeySchema {
		allowed[element.AttributeName] = true
	}
	for _, element := range schema {
		allowed[element.AttributeName] = true
	}
	if projection.ProjectionType == "INCLUDE" {
		for _, name := range projection.NonKeyAttributes {
			allowed[name] = true
		}
	}
	projected := item{}
	for name := range allowed {
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func indexProjectionForName(description tableDescription, indexName string) (indexProjection, []keySchemaElement, bool) {
	if indexName == "" {
		return indexProjection{}, nil, false
	}
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.Projection, index.KeySchema, true
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.Projection, index.KeySchema, true
		}
	}
	return indexProjection{}, nil, false
}

type partiQLSelectStatement struct {
	tableName   string
	projections []string
	conditions  []partiQLCondition
}

type partiQLCondition struct {
	attribute string
	value     attributeValue
}

func parsePartiQLSelect(statement string, parameters []attributeValue) (partiQLSelectStatement, error) {
	statement = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	if statement == "" {
		return partiQLSelectStatement{}, errors.New("statement is required")
	}
	upper := strings.ToUpper(statement)
	if !strings.HasPrefix(upper, "SELECT ") {
		return partiQLSelectStatement{}, errors.New("only SELECT statements are supported")
	}
	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex < 0 {
		return partiQLSelectStatement{}, errors.New("SELECT statement must include FROM")
	}
	projectionPart := strings.TrimSpace(statement[len("SELECT "):fromIndex])
	afterFrom := strings.TrimSpace(statement[fromIndex+len(" FROM "):])
	whereIndex := strings.Index(strings.ToUpper(afterFrom), " WHERE ")
	tableName := afterFrom
	wherePart := ""
	if whereIndex >= 0 {
		tableName = strings.TrimSpace(afterFrom[:whereIndex])
		wherePart = strings.TrimSpace(afterFrom[whereIndex+len(" WHERE "):])
	}
	tableName = trimPartiQLIdentifier(tableName)
	if tableName == "" {
		return partiQLSelectStatement{}, errors.New("table name is required")
	}
	projections, err := parsePartiQLProjections(projectionPart)
	if err != nil {
		return partiQLSelectStatement{}, err
	}
	conditions, err := parsePartiQLWhere(wherePart, parameters)
	if err != nil {
		return partiQLSelectStatement{}, err
	}
	return partiQLSelectStatement{
		tableName:   tableName,
		projections: projections,
		conditions:  conditions,
	}, nil
}

func parsePartiQLProjections(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("SELECT projection is required")
	}
	if value == "*" {
		return nil, nil
	}
	projections := []string{}
	for _, token := range strings.Split(value, ",") {
		name := trimPartiQLIdentifier(strings.TrimSpace(token))
		if name == "" {
			return nil, errors.New("invalid SELECT projection")
		}
		projections = append(projections, name)
	}
	return projections, nil
}

func parsePartiQLWhere(value string, parameters []attributeValue) ([]partiQLCondition, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if len(parameters) > 0 {
			return nil, errors.New("too many PartiQL parameters")
		}
		return nil, nil
	}
	parts := splitPartiQLAnd(value)
	conditions := make([]partiQLCondition, 0, len(parts))
	paramIndex := 0
	for _, part := range parts {
		left, right, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errors.New("WHERE supports equality predicates only")
		}
		attribute := trimPartiQLIdentifier(strings.TrimSpace(left))
		if attribute == "" {
			return nil, errors.New("invalid WHERE attribute")
		}
		right = strings.TrimSpace(right)
		if right != "?" {
			return nil, errors.New("WHERE predicates must use positional parameters")
		}
		if paramIndex >= len(parameters) {
			return nil, errors.New("missing PartiQL parameter")
		}
		conditions = append(conditions, partiQLCondition{
			attribute: attribute,
			value:     cloneAttributeValue(parameters[paramIndex]),
		})
		paramIndex++
	}
	if paramIndex != len(parameters) {
		return nil, errors.New("too many PartiQL parameters")
	}
	return conditions, nil
}

func splitPartiQLAnd(value string) []string {
	fields := strings.Fields(value)
	parts := []string{}
	current := []string{}
	for _, field := range fields {
		if strings.EqualFold(field, "AND") {
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, field)
	}
	if len(current) > 0 {
		parts = append(parts, strings.Join(current, " "))
	}
	return parts
}

func trimPartiQLIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		first := value[0]
		last := value[len(value)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') || (first == '`' && last == '`') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func partiQLConditionsMatch(value item, conditions []partiQLCondition) bool {
	for _, condition := range conditions {
		actual, ok := value[condition.attribute]
		if !ok || !reflect.DeepEqual(actual, condition.value) {
			return false
		}
	}
	return true
}

func projectPartiQLItem(value item, projections []string) item {
	if len(projections) == 0 {
		return cloneItem(value)
	}
	projected := item{}
	for _, name := range projections {
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func partiQLConditionsCoverKey(description tableDescription, conditions []partiQLCondition) bool {
	conditionAttributes := map[string]bool{}
	for _, condition := range conditions {
		conditionAttributes[condition.attribute] = true
	}
	for _, element := range description.KeySchema {
		if !conditionAttributes[element.AttributeName] {
			return false
		}
	}
	return true
}

func matchKeyCondition(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	parts, err := splitConjunctivePredicates(expression)
	if err != nil {
		return false, err
	}
	for _, part := range parts {
		matched, err := matchPredicate(strings.TrimSpace(part), names, values, candidate)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func matchFilter(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return true, nil
	}
	return matchConjunctiveExpression(expression, names, values, candidate)
}

func matchConjunctiveExpression(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	disjuncts, err := splitDisjunctivePredicates(expression)
	if err != nil {
		return false, err
	}
	for _, disjunct := range disjuncts {
		parts, err := splitConjunctivePredicates(disjunct)
		if err != nil {
			return false, err
		}
		matchedAll := true
		for _, part := range parts {
			matched, err := matchPredicate(strings.TrimSpace(part), names, values, candidate)
			if err != nil {
				return false, err
			}
			if !matched {
				matchedAll = false
				break
			}
		}
		if matchedAll {
			return true, nil
		}
	}
	return false, nil
}

func matchPredicate(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	if strings.HasPrefix(strings.ToUpper(expression), "NOT ") {
		matched, err := matchPredicate(strings.TrimSpace(expression[len("NOT "):]), names, values, candidate)
		if err != nil {
			return false, err
		}
		return !matched, nil
	}
	if strings.HasPrefix(expression, "attribute_exists(") && strings.HasSuffix(expression, ")") {
		attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_exists("), ")")), names)
		_, ok := candidate[attr]
		return ok, nil
	}
	if strings.HasPrefix(expression, "attribute_not_exists(") && strings.HasSuffix(expression, ")") {
		attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_not_exists("), ")")), names)
		_, ok := candidate[attr]
		return !ok, nil
	}
	if strings.HasPrefix(expression, "begins_with(") && strings.HasSuffix(expression, ")") {
		args := strings.Split(strings.TrimSuffix(strings.TrimPrefix(expression, "begins_with("), ")"), ",")
		if len(args) != 2 {
			return false, errors.New("invalid begins_with expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeBeginsWith(candidate[attr], expected), nil
	}
	if strings.HasPrefix(expression, "contains(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "contains("), ")"))
		if len(args) != 2 {
			return false, errors.New("invalid contains expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeContains(candidate[attr], expected), nil
	}
	if strings.HasPrefix(expression, "attribute_type(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_type("), ")"))
		if len(args) != 2 {
			return false, errors.New("invalid attribute_type expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeHasType(candidate[attr], expected), nil
	}
	if attrToken, lowerToken, upperToken, ok := splitBetweenExpression(expression); ok {
		attr := resolveAttributeName(strings.TrimSpace(attrToken), names)
		lower, ok := values[strings.TrimSpace(lowerToken)]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(lowerToken))
		}
		upper, ok := values[strings.TrimSpace(upperToken)]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(upperToken))
		}
		actual, ok := candidate[attr]
		if !ok {
			return false, nil
		}
		return compareAttributeValues(actual, lower) >= 0 && compareAttributeValues(actual, upper) <= 0, nil
	}
	if attrToken, valueTokens, ok := splitInExpression(expression); ok {
		attr := resolveAttributeName(strings.TrimSpace(attrToken), names)
		actual, ok := candidate[attr]
		if !ok {
			return false, nil
		}
		if len(valueTokens) == 0 {
			return false, errors.New("IN expression requires at least one value")
		}
		for _, valueToken := range valueTokens {
			valueToken = strings.TrimSpace(valueToken)
			expected, ok := values[valueToken]
			if !ok {
				return false, fmt.Errorf("missing expression attribute value %s", valueToken)
			}
			if attributeValuesEqual(actual, expected) {
				return true, nil
			}
		}
		return false, nil
	}
	nameToken, operator, valueToken, ok := splitComparisonExpression(expression)
	if !ok {
		return false, errors.New("unsupported expression predicate")
	}
	if actualSize, ok, err := evaluateSizeOperand(strings.TrimSpace(nameToken), names, candidate); err != nil {
		return false, err
	} else if ok {
		valueToken = strings.TrimSpace(valueToken)
		expected, ok := values[valueToken]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", valueToken)
		}
		comparison := compareAttributeValues(attributeValue{"N": fmt.Sprintf("%d", actualSize)}, expected)
		switch operator {
		case "=":
			return comparison == 0, nil
		case "<>":
			return comparison != 0, nil
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		case ">=":
			return comparison >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator %s", operator)
		}
	}
	attr := resolveAttributeName(strings.TrimSpace(nameToken), names)
	valueToken = strings.TrimSpace(valueToken)
	expected, ok := values[valueToken]
	if !ok {
		return false, fmt.Errorf("missing expression attribute value %s", valueToken)
	}
	actual, ok := candidate[attr]
	if !ok {
		return false, nil
	}
	comparison := compareAttributeValues(actual, expected)
	switch operator {
	case "=":
		return reflect.DeepEqual(actual, expected), nil
	case "<>":
		return !reflect.DeepEqual(actual, expected), nil
	case "<":
		return comparison < 0, nil
	case "<=":
		return comparison <= 0, nil
	case ">":
		return comparison > 0, nil
	case ">=":
		return comparison >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator %s", operator)
	}
}

func splitDisjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	for _, field := range fields {
		if strings.ToUpper(field) == "OR" {
			if len(current) == 0 {
				return nil, errors.New("invalid OR expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid OR expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitConjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	betweenNeedsAnd := false
	for _, field := range fields {
		upper := strings.ToUpper(field)
		if upper == "BETWEEN" {
			betweenNeedsAnd = true
			current = append(current, field)
			continue
		}
		if upper == "AND" && !betweenNeedsAnd {
			if len(current) == 0 {
				return nil, errors.New("invalid AND expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		if upper == "AND" && betweenNeedsAnd {
			betweenNeedsAnd = false
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid AND expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitBetweenExpression(expression string) (attr string, lower string, upper string, ok bool) {
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.ToUpper(fields[1]) != "BETWEEN" || strings.ToUpper(fields[3]) != "AND" {
		return "", "", "", false
	}
	return fields[0], fields[2], fields[4], true
}

func splitInExpression(expression string) (attr string, values []string, ok bool) {
	left, right, found := strings.Cut(expression, " IN ")
	if !found {
		left, right, found = strings.Cut(expression, " in ")
	}
	if !found {
		return "", nil, false
	}
	right = strings.TrimSpace(right)
	if !strings.HasPrefix(right, "(") || !strings.HasSuffix(right, ")") {
		return "", nil, false
	}
	return strings.TrimSpace(left), splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(right, "("), ")")), true
}

func splitComparisonExpression(expression string) (left string, operator string, right string, ok bool) {
	for _, op := range []string{"<=", ">=", "<>", "=", "<", ">"} {
		if left, right, ok := strings.Cut(expression, op); ok {
			return left, op, right, true
		}
	}
	return "", "", "", false
}

func attributeHasType(actual attributeValue, expected attributeValue) bool {
	expectedType, ok := expected["S"].(string)
	if !ok {
		return false
	}
	return attributeTypeName(actual) == expectedType
}

func evaluateSizeOperand(expression string, names map[string]string, candidate item) (int, bool, error) {
	if !strings.HasPrefix(expression, "size(") || !strings.HasSuffix(expression, ")") {
		return 0, false, nil
	}
	attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "size("), ")")), names)
	value, ok := candidate[attr]
	if !ok {
		return 0, true, nil
	}
	size, ok := attributeSize(value)
	if !ok {
		return 0, true, fmt.Errorf("size is not supported for attribute %s", attr)
	}
	return size, true, nil
}

func attributeSize(value attributeValue) (int, bool) {
	if text, ok := value["S"].(string); ok {
		return len(text), true
	}
	if binary, ok := value["B"].(string); ok {
		return len(binary), true
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		if values, ok := stringSliceAttribute(value, setType); ok {
			return len(values), true
		}
	}
	if entries := attributeValueList(value["L"]); entries != nil {
		return len(entries), true
	}
	if rawMap, ok := value["M"]; ok {
		switch values := rawMap.(type) {
		case map[string]attributeValue:
			return len(values), true
		case map[string]any:
			return len(values), true
		}
	}
	return 0, false
}

func attributeBeginsWith(actual attributeValue, expected attributeValue) bool {
	actualString, ok := actual["S"].(string)
	if !ok {
		return false
	}
	expectedString, ok := expected["S"].(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(actualString, expectedString)
}

func attributeContains(actual attributeValue, expected attributeValue) bool {
	actualString, ok := actual["S"].(string)
	if ok {
		expectedString, ok := expected["S"].(string)
		return ok && strings.Contains(actualString, expectedString)
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		actualValues, ok := stringSliceAttribute(actual, setType)
		if !ok {
			continue
		}
		expectedValues, ok := stringSliceAttribute(expected, setType)
		if ok && len(expectedValues) == 1 {
			return stringSliceContains(actualValues, expectedValues[0])
		}
		if scalar, ok := expected[setElementScalarType(setType)].(string); ok {
			return stringSliceContains(actualValues, scalar)
		}
		return false
	}
	if rawList, ok := actual["L"]; ok {
		for _, entry := range attributeValueList(rawList) {
			if attributeValuesEqual(entry, expected) {
				return true
			}
		}
	}
	return false
}

func setElementScalarType(setType string) string {
	switch setType {
	case "SS":
		return "S"
	case "NS":
		return "N"
	case "BS":
		return "B"
	default:
		return ""
	}
}

func attributeValueList(raw any) []attributeValue {
	switch values := raw.(type) {
	case []attributeValue:
		return append([]attributeValue(nil), values...)
	case []any:
		result := make([]attributeValue, 0, len(values))
		for _, entry := range values {
			if value, ok := entry.(map[string]any); ok {
				result = append(result, attributeValue(value))
			}
		}
		return result
	default:
		return nil
	}
}

func attributeValuesEqual(left attributeValue, right attributeValue) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func checkCondition(expression string, names map[string]string, values map[string]attributeValue, existing item, existed bool) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	candidate := existing
	if !existed {
		candidate = item{}
	}
	matched, err := matchConjunctiveExpression(expression, names, values, candidate)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("condition check failed")
	}
	return nil
}

func applyUpdateExpression(target item, expression string, names map[string]string, values map[string]attributeValue) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return errors.New("update expression is required")
	}
	clauses, err := splitUpdateClauses(expression)
	if err != nil {
		return err
	}
	for _, clause := range clauses {
		switch clause.keyword {
		case "SET":
			assignments := splitCommaSeparated(clause.body)
			for _, assignment := range assignments {
				nameToken, valueToken, ok := strings.Cut(strings.TrimSpace(assignment), "=")
				if !ok {
					return errors.New("invalid SET assignment")
				}
				attr := resolveAttributeName(strings.TrimSpace(nameToken), names)
				value, err := evaluateUpdateValue(target, strings.TrimSpace(valueToken), names, values)
				if err != nil {
					return err
				}
				target[attr] = value
			}
		case "REMOVE":
			removals := splitCommaSeparated(clause.body)
			for _, removal := range removals {
				attr := resolveAttributeName(strings.TrimSpace(removal), names)
				if attr == "" {
					return errors.New("invalid REMOVE path")
				}
				delete(target, attr)
			}
		case "ADD":
			additions := splitCommaSeparated(clause.body)
			for _, addition := range additions {
				fields := strings.Fields(strings.TrimSpace(addition))
				if len(fields) != 2 {
					return errors.New("invalid ADD assignment")
				}
				attr := resolveAttributeName(fields[0], names)
				if attr == "" {
					return errors.New("invalid ADD path")
				}
				value, ok := values[fields[1]]
				if !ok {
					return fmt.Errorf("missing expression attribute value %s", fields[1])
				}
				updated, err := addAttributeValue(target[attr], value)
				if err != nil {
					return err
				}
				target[attr] = updated
			}
		case "DELETE":
			deletions := splitCommaSeparated(clause.body)
			for _, deletion := range deletions {
				fields := strings.Fields(strings.TrimSpace(deletion))
				if len(fields) != 2 {
					return errors.New("invalid DELETE assignment")
				}
				attr := resolveAttributeName(fields[0], names)
				if attr == "" {
					return errors.New("invalid DELETE path")
				}
				value, ok := values[fields[1]]
				if !ok {
					return fmt.Errorf("missing expression attribute value %s", fields[1])
				}
				updated, remove, err := deleteAttributeValue(target[attr], value)
				if err != nil {
					return err
				}
				if remove {
					delete(target, attr)
				} else if updated != nil {
					target[attr] = updated
				}
			}
		default:
			return fmt.Errorf("unsupported update expression clause %s", clause.keyword)
		}
	}
	return nil
}

func evaluateUpdateValue(target item, expression string, names map[string]string, values map[string]attributeValue) (attributeValue, error) {
	if left, operator, right, ok := splitArithmeticUpdateExpression(expression); ok {
		leftValue, err := evaluateUpdateValue(target, left, names, values)
		if err != nil {
			return nil, err
		}
		rightValue, err := evaluateUpdateValue(target, right, names, values)
		if err != nil {
			return nil, err
		}
		if operator == "-" {
			rightValue, err = negateNumberAttribute(rightValue)
			if err != nil {
				return nil, err
			}
		}
		return addAttributeValue(leftValue, rightValue)
	}
	if strings.HasPrefix(expression, "if_not_exists(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "if_not_exists("), ")"))
		if len(args) != 2 {
			return nil, errors.New("invalid if_not_exists expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		if current, ok := target[attr]; ok {
			return cloneAttributeValue(current), nil
		}
		return evaluateUpdateValue(target, strings.TrimSpace(args[1]), names, values)
	}
	if strings.HasPrefix(expression, "list_append(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "list_append("), ")"))
		if len(args) != 2 {
			return nil, errors.New("invalid list_append expression")
		}
		leftValue, err := evaluateUpdateValue(target, strings.TrimSpace(args[0]), names, values)
		if err != nil {
			return nil, err
		}
		rightValue, err := evaluateUpdateValue(target, strings.TrimSpace(args[1]), names, values)
		if err != nil {
			return nil, err
		}
		return appendListAttributeValues(leftValue, rightValue)
	}
	if value, ok := values[expression]; ok {
		return cloneAttributeValue(value), nil
	}
	attr := resolveAttributeName(expression, names)
	if current, ok := target[attr]; ok {
		return cloneAttributeValue(current), nil
	}
	return nil, fmt.Errorf("missing expression attribute value %s", expression)
}

func splitArithmeticUpdateExpression(expression string) (left string, operator string, right string, ok bool) {
	depth := 0
	for index, char := range expression {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '+', '-':
			if depth == 0 {
				return strings.TrimSpace(expression[:index]), string(char), strings.TrimSpace(expression[index+1:]), true
			}
		}
	}
	return "", "", "", false
}

func appendListAttributeValues(left attributeValue, right attributeValue) (attributeValue, error) {
	leftEntries := attributeValueList(left["L"])
	rightEntries := attributeValueList(right["L"])
	if leftEntries == nil || rightEntries == nil {
		return nil, errors.New("list_append requires list attributes")
	}
	combined := make([]any, 0, len(leftEntries)+len(rightEntries))
	for _, entry := range leftEntries {
		combined = append(combined, map[string]any(cloneAttributeValue(entry)))
	}
	for _, entry := range rightEntries {
		combined = append(combined, map[string]any(cloneAttributeValue(entry)))
	}
	return attributeValue{"L": combined}, nil
}

func splitCommaSeparated(value string) []string {
	parts := []string{}
	depth := 0
	start := 0
	for index, char := range value {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(value[start:index]))
				start = index + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

type updateClause struct {
	keyword string
	body    string
}

func splitUpdateClauses(expression string) ([]updateClause, error) {
	type clauseStart struct {
		keyword string
		index   int
	}
	upper := strings.ToUpper(expression)
	starts := []clauseStart{}
	for _, keyword := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		for offset := 0; offset < len(upper); {
			index := strings.Index(upper[offset:], keyword)
			if index < 0 {
				break
			}
			absolute := offset + index
			if isUpdateClauseBoundary(upper, absolute, len(keyword)) {
				starts = append(starts, clauseStart{keyword: keyword, index: absolute})
			}
			offset = absolute + len(keyword)
		}
	}
	if len(starts) == 0 {
		return nil, errors.New("update expression must include SET, REMOVE, ADD, or DELETE")
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i].index < starts[j].index
	})
	clauses := make([]updateClause, 0, len(starts))
	for i, start := range starts {
		next := len(expression)
		if i+1 < len(starts) {
			next = starts[i+1].index
		}
		body := strings.TrimSpace(expression[start.index+len(start.keyword) : next])
		if body == "" {
			return nil, fmt.Errorf("%s update expression clause is empty", start.keyword)
		}
		if start.keyword != "SET" && start.keyword != "REMOVE" && start.keyword != "ADD" && start.keyword != "DELETE" {
			return nil, fmt.Errorf("unsupported update expression clause %s", start.keyword)
		}
		clauses = append(clauses, updateClause{keyword: start.keyword, body: body})
	}
	return clauses, nil
}

func addAttributeValue(current attributeValue, increment attributeValue) (attributeValue, error) {
	if number, ok := increment["N"].(string); ok {
		if current == nil {
			return cloneAttributeValue(increment), nil
		}
		currentNumber, ok := current["N"].(string)
		if !ok {
			return nil, errors.New("ADD number requires existing number attribute")
		}
		sum, err := addNumberStrings(currentNumber, number)
		if err != nil {
			return nil, err
		}
		return attributeValue{"N": sum}, nil
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		valuesToAdd, ok := stringSliceAttribute(increment, setType)
		if !ok {
			continue
		}
		if current == nil {
			return cloneAttributeValue(increment), nil
		}
		currentValues, ok := stringSliceAttribute(current, setType)
		if !ok {
			return nil, fmt.Errorf("ADD %s requires existing %s attribute", setType, setType)
		}
		return attributeValue{setType: unionStrings(currentValues, valuesToAdd)}, nil
	}
	return nil, errors.New("ADD supports N, SS, NS, and BS values")
}

func negateNumberAttribute(value attributeValue) (attributeValue, error) {
	number, ok := value["N"].(string)
	if !ok {
		return nil, errors.New("subtraction requires number attributes")
	}
	parsed, ok := new(big.Rat).SetString(number)
	if !ok {
		return nil, fmt.Errorf("invalid number %q", number)
	}
	negated := new(big.Rat).Neg(parsed)
	if negated.IsInt() {
		return attributeValue{"N": negated.Num().String()}, nil
	}
	precision := decimalPlaces(number)
	if precision < 1 {
		precision = 1
	}
	return attributeValue{"N": strings.TrimRight(strings.TrimRight(negated.FloatString(precision), "0"), ".")}, nil
}

func deleteAttributeValue(current attributeValue, decrement attributeValue) (attributeValue, bool, error) {
	for _, setType := range []string{"SS", "NS", "BS"} {
		valuesToDelete, ok := stringSliceAttribute(decrement, setType)
		if !ok {
			continue
		}
		if len(valuesToDelete) == 0 {
			return nil, false, errors.New("DELETE set value must not be empty")
		}
		if current == nil {
			return nil, false, nil
		}
		currentValues, ok := stringSliceAttribute(current, setType)
		if !ok {
			return nil, false, fmt.Errorf("DELETE %s requires existing %s attribute", setType, setType)
		}
		remaining := subtractStrings(currentValues, valuesToDelete)
		if len(remaining) == 0 {
			return nil, true, nil
		}
		return attributeValue{setType: remaining}, false, nil
	}
	return nil, false, errors.New("DELETE supports SS, NS, and BS values")
}

func addNumberStrings(left string, right string) (string, error) {
	leftNumber, ok := new(big.Rat).SetString(left)
	if !ok {
		return "", fmt.Errorf("invalid number %q", left)
	}
	rightNumber, ok := new(big.Rat).SetString(right)
	if !ok {
		return "", fmt.Errorf("invalid number %q", right)
	}
	sum := new(big.Rat).Add(leftNumber, rightNumber)
	if sum.IsInt() {
		return sum.Num().String(), nil
	}
	precision := maxInt(decimalPlaces(left), decimalPlaces(right))
	if precision < 1 {
		precision = 1
	}
	return strings.TrimRight(strings.TrimRight(sum.FloatString(precision), "0"), "."), nil
}

func decimalPlaces(value string) int {
	if index := strings.IndexByte(value, '.'); index >= 0 {
		return len(value) - index - 1
	}
	return 0
}

func stringSliceAttribute(value attributeValue, key string) ([]string, bool) {
	raw, ok := value[key]
	if !ok {
		return nil, false
	}
	switch values := raw.(type) {
	case []string:
		return append([]string(nil), values...), true
	case []any:
		result := make([]string, 0, len(values))
		for _, entry := range values {
			text, ok := entry.(string)
			if !ok {
				return nil, false
			}
			result = append(result, text)
		}
		return result, true
	default:
		return nil, false
	}
}

func unionStrings(left []string, right []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(left)+len(right))
	for _, value := range append(append([]string(nil), left...), right...) {
		if seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func subtractStrings(left []string, right []string) []string {
	remove := map[string]bool{}
	for _, value := range right {
		remove[value] = true
	}
	result := make([]string, 0, len(left))
	for _, value := range left {
		if !remove[value] {
			result = append(result, value)
		}
	}
	return result
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func isUpdateClauseBoundary(expression string, index int, keywordLength int) bool {
	beforeOK := index == 0 || expression[index-1] == ' '
	after := index + keywordLength
	afterOK := after < len(expression) && expression[after] == ' '
	return beforeOK && afterOK
}

func resolveAttributeName(token string, names map[string]string) string {
	if strings.HasPrefix(token, "#") {
		if value, ok := names[token]; ok {
			return value
		}
	}
	return token
}

func cloneItem(value item) item {
	clone := make(item, len(value))
	for name, attr := range value {
		clone[name] = cloneAttributeValue(attr)
	}
	return clone
}

func cloneItems(values map[string]item) map[string]item {
	clone := make(map[string]item, len(values))
	for key, value := range values {
		clone[key] = cloneItem(value)
	}
	return clone
}

func cloneTableDescription(value tableDescription) tableDescription {
	clone := value
	clone.AttributeDefinitions = append([]attributeDefinition(nil), value.AttributeDefinitions...)
	clone.KeySchema = append([]keySchemaElement(nil), value.KeySchema...)
	clone.GlobalSecondaryIndexes = append([]globalSecondaryIndexDescription(nil), value.GlobalSecondaryIndexes...)
	clone.LocalSecondaryIndexes = append([]localSecondaryIndexDescription(nil), value.LocalSecondaryIndexes...)
	clone.StreamSpecification = cloneStreamSpecification(value.StreamSpecification)
	clone.TimeToLiveDescription = cloneTTLDescription(value.TimeToLiveDescription)
	return clone
}

func cloneStreamRecords(values []streamRecord) []streamRecord {
	clone := make([]streamRecord, len(values))
	for i, value := range values {
		clone[i] = value
		clone[i].DynamoDB.Keys = cloneItem(value.DynamoDB.Keys)
		clone[i].DynamoDB.NewImage = cloneItem(value.DynamoDB.NewImage)
		clone[i].DynamoDB.OldImage = cloneItem(value.DynamoDB.OldImage)
	}
	return clone
}

func cloneTags(value map[string]string) map[string]string {
	clone := make(map[string]string, len(value))
	for key, val := range value {
		clone[key] = val
	}
	return clone
}

func continuousBackupsDescriptionForState(state *tableState) continuousBackupsDescription {
	if state != nil && state.continuousBackups != nil {
		return *cloneContinuousBackupsDescription(state.continuousBackups)
	}
	return continuousBackupsDescription{
		ContinuousBackupsStatus: "ENABLED",
		PointInTimeRecoveryDescription: pointInTimeRecoveryDescription{
			PointInTimeRecoveryStatus: "DISABLED",
		},
	}
}

func backupDescriptionForTable(description tableDescription, backupName string, createdAt int64) backupDescription {
	backupARN := fmt.Sprintf("%s/backup/%d-%s", description.TableArn, createdAt, backupName)
	return backupDescription{
		BackupDetails: backupDetails{
			BackupArn:              backupARN,
			BackupCreationDateTime: createdAt,
			BackupName:             backupName,
			BackupSizeBytes:        description.TableSizeBytes,
			BackupStatus:           "AVAILABLE",
			BackupType:             "USER",
		},
		SourceTableDetails: sourceTableDetails{
			AttributeDefinitions:  append([]attributeDefinition(nil), description.AttributeDefinitions...),
			BillingMode:           billingModeFromDescription(description),
			ItemCount:             description.ItemCount,
			KeySchema:             append([]keySchemaElement(nil), description.KeySchema...),
			TableArn:              description.TableArn,
			TableCreationDateTime: description.CreationDateTime,
			TableID:               description.TableArn,
			TableName:             description.TableName,
			TableSizeBytes:        description.TableSizeBytes,
		},
	}
}

func (s *Server) restoredTableDescription(targetTableName string, backup backupDescription, createdAt int64) tableDescription {
	region := defaultString(s.config.Region, "us-east-1")
	description, ok := s.backupTables[backup.BackupDetails.BackupArn]
	if !ok {
		description = tableDescription{
			AttributeDefinitions: append([]attributeDefinition(nil), backup.SourceTableDetails.AttributeDefinitions...),
			BillingModeSummary:   &billingModeSummary{BillingMode: defaultString(backup.SourceTableDetails.BillingMode, "PAY_PER_REQUEST")},
			KeySchema:            append([]keySchemaElement(nil), backup.SourceTableDetails.KeySchema...),
		}
	} else {
		description = cloneTableDescription(description)
	}
	description.CreationDateTime = createdAt
	description.ItemCount = len(s.backupItems[backup.BackupDetails.BackupArn])
	description.LatestStreamArn = ""
	description.LatestStreamLabel = ""
	description.StreamSpecification = nil
	description.TableArn = "arn:aws:dynamodb:" + region + ":000000000000:table/" + targetTableName
	description.TableName = targetTableName
	description.TableSizeBytes = backup.BackupDetails.BackupSizeBytes
	description.TableStatus = "ACTIVE"
	for i := range description.GlobalSecondaryIndexes {
		description.GlobalSecondaryIndexes[i].IndexArn = description.TableArn + "/index/" + description.GlobalSecondaryIndexes[i].IndexName
	}
	for i := range description.LocalSecondaryIndexes {
		description.LocalSecondaryIndexes[i].IndexArn = description.TableArn + "/index/" + description.LocalSecondaryIndexes[i].IndexName
	}
	return description
}

func backupSummaryForDescription(description backupDescription) backupSummary {
	return backupSummary{
		BackupArn:              description.BackupDetails.BackupArn,
		BackupCreationDateTime: description.BackupDetails.BackupCreationDateTime,
		BackupName:             description.BackupDetails.BackupName,
		BackupSizeBytes:        description.BackupDetails.BackupSizeBytes,
		BackupStatus:           description.BackupDetails.BackupStatus,
		BackupType:             description.BackupDetails.BackupType,
		TableArn:               description.SourceTableDetails.TableArn,
		TableName:              description.SourceTableDetails.TableName,
	}
}

func billingModeFromDescription(description tableDescription) string {
	if description.BillingModeSummary == nil {
		return "PAY_PER_REQUEST"
	}
	return description.BillingModeSummary.BillingMode
}

func cloneContinuousBackupsDescription(value *continuousBackupsDescription) *continuousBackupsDescription {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func resourcePolicyRevision(policy string) string {
	sum := sha256.Sum256([]byte(policy))
	return fmt.Sprintf("%x", sum[:])
}

func cloneAttributeValue(value attributeValue) attributeValue {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var clone attributeValue
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return value
	}
	return clone
}

func dashboardItemPayload(value item) map[string]any {
	payload := make(map[string]any, len(value))
	for name, attr := range value {
		payload[name] = cloneAttributeValue(attr)
	}
	return payload
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func addConsumedCapacity(response map[string]any, tableName string, mode string) {
	if mode == "" || strings.EqualFold(mode, "NONE") {
		return
	}
	response["ConsumedCapacity"] = map[string]any{
		"TableName":     tableName,
		"CapacityUnits": float64(1),
	}
}

func validReturnConsumedCapacity(value string) bool {
	switch strings.ToUpper(defaultString(value, "NONE")) {
	case "NONE", "TOTAL", "INDEXES":
		return true
	default:
		return false
	}
}

func appendBatchConsumedCapacity(values *[]map[string]any, tableName string, mode string) {
	if mode == "" || strings.EqualFold(mode, "NONE") {
		return
	}
	*values = append(*values, map[string]any{
		"TableName":     tableName,
		"CapacityUnits": float64(1),
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, name string, message string) {
	w.Header().Set("X-Amzn-Errortype", name)
	writeJSON(w, status, map[string]string{
		"__type":  "com.amazonaws.dynamodb.v20120810#" + name,
		"message": message,
	})
}

func writeConditionCheckFailed(w http.ResponseWriter, message string, returnValues string, oldItem item, existed bool) {
	w.Header().Set("X-Amzn-Errortype", "ConditionalCheckFailedException")
	response := map[string]any{
		"__type":  "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
		"message": message,
	}
	if returnValues == "ALL_OLD" && existed {
		response["Item"] = cloneItem(oldItem)
	}
	writeJSON(w, http.StatusBadRequest, response)
}
