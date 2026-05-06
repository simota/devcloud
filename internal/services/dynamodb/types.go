package dynamodb

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
