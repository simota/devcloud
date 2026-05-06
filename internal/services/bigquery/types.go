package bigquery

import "encoding/json"

type projectsListResponse struct {
	Kind       string            `json:"kind"`
	Projects   []projectListItem `json:"projects"`
	TotalItems int               `json:"totalItems"`
}

type projectListItem struct {
	Kind         string           `json:"kind"`
	ID           string           `json:"id"`
	NumericID    string           `json:"numericId"`
	ProjectRef   projectReference `json:"projectReference"`
	FriendlyName string           `json:"friendlyName"`
}

type projectReference struct {
	ProjectID string `json:"projectId"`
}

type datasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

type tableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

type routineReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	RoutineID string `json:"routineId"`
}

type datasetResource struct {
	Kind             string            `json:"kind,omitempty"`
	ID               string            `json:"id,omitempty"`
	SelfLink         string            `json:"selfLink,omitempty"`
	DatasetReference datasetReference  `json:"datasetReference"`
	Location         string            `json:"location,omitempty"`
	FriendlyName     string            `json:"friendlyName,omitempty"`
	Description      string            `json:"description,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	ETag             string            `json:"etag,omitempty"`
	CreationTime     string            `json:"creationTime,omitempty"`
	LastModifiedTime string            `json:"lastModifiedTime,omitempty"`
}

type datasetsListResponse struct {
	Kind          string            `json:"kind"`
	Datasets      []datasetListItem `json:"datasets,omitempty"`
	TotalItems    int               `json:"totalItems"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

type datasetListItem struct {
	Kind             string           `json:"kind"`
	ID               string           `json:"id"`
	DatasetReference datasetReference `json:"datasetReference"`
	Location         string           `json:"location,omitempty"`
	FriendlyName     string           `json:"friendlyName,omitempty"`
}

type tableResource struct {
	Kind              string             `json:"kind,omitempty"`
	ID                string             `json:"id,omitempty"`
	SelfLink          string             `json:"selfLink,omitempty"`
	TableReference    tableReference     `json:"tableReference"`
	Type              string             `json:"type,omitempty"`
	Schema            tableSchema        `json:"schema,omitempty"`
	FriendlyName      string             `json:"friendlyName,omitempty"`
	Description       string             `json:"description,omitempty"`
	Labels            map[string]string  `json:"labels,omitempty"`
	TimePartitioning  *timePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *rangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *clustering        `json:"clustering,omitempty"`
	View              *viewDefinition    `json:"view,omitempty"`
	ETag              string             `json:"etag,omitempty"`
	CreationTime      string             `json:"creationTime,omitempty"`
	LastModifiedTime  string             `json:"lastModifiedTime,omitempty"`
	NumRows           string             `json:"numRows,omitempty"`
	NumBytes          string             `json:"numBytes,omitempty"`
	Location          string             `json:"location,omitempty"`
}

type timePartitioning struct {
	Type          string `json:"type,omitempty"`
	Field         string `json:"field,omitempty"`
	ExpirationMS  string `json:"expirationMs,omitempty"`
	RequireFilter bool   `json:"requirePartitionFilter,omitempty"`
}

type rangePartitioning struct {
	Field string         `json:"field,omitempty"`
	Range partitionRange `json:"range,omitempty"`
}

type partitionRange struct {
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
	Interval string `json:"interval,omitempty"`
}

type clustering struct {
	Fields []string `json:"fields,omitempty"`
}

type viewDefinition struct {
	Query        string `json:"query,omitempty"`
	UseLegacySQL bool   `json:"useLegacySql"`
}

type tableSchema struct {
	Fields []tableFieldSchema `json:"fields,omitempty"`
}

type tableFieldSchema struct {
	Name        string             `json:"name"`
	Type        string             `json:"type,omitempty"`
	Mode        string             `json:"mode,omitempty"`
	Description string             `json:"description,omitempty"`
	Fields      []tableFieldSchema `json:"fields,omitempty"`
}

type tablesListResponse struct {
	Kind          string          `json:"kind"`
	Tables        []tableListItem `json:"tables,omitempty"`
	TotalItems    int             `json:"totalItems"`
	NextPageToken string          `json:"nextPageToken,omitempty"`
}

type tableListItem struct {
	Kind              string             `json:"kind"`
	ID                string             `json:"id"`
	TableReference    tableReference     `json:"tableReference"`
	Type              string             `json:"type,omitempty"`
	FriendlyName      string             `json:"friendlyName,omitempty"`
	TimePartitioning  *timePartitioning  `json:"timePartitioning,omitempty"`
	RangePartitioning *rangePartitioning `json:"rangePartitioning,omitempty"`
	Clustering        *clustering        `json:"clustering,omitempty"`
	View              *viewDefinition    `json:"view,omitempty"`
}

type routineResource struct {
	Kind              string               `json:"kind,omitempty"`
	ID                string               `json:"id,omitempty"`
	SelfLink          string               `json:"selfLink,omitempty"`
	RoutineReference  routineReference     `json:"routineReference"`
	RoutineType       string               `json:"routineType,omitempty"`
	Language          string               `json:"language,omitempty"`
	Arguments         []routineArgument    `json:"arguments,omitempty"`
	ReturnType        *standardSQLDataType `json:"returnType,omitempty"`
	DefinitionBody    string               `json:"definitionBody,omitempty"`
	Description       string               `json:"description,omitempty"`
	DeterminismLevel  string               `json:"determinismLevel,omitempty"`
	ImportedLibraries []string             `json:"importedLibraries,omitempty"`
	ETag              string               `json:"etag,omitempty"`
	CreationTime      string               `json:"creationTime,omitempty"`
	LastModifiedTime  string               `json:"lastModifiedTime,omitempty"`
}

type routineArgument struct {
	Name         string               `json:"name,omitempty"`
	Kind         string               `json:"kind,omitempty"`
	Mode         string               `json:"mode,omitempty"`
	DataType     *standardSQLDataType `json:"dataType,omitempty"`
	ArgumentKind string               `json:"argumentKind,omitempty"`
}

type standardSQLDataType struct {
	TypeKind         string                 `json:"typeKind,omitempty"`
	ArrayElementType *standardSQLDataType   `json:"arrayElementType,omitempty"`
	StructType       *standardSQLStructType `json:"structType,omitempty"`
}

type standardSQLStructType struct {
	Fields []standardSQLField `json:"fields,omitempty"`
}

type standardSQLField struct {
	Name string               `json:"name,omitempty"`
	Type *standardSQLDataType `json:"type,omitempty"`
}

type routinesListResponse struct {
	Kind          string            `json:"kind"`
	Routines      []routineResource `json:"routines,omitempty"`
	TotalItems    int               `json:"totalItems"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
}

type insertAllRequest struct {
	SkipInvalidRows     bool           `json:"skipInvalidRows"`
	IgnoreUnknownValues bool           `json:"ignoreUnknownValues"`
	Rows                []insertAllRow `json:"rows"`
}

type insertAllRow struct {
	InsertID string                     `json:"insertId,omitempty"`
	JSON     map[string]json.RawMessage `json:"json"`
}

type insertAllResponse struct {
	Kind         string        `json:"kind"`
	InsertErrors []insertError `json:"insertErrors,omitempty"`
}

type insertError struct {
	Index  int               `json:"index"`
	Errors []insertErrorItem `json:"errors"`
}

type insertErrorItem struct {
	Reason   string `json:"reason"`
	Location string `json:"location,omitempty"`
	Message  string `json:"message"`
}

type storedRow struct {
	InsertID   string                     `json:"insertId,omitempty"`
	JSON       map[string]json.RawMessage `json:"json"`
	InsertedAt string                     `json:"insertedAt"`
}

type tableDataListResponse struct {
	Kind      string         `json:"kind"`
	ETag      string         `json:"etag,omitempty"`
	TotalRows string         `json:"totalRows"`
	PageToken string         `json:"pageToken,omitempty"`
	Rows      []tableDataRow `json:"rows,omitempty"`
}

type tableDataRow struct {
	F []tableCell `json:"f"`
}

type tableCell struct {
	V any `json:"v"`
}

type serviceAccountResponse struct {
	Kind  string `json:"kind"`
	Email string `json:"email"`
}

type queryRequest struct {
	Query           string           `json:"query"`
	UseLegacySQL    *bool            `json:"useLegacySql,omitempty"`
	Location        string           `json:"location,omitempty"`
	MaxResults      int              `json:"maxResults,omitempty"`
	DryRun          bool             `json:"dryRun,omitempty"`
	QueryParameters []queryParameter `json:"queryParameters,omitempty"`
}

type jobInsertRequest struct {
	JobReference  jobReference     `json:"jobReference,omitempty"`
	Configuration jobConfiguration `json:"configuration"`
}

type setIAMPolicyRequest struct {
	Policy iamPolicy `json:"policy"`
}

type testIAMPermissionsRequest struct {
	Permissions []string `json:"permissions,omitempty"`
}

type testIAMPermissionsResponse struct {
	Permissions []string `json:"permissions,omitempty"`
}

type iamPolicy struct {
	Version  int          `json:"version,omitempty"`
	ETag     string       `json:"etag,omitempty"`
	Bindings []iamBinding `json:"bindings,omitempty"`
}

type iamBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members,omitempty"`
}

type jobConfiguration struct {
	DryRun  bool                    `json:"dryRun,omitempty"`
	Query   queryJobConfiguration   `json:"query,omitempty"`
	Copy    copyJobConfiguration    `json:"copy,omitempty"`
	Load    loadJobConfiguration    `json:"load,omitempty"`
	Extract extractJobConfiguration `json:"extract,omitempty"`
}

type queryJobConfiguration struct {
	Query             string           `json:"query,omitempty"`
	UseLegacySQL      *bool            `json:"useLegacySql,omitempty"`
	QueryParameters   []queryParameter `json:"queryParameters,omitempty"`
	DestinationTable  tableReference   `json:"destinationTable,omitempty"`
	CreateDisposition string           `json:"createDisposition,omitempty"`
	WriteDisposition  string           `json:"writeDisposition,omitempty"`
}

type queryParameter struct {
	Name           string              `json:"name,omitempty"`
	ParameterType  queryParameterType  `json:"parameterType"`
	ParameterValue queryParameterValue `json:"parameterValue"`
}

type queryParameterType struct {
	Type string `json:"type"`
}

type queryParameterValue struct {
	Value string `json:"value,omitempty"`
}

type copyJobConfiguration struct {
	SourceTable       tableReference   `json:"sourceTable,omitempty"`
	SourceTables      []tableReference `json:"sourceTables,omitempty"`
	DestinationTable  tableReference   `json:"destinationTable,omitempty"`
	CreateDisposition string           `json:"createDisposition,omitempty"`
	WriteDisposition  string           `json:"writeDisposition,omitempty"`
}

type loadJobConfiguration struct {
	SourceURIs        []string       `json:"sourceUris,omitempty"`
	DestinationTable  tableReference `json:"destinationTable,omitempty"`
	Schema            tableSchema    `json:"schema,omitempty"`
	SourceFormat      string         `json:"sourceFormat,omitempty"`
	SkipLeadingRows   int            `json:"skipLeadingRows,omitempty"`
	CreateDisposition string         `json:"createDisposition,omitempty"`
	WriteDisposition  string         `json:"writeDisposition,omitempty"`
}

type extractJobConfiguration struct {
	SourceTable       tableReference `json:"sourceTable,omitempty"`
	DestinationURIs   []string       `json:"destinationUris,omitempty"`
	DestinationFormat string         `json:"destinationFormat,omitempty"`
}

type queryResponse struct {
	Kind         string         `json:"kind"`
	Schema       tableSchema    `json:"schema,omitempty"`
	JobReference jobReference   `json:"jobReference"`
	TotalRows    string         `json:"totalRows"`
	PageToken    string         `json:"pageToken,omitempty"`
	Rows         []tableDataRow `json:"rows,omitempty"`
	JobComplete  bool           `json:"jobComplete"`
	CacheHit     bool           `json:"cacheHit"`
}

type jobsListResponse struct {
	Kind          string        `json:"kind"`
	Jobs          []jobResource `json:"jobs,omitempty"`
	NextPageToken string        `json:"nextPageToken,omitempty"`
}

type jobCancelResponse struct {
	Kind string      `json:"kind"`
	Job  jobResource `json:"job"`
}

type jobReference struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

type jobResource struct {
	Kind          string           `json:"kind"`
	ID            string           `json:"id"`
	SelfLink      string           `json:"selfLink"`
	JobReference  jobReference     `json:"jobReference"`
	Configuration jobConfiguration `json:"configuration,omitempty"`
	Status        jobStatus        `json:"status"`
	Statistics    jobStatistics    `json:"statistics,omitempty"`
}

type jobStatus struct {
	State string `json:"state"`
}

type jobStatistics struct {
	CreationTime string          `json:"creationTime,omitempty"`
	StartTime    string          `json:"startTime,omitempty"`
	EndTime      string          `json:"endTime,omitempty"`
	Query        queryStatistics `json:"query,omitempty"`
}

type queryStatistics struct {
	TotalRows string `json:"totalRows,omitempty"`
	CacheHit  bool   `json:"cacheHit"`
	DryRun    bool   `json:"dryRun,omitempty"`
}

type queryJobRecord struct {
	Job      jobResource   `json:"job"`
	Response queryResponse `json:"response"`
}

type queryExecutionResult struct {
	Fields []tableFieldSchema
	Rows   []tableDataRow
}

type simpleSelectQuery struct {
	ProjectID            string
	DatasetID            string
	TableID              string
	SelectedFields       []string
	Aggregate            aggregateSelection
	WhereConditions      []whereCondition
	WhereConditionGroups [][]whereCondition
	WhereField           string
	WhereOperator        string
	WhereValueRaw        json.RawMessage
	GroupBy              string
	OrderBy              string
	OrderDesc            bool
	Limit                int
	Offset               int
}

type aggregateSelection struct {
	Function string
	Field    string
	Alias    string
}

type whereCondition struct {
	Field    string
	Operator string
	ValueRaw json.RawMessage
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Errors  []errorItem `json:"errors"`
	Status  string      `json:"status"`
}

type errorItem struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}
