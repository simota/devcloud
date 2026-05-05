package redshift

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"devcloud/internal/services/redshift/backend"
	"devcloud/internal/services/redshift/backend/memory"
	"devcloud/internal/services/redshift/translator"
	s3svc "devcloud/internal/services/s3"
)

type Config struct {
	SQLAddr           string
	APIAddr           string
	Region            string
	ClusterIdentifier string
	Database          string
	NodeType          string
	NumberOfNodes     int
	StoragePath       string
	MaxStatementBytes int64
	MaxCopyInputBytes int64
	AuthMode          string
	User              string
	Password          string
	AccountID         string
	ObjectStore       s3svc.BucketStore
	SQLBackend        backend.SQLBackend
	BackendKind       string
	BackendMode       string
	Translator        translator.RedshiftTranslator
}

type Server struct {
	config           Config
	mu               sync.Mutex
	db               *database
	clusters         map[string]ClusterSnapshot
	snapshots        map[string]ClusterSnapshotMetadata
	statements       map[string]*statement
	clientTokenIndex map[string]string
	sessions         map[string]*session
	nextStatementID  int64
	nextSessionID    int64
	backend          backend.SQLBackend
	translator       translator.RedshiftTranslator
}

func NewServer(cfg Config) *Server {
	db := &database{schemas: map[string]*schema{}}
	db.schemas["public"] = &schema{tables: map[string]*table{}}
	clusters := map[string]ClusterSnapshot{defaultClusterIdentifier(cfg): clusterSnapshotFromConfig(cfg)}
	snapshots := map[string]ClusterSnapshotMetadata{}
	statements := map[string]*statement{}
	clientTokenIndex := map[string]string{}
	var nextStatementID int64
	if storedDB, storedClusters, storedSnapshots, storedStatements, storedClientTokenIndex, storedNextStatementID, err := loadState(cfg); err == nil {
		if storedDB != nil {
			db = storedDB
		}
		if len(storedClusters) > 0 {
			clusters = storedClusters
			normalizeClusterEndpoints(clusters, cfg)
		}
		if len(storedSnapshots) > 0 {
			snapshots = storedSnapshots
		}
		if len(storedStatements) > 0 {
			statements = storedStatements
		}
		if len(storedClientTokenIndex) > 0 {
			clientTokenIndex = storedClientTokenIndex
		}
		nextStatementID = storedNextStatementID
	}
	ensurePublicSchema(db)
	server := &Server{
		config:           cfg,
		db:               db,
		clusters:         clusters,
		snapshots:        snapshots,
		statements:       statements,
		clientTokenIndex: clientTokenIndex,
		sessions:         map[string]*session{},
		nextStatementID:  nextStatementID,
	}
	if cfg.SQLBackend != nil {
		server.backend = cfg.SQLBackend
	} else {
		server.backend = memory.New(server.executeSQLMemoryBackend, server.memoryCatalogSnapshot)
	}
	if cfg.Translator != nil {
		server.translator = cfg.Translator
	} else {
		server.translator = translator.NewPassthrough()
	}
	return server
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 2)
	go func() { errCh <- s.runSQL(ctx) }()
	go func() { errCh <- s.runAPI(ctx) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) runSQL(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.SQLAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handleSQLConn(conn)
	}
}

func (s *Server) runAPI(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.config.APIAddr,
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
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleHealth)
	mux.HandleFunc("/", s.handleAPI)
	return mux
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes().ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, "GET, HEAD")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "redshift",
		"status":  "running",
		"running": true,
	})
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSONError(w, http.StatusNotFound, "NotFound", "not found")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		methodNotAllowed(w, "GET, POST")
		return
	}
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if strings.HasPrefix(target, "RedshiftServerless.") {
			s.handleServerlessTarget(w, r, target)
			return
		}
		s.handleDataAPITarget(w, r, target)
		return
	}
	action := r.URL.Query().Get("Action")
	if action == "" && r.Method == http.MethodPost {
		if err := r.ParseForm(); err == nil {
			action = r.Form.Get("Action")
		}
	}
	switch action {
	case "DescribeClusters":
		s.handleDescribeClusters(w, r)
	case "GetClusterCredentials":
		s.handleGetClusterCredentials(w, r)
	case "CreateCluster":
		s.handleCreateCluster(w, r)
	case "DeleteCluster":
		s.handleDeleteCluster(w, r)
	case "DescribeClusterSnapshots":
		s.handleDescribeClusterSnapshots(w, r)
	case "CreateClusterSnapshot":
		s.handleCreateClusterSnapshot(w, r)
	case "DeleteClusterSnapshot":
		s.handleDeleteClusterSnapshot(w, r)
	case "RestoreFromClusterSnapshot":
		s.handleRestoreFromClusterSnapshot(w, r)
	case "DescribeTags":
		s.handleDescribeTags(w, r)
	case "DescribeClusterParameterGroups":
		s.handleDescribeClusterParameterGroups(w, r)
	case "DescribeClusterParameters":
		s.handleDescribeClusterParameters(w, r)
	case "CreateTags":
		s.handleCreateTags(w, r)
	case "DeleteTags":
		s.handleDeleteTags(w, r)
	case "":
		writeJSON(w, http.StatusOK, s.Snapshot())
	default:
		writeJSONError(w, http.StatusBadRequest, "InvalidAction", "unsupported redshift action")
	}
}

func (s *Server) handleServerlessTarget(w http.ResponseWriter, r *http.Request, target string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	operation := target
	if index := strings.LastIndex(operation, "."); index >= 0 {
		operation = operation[index+1:]
	}
	switch operation {
	case "ListNamespaces":
		s.handleListServerlessNamespaces(w, r)
	case "GetNamespace":
		s.handleGetServerlessNamespace(w, r)
	case "ListWorkgroups":
		s.handleListServerlessWorkgroups(w, r)
	case "GetWorkgroup":
		s.handleGetServerlessWorkgroup(w, r)
	default:
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "unsupported redshift-serverless action")
	}
}

func (s *Server) handleDataAPITarget(w http.ResponseWriter, r *http.Request, target string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	operation := target
	if index := strings.LastIndex(operation, "."); index >= 0 {
		operation = operation[index+1:]
	}
	switch operation {
	case "BatchExecuteStatement":
		s.handleBatchExecuteStatement(w, r)
	case "ExecuteStatement":
		s.handleExecuteStatement(w, r)
	case "DescribeStatement":
		s.handleDescribeStatement(w, r)
	case "GetStatementResult":
		s.handleGetStatementResult(w, r)
	case "GetStatementResultV2":
		s.handleGetStatementResultV2(w, r)
	case "ListStatements":
		s.handleListStatements(w, r)
	case "CancelStatement":
		s.handleCancelStatement(w, r)
	case "ListDatabases":
		s.handleListDatabases(w, r)
	case "ListSchemas":
		s.handleListSchemas(w, r)
	case "ListTables":
		s.handleListTables(w, r)
	case "DescribeTable":
		s.handleDescribeTable(w, r)
	default:
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "unsupported redshift-data action")
	}
}

func (s *Server) handleExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request executeStatementRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.SQL) == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Sql is required")
		return
	}
	if err := s.validateStatementSize(request.SQL); err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if request.ClientToken != "" {
		s.mu.Lock()
		if id := s.clientTokenIndex[request.ClientToken]; id != "" {
			stmt := s.statements[id]
			s.mu.Unlock()
			writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
			return
		}
		s.mu.Unlock()
	}

	createdAt := time.Now().UTC()
	resultFormat, err := normalizeDataAPIResultFormat(request.ResultFormat)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	sessionID, err := s.sessionIDForRequest(request.SessionID, request.SessionKeepAliveSeconds, createdAt)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	result, err := s.executeSQL(request.SQL)
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(request.ClusterIdentifier, defaultString(s.config.ClusterIdentifier, "devcloud")),
		Database:          defaultString(request.Database, defaultString(s.config.Database, "dev")),
		DbUser:            defaultString(request.DbUser, defaultString(s.config.User, "dev")),
		SessionID:         sessionID,
		QueryString:       request.SQL,
		ResultFormat:      resultFormat,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            result,
		HasResultSet:      len(result.fields) > 0,
	}
	if err != nil {
		stmt.Status = "FAILED"
		stmt.Error = err.Error()
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	if request.ClientToken != "" {
		s.clientTokenIndex[request.ClientToken] = stmt.ID
	}
	_ = s.persistLocked()
	s.mu.Unlock()

	writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
}

func (s *Server) handleBatchExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request batchExecuteStatementRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	sqls := compactSQLStatements(request.SQLs)
	if len(sqls) == 0 {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Sqls is required")
		return
	}
	for _, sql := range sqls {
		if err := s.validateStatementSize(sql); err != nil {
			writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}
	if request.ClientToken != "" {
		s.mu.Lock()
		if id := s.clientTokenIndex[request.ClientToken]; id != "" {
			stmt := s.statements[id]
			s.mu.Unlock()
			writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
			return
		}
		s.mu.Unlock()
	}

	createdAt := time.Now().UTC()
	resultFormat, err := normalizeDataAPIResultFormat(request.ResultFormat)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	sessionID, err := s.sessionIDForRequest(request.SessionID, request.SessionKeepAliveSeconds, createdAt)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	queryString := strings.Join(sqls, ";\n")
	result, err := s.executeSQLBatch(sqls)
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(request.ClusterIdentifier, defaultString(s.config.ClusterIdentifier, "devcloud")),
		Database:          defaultString(request.Database, defaultString(s.config.Database, "dev")),
		DbUser:            defaultString(request.DbUser, defaultString(s.config.User, "dev")),
		SessionID:         sessionID,
		QueryString:       queryString,
		ResultFormat:      resultFormat,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            result,
		HasResultSet:      len(result.fields) > 0,
	}
	if err != nil {
		stmt.Status = "FAILED"
		stmt.Error = err.Error()
		stmt.HasResultSet = false
		stmt.Result = queryResult{}
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	if request.ClientToken != "" {
		s.clientTokenIndex[request.ClientToken] = stmt.ID
	}
	_ = s.persistLocked()
	s.mu.Unlock()

	writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
}

func (s *Server) handleDescribeStatement(w http.ResponseWriter, r *http.Request) {
	var request statementIDRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	writeDataAPIJSON(w, http.StatusOK, describeStatementResponseFromStatement(stmt))
}

func (s *Server) handleGetStatementResult(w http.ResponseWriter, r *http.Request) {
	var request getStatementResultRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	if stmt.Status != "FINISHED" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "statement has no finished result")
		return
	}
	rows, nextToken, err := paginateRows(stmt.Result.rows, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := getStatementResultResponse(stmt.Result, rows)
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetStatementResultV2(w http.ResponseWriter, r *http.Request) {
	var request getStatementResultRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	if stmt.Status != "FINISHED" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "statement has no finished result")
		return
	}
	if !strings.EqualFold(defaultString(stmt.ResultFormat, "JSON"), "CSV") {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "GetStatementResultV2 requires a statement executed with ResultFormat CSV")
		return
	}
	rows, nextToken, err := paginateRows(stmt.Result.rows, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := getStatementResultV2Response(stmt.Result, rows)
	if err != nil {
		writeDataAPIError(w, http.StatusInternalServerError, "InternalServerException", "failed to encode CSV result")
		return
	}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelStatement(w http.ResponseWriter, r *http.Request) {
	var request statementIDRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	if request.ID == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Id is required")
		return
	}
	s.mu.Lock()
	stmt := s.statements[request.ID]
	if stmt == nil {
		s.mu.Unlock()
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "statement does not exist")
		return
	}
	cancelled := stmt.Status == "SUBMITTED" || stmt.Status == "STARTED"
	if cancelled {
		stmt.Status = "ABORTED"
		stmt.UpdatedAt = time.Now().UTC()
		_ = s.persistLocked()
	}
	s.mu.Unlock()
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"Status": cancelled})
}

func (s *Server) handleListStatements(w http.ResponseWriter, r *http.Request) {
	var request listStatementsRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	statements := make([]statementListItem, 0, len(s.statements))
	ids := make([]string, 0, len(s.statements))
	for id := range s.statements {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		stmt := s.statements[id]
		if request.Status != "" && !strings.EqualFold(stmt.Status, request.Status) {
			continue
		}
		statements = append(statements, statementListItem{
			ID:           stmt.ID,
			QueryString:  safeStatementQueryString(stmt.QueryString),
			Status:       stmt.Status,
			CreatedAt:    stmt.CreatedAt.Unix(),
			UpdatedAt:    stmt.UpdatedAt.Unix(),
			HasResultSet: stmt.HasResultSet,
		})
	}
	s.mu.Unlock()
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"Statements": statements})
}

func (s *Server) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"Databases": []string{defaultString(s.config.Database, "dev")},
	})
}

func (s *Server) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	schemas := make([]string, 0, len(s.db.schemas))
	for name := range s.db.schemas {
		if metadataPatternMatches(name, request.SchemaPattern) {
			schemas = append(schemas, name)
		}
	}
	s.mu.Unlock()
	sort.Strings(schemas)
	page, nextToken, err := paginateStrings(schemas, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"Schemas": page}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	var tables []tableMember
	schemaNames := make([]string, 0, len(s.db.schemas))
	for schemaName := range s.db.schemas {
		if request.Schema != "" && schemaName != request.Schema {
			continue
		}
		if metadataPatternMatches(schemaName, request.SchemaPattern) {
			schemaNames = append(schemaNames, schemaName)
		}
	}
	sort.Strings(schemaNames)
	for _, schemaName := range schemaNames {
		schemaState := s.db.schemas[schemaName]
		tableNames := make([]string, 0, len(schemaState.tables))
		for name := range schemaState.tables {
			if metadataPatternMatches(name, request.TablePattern) {
				tableNames = append(tableNames, name)
			}
		}
		sort.Strings(tableNames)
		for _, name := range tableNames {
			tables = append(tables, tableMember{Name: name, Schema: schemaName, Type: tableDataAPIType(schemaState.tables[name])})
		}
	}
	s.mu.Unlock()
	page, nextToken, err := paginateTableMembers(tables, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"Tables": page}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	var request describeTableRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	name := qualifiedName{schema: defaultString(request.Schema, "public"), table: request.Table}
	if name.table == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Table is required")
		return
	}
	s.mu.Lock()
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		s.mu.Unlock()
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "table does not exist")
		return
	}
	columns := make([]columnMetadata, 0, len(tableState.columns))
	for i, column := range tableState.columns {
		columns = append(columns, columnMetadataFromColumn(column, i))
	}
	s.mu.Unlock()
	page, nextToken, err := paginateColumnMetadata(columns, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"ColumnList": page, "TableName": name.table}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleDescribeClusters(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	clusters := s.clusterXMLsLocked()
	s.mu.Unlock()
	response := describeClustersResponse{
		XMLName:   xml.Name{Local: "DescribeClustersResponse"},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
		Clusters:  clusters,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleListServerlessNamespaces(w http.ResponseWriter, r *http.Request) {
	var request serverlessListRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"namespaces": []serverlessNamespace{s.serverlessNamespace()},
	})
}

func (s *Server) handleGetServerlessNamespace(w http.ResponseWriter, r *http.Request) {
	var request serverlessNamespaceRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	namespace := s.serverlessNamespace()
	if request.NamespaceName != "" && request.NamespaceName != namespace.NamespaceName {
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "namespace does not exist")
		return
	}
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"namespace": namespace})
}

func (s *Server) handleListServerlessWorkgroups(w http.ResponseWriter, r *http.Request) {
	var request serverlessListRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"workgroups": []serverlessWorkgroup{s.serverlessWorkgroup()},
	})
}

func (s *Server) handleGetServerlessWorkgroup(w http.ResponseWriter, r *http.Request) {
	var request serverlessWorkgroupRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	workgroup := s.serverlessWorkgroup()
	if request.WorkgroupName != "" && request.WorkgroupName != workgroup.WorkgroupName {
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "workgroup does not exist")
		return
	}
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"workgroup": workgroup})
}

func (s *Server) handleGetClusterCredentials(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	identifier := defaultString(r.Form.Get("ClusterIdentifier"), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	_, exists := s.clusters[identifier]
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	durationSeconds := parseCredentialDurationSeconds(r.Form.Get("DurationSeconds"))
	response := getClusterCredentialsResponse{
		XMLName:    xml.Name{Local: "GetClusterCredentialsResponse"},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		DbUser:     defaultString(r.Form.Get("DbUser"), defaultString(s.config.User, "dev")),
		DbPassword: defaultString(s.config.Password, "dev"),
		Expiration: time.Now().UTC().Add(time.Duration(durationSeconds) * time.Second).Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateCluster(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	cluster := s.clusterSnapshotFromForm(r)
	s.mu.Lock()
	if _, exists := s.clusters[cluster.ClusterIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterAlreadyExists", "cluster already exists")
		return
	}
	s.clusters[cluster.ClusterIdentifier] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift cluster metadata failed")
		return
	}
	s.mu.Unlock()
	s.writeClusterActionXML(w, "CreateClusterResponse", "CreateClusterResult", cluster)
}

func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	identifier := defaultString(r.Form.Get("ClusterIdentifier"), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	cluster, exists := s.clusters[identifier]
	if exists {
		delete(s.clusters, identifier)
	}
	if exists {
		if err := s.persistLocked(); err != nil {
			s.mu.Unlock()
			writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift cluster metadata failed")
			return
		}
	}
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	s.writeClusterActionXML(w, "DeleteClusterResponse", "DeleteClusterResult", cluster)
}

func (s *Server) handleDescribeClusterSnapshots(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	clusterIdentifier := strings.TrimSpace(r.Form.Get("ClusterIdentifier"))
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	s.mu.Lock()
	snapshots := s.clusterSnapshotXMLsLocked(clusterIdentifier, snapshotIdentifier)
	s.mu.Unlock()
	response := describeClusterSnapshotsResponse{
		XMLName:   xml.Name{Local: "DescribeClusterSnapshotsResponse"},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
		Snapshots: snapshots,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	clusterIdentifier := defaultString(strings.TrimSpace(r.Form.Get("ClusterIdentifier")), defaultClusterIdentifier(s.config))
	s.mu.Lock()
	cluster, exists := s.clusters[clusterIdentifier]
	if !exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	if _, exists := s.snapshots[snapshotIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterSnapshotAlreadyExists", "cluster snapshot already exists")
		return
	}
	snapshot := clusterSnapshotMetadataFromCluster(snapshotIdentifier, cluster)
	s.snapshots[snapshotIdentifier] = snapshot
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift snapshot metadata failed")
		return
	}
	s.mu.Unlock()
	s.writeClusterSnapshotActionXML(w, "CreateClusterSnapshotResponse", "CreateClusterSnapshotResult", snapshot)
}

func (s *Server) handleDeleteClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	s.mu.Lock()
	snapshot, exists := s.snapshots[snapshotIdentifier]
	if exists {
		delete(s.snapshots, snapshotIdentifier)
	}
	if exists {
		if err := s.persistLocked(); err != nil {
			s.mu.Unlock()
			writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift snapshot metadata failed")
			return
		}
	}
	s.mu.Unlock()
	if !exists {
		writeJSONError(w, http.StatusNotFound, "ClusterSnapshotNotFound", "cluster snapshot does not exist")
		return
	}
	s.writeClusterSnapshotActionXML(w, "DeleteClusterSnapshotResponse", "DeleteClusterSnapshotResult", snapshot)
}

func (s *Server) handleRestoreFromClusterSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	snapshotIdentifier := strings.TrimSpace(r.Form.Get("SnapshotIdentifier"))
	if snapshotIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "SnapshotIdentifier is required")
		return
	}
	clusterIdentifier := strings.TrimSpace(r.Form.Get("ClusterIdentifier"))
	if clusterIdentifier == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ClusterIdentifier is required")
		return
	}

	s.mu.Lock()
	snapshot, exists := s.snapshots[snapshotIdentifier]
	if !exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterSnapshotNotFound", "cluster snapshot does not exist")
		return
	}
	if _, exists := s.clusters[clusterIdentifier]; exists {
		s.mu.Unlock()
		writeJSONError(w, http.StatusBadRequest, "ClusterAlreadyExists", "cluster already exists")
		return
	}
	cluster := clusterSnapshotFromSnapshotMetadata(clusterIdentifier, snapshot, s.config)
	s.clusters[clusterIdentifier] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift restored cluster metadata failed")
		return
	}
	s.mu.Unlock()

	s.writeClusterActionXML(w, "RestoreFromClusterSnapshotResponse", "RestoreFromClusterSnapshotResult", cluster)
}

func (s *Server) handleDescribeTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	s.mu.Lock()
	taggedResources := s.taggedResourcesLocked(resourceName)
	s.mu.Unlock()
	response := describeTagsResponse{
		XMLName:         xml.Name{Local: "DescribeTagsResponse"},
		Xmlns:           "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:       "devcloud-redshift",
		TaggedResources: taggedResources,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleDescribeClusterParameterGroups(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	name := strings.TrimSpace(r.Form.Get("ParameterGroupName"))
	groups := []parameterGroupXML{defaultParameterGroupXML()}
	if name != "" && name != groups[0].ParameterGroupName {
		groups = nil
	}
	response := describeClusterParameterGroupsResponse{
		XMLName:         xml.Name{Local: "DescribeClusterParameterGroupsResponse"},
		Xmlns:           "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:       "devcloud-redshift",
		ParameterGroups: groups,
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleDescribeClusterParameters(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	name := defaultString(strings.TrimSpace(r.Form.Get("ParameterGroupName")), defaultParameterGroupName)
	if name != defaultParameterGroupName {
		writeJSONError(w, http.StatusNotFound, "ClusterParameterGroupNotFound", "cluster parameter group does not exist")
		return
	}
	response := describeClusterParametersResponse{
		XMLName:    xml.Name{Local: "DescribeClusterParametersResponse"},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		Parameters: defaultClusterParameters(),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) handleCreateTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	if resourceName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ResourceName is required")
		return
	}
	tags := parseTagMembers(r.Form)
	if len(tags) == 0 {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Tags are required")
		return
	}
	s.mu.Lock()
	cluster, id, ok := s.clusterByResourceNameLocked(resourceName)
	if !ok {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	cluster.Tags = mergeTags(cluster.Tags, tags)
	s.clusters[id] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift tag metadata failed")
		return
	}
	s.mu.Unlock()
	writeEmptyQueryXML(w, "CreateTagsResponse")
}

func (s *Server) handleDeleteTags(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "invalid redshift query request")
		return
	}
	resourceName := r.Form.Get("ResourceName")
	if resourceName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "ResourceName is required")
		return
	}
	keys := parseTagKeyMembers(r.Form)
	if len(keys) == 0 {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "TagKeys are required")
		return
	}
	s.mu.Lock()
	cluster, id, ok := s.clusterByResourceNameLocked(resourceName)
	if !ok {
		s.mu.Unlock()
		writeJSONError(w, http.StatusNotFound, "ClusterNotFound", "cluster does not exist")
		return
	}
	cluster.Tags = deleteTags(cluster.Tags, keys)
	s.clusters[id] = cluster
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeJSONError(w, http.StatusInternalServerError, "InternalFailure", "persist redshift tag metadata failed")
		return
	}
	s.mu.Unlock()
	writeEmptyQueryXML(w, "DeleteTagsResponse")
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	clusters := s.clusterSnapshotsLocked()
	s.mu.Unlock()
	return Snapshot{
		Status:      "running",
		Running:     true,
		Region:      defaultString(s.config.Region, "us-east-1"),
		StoragePath: s.config.StoragePath,
		BackendKind: defaultString(s.config.BackendKind, "memory"),
		BackendMode: defaultString(s.config.BackendMode, "embedded"),
		Clusters:    clusters,
	}
}

func (s *Server) clusterSnapshot() ClusterSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cluster, ok := s.clusters[defaultClusterIdentifier(s.config)]; ok {
		return cluster
	}
	return clusterSnapshotFromConfig(s.config)
}

func (s *Server) clusterXML() clusterXML {
	return clusterXMLFromSnapshot(s.clusterSnapshot())
}

func (s *Server) serverlessNamespace() serverlessNamespace {
	cluster := s.clusterSnapshot()
	database := defaultString(cluster.DatabaseName, defaultString(s.config.Database, "dev"))
	return serverlessNamespace{
		NamespaceName: database,
		DBName:        database,
		Status:        "AVAILABLE",
	}
}

func (s *Server) serverlessWorkgroup() serverlessWorkgroup {
	cluster := s.clusterSnapshot()
	return serverlessWorkgroup{
		WorkgroupName: cluster.ClusterIdentifier,
		NamespaceName: defaultString(cluster.DatabaseName, defaultString(s.config.Database, "dev")),
		Status:        "AVAILABLE",
		Endpoint:      cluster.Endpoint,
	}
}

func (s *Server) clusterSnapshotFromForm(r *http.Request) ClusterSnapshot {
	cluster := clusterSnapshotFromConfig(s.config)
	cluster.ClusterIdentifier = defaultString(r.Form.Get("ClusterIdentifier"), cluster.ClusterIdentifier)
	cluster.DatabaseName = defaultString(r.Form.Get("DBName"), cluster.DatabaseName)
	cluster.NodeType = defaultString(r.Form.Get("NodeType"), cluster.NodeType)
	cluster.MasterUsername = defaultString(r.Form.Get("MasterUsername"), cluster.MasterUsername)
	if nodes, err := strconv.Atoi(r.Form.Get("NumberOfNodes")); err == nil && nodes > 0 {
		cluster.NumberOfNodes = nodes
	}
	cluster.Tags = parseTagMembers(r.Form)
	return cluster
}

func (s *Server) clusterSnapshotsLocked() []ClusterSnapshot {
	ids := make([]string, 0, len(s.clusters))
	for id := range s.clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	clusters := make([]ClusterSnapshot, 0, len(ids))
	for _, id := range ids {
		clusters = append(clusters, s.clusters[id])
	}
	return clusters
}

func (s *Server) clusterXMLsLocked() []clusterXML {
	clusters := s.clusterSnapshotsLocked()
	result := make([]clusterXML, 0, len(clusters))
	for _, cluster := range clusters {
		result = append(result, clusterXMLFromSnapshot(cluster))
	}
	return result
}

func (s *Server) taggedResourcesLocked(resourceName string) []taggedResourceXML {
	clusters := s.clusterSnapshotsLocked()
	result := make([]taggedResourceXML, 0)
	for _, cluster := range clusters {
		arn := s.clusterARN(cluster.ClusterIdentifier)
		if resourceName != "" && resourceName != arn {
			continue
		}
		for _, tag := range cluster.Tags {
			result = append(result, taggedResourceXML{
				ResourceName: arn,
				ResourceType: "cluster",
				Tag:          tag,
			})
		}
	}
	return result
}

func (s *Server) clusterByResourceNameLocked(resourceName string) (ClusterSnapshot, string, bool) {
	for id, cluster := range s.clusters {
		if resourceName == s.clusterARN(id) {
			return cluster, id, true
		}
	}
	return ClusterSnapshot{}, "", false
}

func (s *Server) clusterARN(identifier string) string {
	return "arn:aws:redshift:" + defaultString(s.config.Region, "us-east-1") + ":" + defaultString(s.config.AccountID, "000000000000") + ":cluster:" + identifier
}

func (s *Server) writeClusterActionXML(w http.ResponseWriter, responseName string, resultName string, cluster ClusterSnapshot) {
	response := clusterActionResponse{
		XMLName:    xml.Name{Local: responseName},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		ResultName: xml.Name{Local: resultName},
		Cluster:    clusterXMLFromSnapshot(cluster),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) writeClusterSnapshotActionXML(w http.ResponseWriter, responseName string, resultName string, snapshot ClusterSnapshotMetadata) {
	response := clusterSnapshotActionResponse{
		XMLName:    xml.Name{Local: responseName},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		ResultName: xml.Name{Local: resultName},
		Snapshot:   clusterSnapshotXMLFromMetadata(snapshot),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func writeEmptyQueryXML(w http.ResponseWriter, responseName string) {
	response := emptyQueryResponse{
		XMLName:   xml.Name{Local: responseName},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func clusterSnapshotFromConfig(cfg Config) ClusterSnapshot {
	return ClusterSnapshot{
		ClusterIdentifier: defaultClusterIdentifier(cfg),
		ClusterStatus:     "available",
		DatabaseName:      defaultString(cfg.Database, "dev"),
		Endpoint: ClusterEndpoint{
			Address: hostFromAddr(cfg.SQLAddr),
			Port:    portFromAddr(cfg.SQLAddr, 5439),
		},
		NodeType:       defaultString(cfg.NodeType, "dc2.large"),
		NumberOfNodes:  positiveOrDefault(cfg.NumberOfNodes, 1),
		MasterUsername: defaultString(cfg.User, "dev"),
	}
}

func clusterXMLFromSnapshot(cluster ClusterSnapshot) clusterXML {
	return clusterXML{
		ClusterIdentifier: cluster.ClusterIdentifier,
		ClusterStatus:     cluster.ClusterStatus,
		DBName:            cluster.DatabaseName,
		Endpoint: endpointXML{
			Address: cluster.Endpoint.Address,
			Port:    cluster.Endpoint.Port,
		},
		NodeType:       cluster.NodeType,
		NumberOfNodes:  cluster.NumberOfNodes,
		MasterUsername: cluster.MasterUsername,
	}
}

func defaultClusterIdentifier(cfg Config) string {
	return defaultString(cfg.ClusterIdentifier, "devcloud")
}

func (s *Server) clusterSnapshotXMLsLocked(clusterIdentifier string, snapshotIdentifier string) []clusterSnapshotXML {
	ids := make([]string, 0, len(s.snapshots))
	for id, snapshot := range s.snapshots {
		if snapshotIdentifier != "" && id != snapshotIdentifier {
			continue
		}
		if clusterIdentifier != "" && snapshot.ClusterIdentifier != clusterIdentifier {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]clusterSnapshotXML, 0, len(ids))
	for _, id := range ids {
		result = append(result, clusterSnapshotXMLFromMetadata(s.snapshots[id]))
	}
	return result
}

func clusterSnapshotMetadataFromCluster(identifier string, cluster ClusterSnapshot) ClusterSnapshotMetadata {
	return ClusterSnapshotMetadata{
		SnapshotIdentifier: identifier,
		ClusterIdentifier:  cluster.ClusterIdentifier,
		SnapshotCreateTime: time.Now().UTC().Format(time.RFC3339),
		Status:             "available",
		Port:               cluster.Endpoint.Port,
		AvailabilityZone:   "devcloud-local",
		ClusterCreateTime:  time.Now().UTC().Format(time.RFC3339),
		MasterUsername:     cluster.MasterUsername,
		ClusterVersion:     "1.0",
		EngineFullVersion:  "devcloud-redshift-1.0",
		NodeType:           cluster.NodeType,
		NumberOfNodes:      cluster.NumberOfNodes,
		DBName:             cluster.DatabaseName,
		Encrypted:          false,
	}
}

func clusterSnapshotFromSnapshotMetadata(identifier string, snapshot ClusterSnapshotMetadata, cfg Config) ClusterSnapshot {
	return ClusterSnapshot{
		ClusterIdentifier: identifier,
		ClusterStatus:     "available",
		DatabaseName:      defaultString(snapshot.DBName, defaultString(cfg.Database, "dev")),
		Endpoint: ClusterEndpoint{
			Address: hostFromAddr(cfg.SQLAddr),
			Port:    positiveOrDefault(snapshot.Port, portFromAddr(cfg.SQLAddr, 5439)),
		},
		NodeType:       defaultString(snapshot.NodeType, defaultString(cfg.NodeType, "dc2.large")),
		NumberOfNodes:  positiveOrDefault(snapshot.NumberOfNodes, positiveOrDefault(cfg.NumberOfNodes, 1)),
		MasterUsername: defaultString(snapshot.MasterUsername, defaultString(cfg.User, "dev")),
	}
}

type Snapshot struct {
	Status      string            `json:"status"`
	Running     bool              `json:"running"`
	Region      string            `json:"region"`
	StoragePath string            `json:"storagePath,omitempty"`
	BackendKind string            `json:"backendKind"`
	BackendMode string            `json:"backendMode"`
	Clusters    []ClusterSnapshot `json:"clusters"`
}

type CatalogSnapshot struct {
	Database string                `json:"database"`
	Schemas  []SchemaSnapshot      `json:"schemas"`
	Tables   []TableSnapshot       `json:"tables"`
	Columns  []TableColumnSnapshot `json:"columns"`
}

type SchemaSnapshot struct {
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	TableCount int    `json:"tableCount"`
}

type TableSnapshot struct {
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	ColumnCount int      `json:"columnCount"`
	RowCount    int      `json:"rowCount"`
	DistStyle   string   `json:"distStyle"`
	DistKey     string   `json:"distKey,omitempty"`
	SortKeys    []string `json:"sortKeys,omitempty"`
}

type TableDetailSnapshot struct {
	Table   TableSnapshot         `json:"table"`
	Columns []TableColumnSnapshot `json:"columns"`
	Rows    [][]string            `json:"rows,omitempty"`
}

type TableColumnSnapshot struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	Name         string `json:"name"`
	DataType     string `json:"dataType"`
	Ordinal      int    `json:"ordinal"`
	Encoding     string `json:"encoding,omitempty"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Identity     bool   `json:"identity,omitempty"`
}

type StatementSnapshot struct {
	ID                string `json:"id"`
	ClusterIdentifier string `json:"clusterIdentifier"`
	Database          string `json:"database"`
	DbUser            string `json:"dbUser"`
	SessionID         string `json:"sessionId,omitempty"`
	Status            string `json:"status"`
	QueryPreview      string `json:"queryPreview"`
	QueryRedacted     bool   `json:"queryRedacted"`
	QueryTruncated    bool   `json:"queryTruncated"`
	CreatedAt         int64  `json:"createdAt"`
	UpdatedAt         int64  `json:"updatedAt"`
	HasResultSet      bool   `json:"hasResultSet"`
	ResultRows        int    `json:"resultRows"`
	RedshiftQueryID   int64  `json:"redshiftQueryId"`
}

type QueryFieldSnapshot struct {
	Name     string `json:"name"`
	TypeName string `json:"typeName"`
}

type QueryResultSnapshot struct {
	Statement  StatementSnapshot    `json:"statement"`
	Columns    []QueryFieldSnapshot `json:"columns"`
	Rows       [][]string           `json:"rows"`
	RowCount   int                  `json:"rowCount"`
	CommandTag string               `json:"commandTag"`
}

func (s *Server) CatalogSnapshot() CatalogSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := CatalogSnapshot{
		Database: defaultString(s.config.Database, "dev"),
	}
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		schemaState := s.db.schemas[schemaName]
		snapshot.Schemas = append(snapshot.Schemas, SchemaSnapshot{
			Name:       schemaName,
			Owner:      defaultString(s.config.User, "dev"),
			TableCount: len(schemaState.tables),
		})
		for _, tableName := range sortedTableNames(schemaState.tables) {
			tableState := schemaState.tables[tableName]
			snapshot.Tables = append(snapshot.Tables, TableSnapshot{
				Schema:      schemaName,
				Name:        tableName,
				Type:        tableSnapshotType(tableState),
				ColumnCount: len(tableState.columns),
				RowCount:    len(tableState.rows),
				DistStyle:   defaultString(tableState.distStyle, "even"),
				DistKey:     tableState.distKey,
				SortKeys:    append([]string(nil), tableState.sortKeys...),
			})
			for i, columnState := range tableState.columns {
				snapshot.Columns = append(snapshot.Columns, TableColumnSnapshot{
					Schema:       schemaName,
					Table:        tableName,
					Name:         columnState.name,
					DataType:     columnState.dataType,
					Ordinal:      i + 1,
					Encoding:     columnState.encoding,
					DefaultValue: columnState.defaultValue,
					Identity:     columnState.identity,
				})
			}
		}
	}
	return snapshot
}

func (s *Server) TableDetailSnapshot(schemaName string, tableName string, limit int) (TableDetailSnapshot, bool) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	name := qualifiedName{schema: defaultString(schemaName, "public"), table: tableName}
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		return TableDetailSnapshot{}, false
	}
	detail := TableDetailSnapshot{
		Table: TableSnapshot{
			Schema:      tableState.name.schema,
			Name:        tableState.name.table,
			Type:        tableSnapshotType(tableState),
			ColumnCount: len(tableState.columns),
			RowCount:    len(tableState.rows),
			DistStyle:   defaultString(tableState.distStyle, "even"),
			DistKey:     tableState.distKey,
			SortKeys:    append([]string(nil), tableState.sortKeys...),
		},
	}
	for i, columnState := range tableState.columns {
		detail.Columns = append(detail.Columns, TableColumnSnapshot{
			Schema:       tableState.name.schema,
			Table:        tableState.name.table,
			Name:         columnState.name,
			DataType:     columnState.dataType,
			Ordinal:      i + 1,
			Encoding:     columnState.encoding,
			DefaultValue: columnState.defaultValue,
			Identity:     columnState.identity,
		})
	}
	rowLimit := min(limit, len(tableState.rows))
	detail.Rows = cloneRows(tableState.rows[:rowLimit])
	return detail, true
}

func (s *Server) StatementSnapshots() []StatementSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.statements))
	for id := range s.statements {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]StatementSnapshot, 0, len(ids))
	for _, id := range ids {
		stmt := s.statements[id]
		result = append(result, statementSnapshotFromStatement(stmt))
	}
	return result
}

func (s *Server) ExecuteDashboardSQL(sql string, maxRows int) (QueryResultSnapshot, error) {
	if maxRows <= 0 {
		maxRows = 100
	}
	statements := splitSQLStatements(sql)
	if len(statements) == 0 {
		return QueryResultSnapshot{}, errors.New("SQL is required")
	}
	var lastResult queryResult
	var lastSnapshot StatementSnapshot
	for _, statementText := range statements {
		if err := s.validateStatementSize(statementText); err != nil {
			lastSnapshot = s.recordSQLHistory("[statement exceeds maxStatementBytes]", queryResult{}, err)
			return QueryResultSnapshot{Statement: lastSnapshot}, err
		}
		result, err := s.executeSQL(statementText)
		lastSnapshot = s.recordSQLHistory(statementText, result, err)
		lastResult = result
		if err != nil {
			return QueryResultSnapshot{Statement: lastSnapshot}, err
		}
	}
	columns := make([]QueryFieldSnapshot, 0, len(lastResult.fields))
	for _, field := range lastResult.fields {
		columns = append(columns, QueryFieldSnapshot{Name: field.Name, TypeName: pgFieldTypeName(field)})
	}
	rowLimit := min(maxRows, len(lastResult.rows))
	return QueryResultSnapshot{
		Statement:  lastSnapshot,
		Columns:    columns,
		Rows:       cloneRows(lastResult.rows[:rowLimit]),
		RowCount:   len(lastResult.rows),
		CommandTag: lastResult.tag,
	}, nil
}

func statementSnapshotFromStatement(stmt *statement) StatementSnapshot {
	preview, redacted, truncated := safeSQLPreview(stmt.QueryString, 200)
	return StatementSnapshot{
		ID:                stmt.ID,
		ClusterIdentifier: stmt.ClusterIdentifier,
		Database:          stmt.Database,
		DbUser:            stmt.DbUser,
		SessionID:         stmt.SessionID,
		Status:            stmt.Status,
		QueryPreview:      preview,
		QueryRedacted:     redacted,
		QueryTruncated:    truncated,
		CreatedAt:         stmt.CreatedAt.Unix(),
		UpdatedAt:         stmt.UpdatedAt.Unix(),
		HasResultSet:      stmt.HasResultSet,
		ResultRows:        len(stmt.Result.rows),
		RedshiftQueryID:   redshiftQueryID(stmt.ID),
	}
}

func pgFieldTypeName(field pgField) string {
	switch field.TypeOID {
	case pgTypeInt4OID:
		return "int4"
	case pgTypeBoolOID:
		return "bool"
	case pgTypeFloat8OID:
		return "float8"
	}
	return "varchar"
}

type ClusterSnapshot struct {
	ClusterIdentifier string          `json:"clusterIdentifier"`
	ClusterStatus     string          `json:"clusterStatus"`
	DatabaseName      string          `json:"databaseName"`
	Endpoint          ClusterEndpoint `json:"endpoint"`
	NodeType          string          `json:"nodeType"`
	NumberOfNodes     int             `json:"numberOfNodes"`
	MasterUsername    string          `json:"masterUsername"`
	Tags              []Tag           `json:"tags,omitempty"`
}

type ClusterSnapshotMetadata struct {
	SnapshotIdentifier string `json:"snapshotIdentifier"`
	ClusterIdentifier  string `json:"clusterIdentifier"`
	SnapshotCreateTime string `json:"snapshotCreateTime"`
	Status             string `json:"status"`
	Port               int    `json:"port"`
	AvailabilityZone   string `json:"availabilityZone"`
	ClusterCreateTime  string `json:"clusterCreateTime"`
	MasterUsername     string `json:"masterUsername"`
	ClusterVersion     string `json:"clusterVersion"`
	EngineFullVersion  string `json:"engineFullVersion"`
	NodeType           string `json:"nodeType"`
	NumberOfNodes      int    `json:"numberOfNodes"`
	DBName             string `json:"dbName"`
	Encrypted          bool   `json:"encrypted"`
}

type ClusterEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type Tag struct {
	Key   string `json:"key" xml:"Key"`
	Value string `json:"value" xml:"Value"`
}

type serverlessListRequest struct {
	MaxResults int    `json:"maxResults,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
}

type serverlessNamespaceRequest struct {
	NamespaceName string `json:"namespaceName"`
}

type serverlessWorkgroupRequest struct {
	WorkgroupName string `json:"workgroupName"`
}

type serverlessNamespace struct {
	NamespaceName string `json:"namespaceName"`
	DBName        string `json:"dbName"`
	Status        string `json:"status"`
}

type serverlessWorkgroup struct {
	WorkgroupName string          `json:"workgroupName"`
	NamespaceName string          `json:"namespaceName"`
	Status        string          `json:"status"`
	Endpoint      ClusterEndpoint `json:"endpoint"`
}

type database struct {
	schemas map[string]*schema
}

type schema struct {
	tables map[string]*table
}

type table struct {
	name      qualifiedName
	columns   []column
	rows      [][]string
	kind      string
	viewSQL   string
	distStyle string
	distKey   string
	sortKeys  []string
}

type column struct {
	name         string
	dataType     string
	encoding     string
	defaultValue string
	identity     bool
	distKey      bool
	sortKey      bool
}

type qualifiedName struct {
	schema string
	table  string
}

func isView(tableState *table) bool {
	return tableState != nil && strings.EqualFold(tableState.kind, "VIEW")
}

func isMaterializedView(tableState *table) bool {
	return tableState != nil && strings.EqualFold(tableState.kind, "MATERIALIZED VIEW")
}

func isReadOnlyRelation(tableState *table) bool {
	return isView(tableState) || isMaterializedView(tableState)
}

func tableSnapshotType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED_VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "TABLE"
}

func tableDataAPIType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED_VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "TABLE"
}

func informationSchemaTableType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "BASE TABLE"
}

func pgClassRelKind(tableState *table) string {
	if isMaterializedView(tableState) {
		return "m"
	}
	if isView(tableState) {
		return "v"
	}
	return "r"
}

func columnsFromFields(fields []pgField) []column {
	columns := make([]column, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, column{name: field.Name, dataType: pgFieldTypeName(field)})
	}
	return columns
}

type describeClustersResponse struct {
	XMLName   xml.Name     `xml:"DescribeClustersResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"ResponseMetadata>RequestId"`
	Clusters  []clusterXML `xml:"DescribeClustersResult>Clusters>member"`
}

type getClusterCredentialsResponse struct {
	XMLName    xml.Name `xml:"GetClusterCredentialsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	DbUser     string   `xml:"GetClusterCredentialsResult>DbUser"`
	DbPassword string   `xml:"GetClusterCredentialsResult>DbPassword"`
	Expiration string   `xml:"GetClusterCredentialsResult>Expiration"`
	RequestID  string   `xml:"ResponseMetadata>RequestId"`
}

type clusterActionResponse struct {
	XMLName    xml.Name
	Xmlns      string
	RequestID  string
	ResultName xml.Name
	Cluster    clusterXML
}

type clusterSnapshotActionResponse struct {
	XMLName    xml.Name
	Xmlns      string
	RequestID  string
	ResultName xml.Name
	Snapshot   clusterSnapshotXML
}

type emptyQueryResponse struct {
	XMLName   xml.Name
	Xmlns     string `xml:"xmlns,attr"`
	RequestID string `xml:"ResponseMetadata>RequestId"`
}

func (r clusterActionResponse) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = r.XMLName
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xmlns"}, Value: r.Xmlns})
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	result := struct {
		Cluster clusterXML `xml:"Cluster"`
	}{Cluster: r.Cluster}
	if err := e.EncodeElement(result, xml.StartElement{Name: r.ResultName}); err != nil {
		return err
	}
	metadata := struct {
		RequestID string `xml:"RequestId"`
	}{RequestID: r.RequestID}
	if err := e.EncodeElement(metadata, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

func (r clusterSnapshotActionResponse) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = r.XMLName
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xmlns"}, Value: r.Xmlns})
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	result := struct {
		Snapshot clusterSnapshotXML `xml:"Snapshot"`
	}{Snapshot: r.Snapshot}
	if err := e.EncodeElement(result, xml.StartElement{Name: r.ResultName}); err != nil {
		return err
	}
	metadata := struct {
		RequestID string `xml:"RequestId"`
	}{RequestID: r.RequestID}
	if err := e.EncodeElement(metadata, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

type clusterXML struct {
	ClusterIdentifier string      `xml:"ClusterIdentifier"`
	ClusterStatus     string      `xml:"ClusterStatus"`
	DBName            string      `xml:"DBName"`
	Endpoint          endpointXML `xml:"Endpoint"`
	NodeType          string      `xml:"NodeType"`
	NumberOfNodes     int         `xml:"NumberOfNodes"`
	MasterUsername    string      `xml:"MasterUsername"`
}

type describeClusterSnapshotsResponse struct {
	XMLName   xml.Name             `xml:"DescribeClusterSnapshotsResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	Snapshots []clusterSnapshotXML `xml:"DescribeClusterSnapshotsResult>Snapshots>member"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
}

type clusterSnapshotXML struct {
	SnapshotIdentifier string `xml:"SnapshotIdentifier"`
	ClusterIdentifier  string `xml:"ClusterIdentifier"`
	SnapshotCreateTime string `xml:"SnapshotCreateTime"`
	Status             string `xml:"Status"`
	Port               int    `xml:"Port"`
	AvailabilityZone   string `xml:"AvailabilityZone"`
	ClusterCreateTime  string `xml:"ClusterCreateTime"`
	MasterUsername     string `xml:"MasterUsername"`
	ClusterVersion     string `xml:"ClusterVersion"`
	EngineFullVersion  string `xml:"EngineFullVersion"`
	NodeType           string `xml:"NodeType"`
	NumberOfNodes      int    `xml:"NumberOfNodes"`
	DBName             string `xml:"DBName"`
	Encrypted          bool   `xml:"Encrypted"`
}

func clusterSnapshotXMLFromMetadata(snapshot ClusterSnapshotMetadata) clusterSnapshotXML {
	return clusterSnapshotXML{
		SnapshotIdentifier: snapshot.SnapshotIdentifier,
		ClusterIdentifier:  snapshot.ClusterIdentifier,
		SnapshotCreateTime: snapshot.SnapshotCreateTime,
		Status:             snapshot.Status,
		Port:               snapshot.Port,
		AvailabilityZone:   snapshot.AvailabilityZone,
		ClusterCreateTime:  snapshot.ClusterCreateTime,
		MasterUsername:     snapshot.MasterUsername,
		ClusterVersion:     snapshot.ClusterVersion,
		EngineFullVersion:  snapshot.EngineFullVersion,
		NodeType:           snapshot.NodeType,
		NumberOfNodes:      snapshot.NumberOfNodes,
		DBName:             snapshot.DBName,
		Encrypted:          snapshot.Encrypted,
	}
}

type endpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type describeTagsResponse struct {
	XMLName         xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	TaggedResources []taggedResourceXML `xml:"DescribeTagsResult>TaggedResources>member"`
	RequestID       string              `xml:"ResponseMetadata>RequestId"`
}

type taggedResourceXML struct {
	ResourceName string `xml:"ResourceName"`
	ResourceType string `xml:"ResourceType"`
	Tag          Tag    `xml:"Tag"`
}

const defaultParameterGroupName = "default.redshift-1.0"

type describeClusterParameterGroupsResponse struct {
	XMLName         xml.Name            `xml:"DescribeClusterParameterGroupsResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	ParameterGroups []parameterGroupXML `xml:"DescribeClusterParameterGroupsResult>ParameterGroups>member"`
	RequestID       string              `xml:"ResponseMetadata>RequestId"`
}

type parameterGroupXML struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description"`
}

type describeClusterParametersResponse struct {
	XMLName    xml.Name       `xml:"DescribeClusterParametersResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	Parameters []parameterXML `xml:"DescribeClusterParametersResult>Parameters>member"`
	RequestID  string         `xml:"ResponseMetadata>RequestId"`
}

type parameterXML struct {
	ParameterName        string `xml:"ParameterName"`
	ParameterValue       string `xml:"ParameterValue"`
	Description          string `xml:"Description"`
	Source               string `xml:"Source"`
	DataType             string `xml:"DataType"`
	AllowedValues        string `xml:"AllowedValues,omitempty"`
	ApplyType            string `xml:"ApplyType"`
	IsModifiable         bool   `xml:"IsModifiable"`
	MinimumEngineVersion string `xml:"MinimumEngineVersion"`
}

func defaultParameterGroupXML() parameterGroupXML {
	return parameterGroupXML{
		ParameterGroupName:   defaultParameterGroupName,
		ParameterGroupFamily: "redshift-1.0",
		Description:          "Default devcloud Redshift-compatible parameter group",
	}
}

func defaultClusterParameters() []parameterXML {
	return []parameterXML{
		{
			ParameterName:        "datestyle",
			ParameterValue:       "ISO, MDY",
			Description:          "Sets the display format for date and time values.",
			Source:               "engine-default",
			DataType:             "string",
			ApplyType:            "static",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
		{
			ParameterName:        "enable_user_activity_logging",
			ParameterValue:       "false",
			Description:          "Controls user activity logging metadata for the local Redshift-compatible server.",
			Source:               "engine-default",
			DataType:             "boolean",
			AllowedValues:        "true,false",
			ApplyType:            "dynamic",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
		{
			ParameterName:        "max_query_execution_time",
			ParameterValue:       "0",
			Description:          "Maximum query execution time in seconds. Zero means unlimited in devcloud.",
			Source:               "engine-default",
			DataType:             "integer",
			AllowedValues:        "0-86400",
			ApplyType:            "dynamic",
			IsModifiable:         false,
			MinimumEngineVersion: "1.0",
		},
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSONError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]any{
		"__type":  code,
		"message": message,
	})
}

func writeDataAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func writeDataAPIError(w http.ResponseWriter, status int, code string, message string) {
	writeDataAPIJSON(w, status, map[string]any{
		"__type":  code,
		"message": message,
	})
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func normalizeDataAPIResultFormat(value string) (string, error) {
	if value == "" {
		return "JSON", nil
	}
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch normalized {
	case "JSON", "CSV":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported ResultFormat %q", value)
	}
}

func metadataPatternMatches(value string, pattern string) bool {
	if pattern == "" {
		return true
	}
	return sqlLikeMatch(strings.ToLower(value), strings.ToLower(pattern))
}

func sqlLikeMatch(value string, pattern string) bool {
	valueRunes := []rune(value)
	patternRunes := []rune(pattern)
	memo := map[[2]int]bool{}
	seen := map[[2]int]bool{}
	var match func(int, int) bool
	match = func(valueIndex int, patternIndex int) bool {
		key := [2]int{valueIndex, patternIndex}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		if patternIndex == len(patternRunes) {
			memo[key] = valueIndex == len(valueRunes)
			return memo[key]
		}
		switch patternRunes[patternIndex] {
		case '%':
			memo[key] = match(valueIndex, patternIndex+1) ||
				(valueIndex < len(valueRunes) && match(valueIndex+1, patternIndex))
		case '_':
			memo[key] = valueIndex < len(valueRunes) && match(valueIndex+1, patternIndex+1)
		default:
			memo[key] = valueIndex < len(valueRunes) &&
				valueRunes[valueIndex] == patternRunes[patternIndex] &&
				match(valueIndex+1, patternIndex+1)
		}
		return memo[key]
	}
	return match(0, 0)
}

func paginateStrings(values []string, maxResults int, nextToken string) ([]string, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []string{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateTableMembers(values []tableMember, maxResults int, nextToken string) ([]tableMember, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []tableMember{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateColumnMetadata(values []columnMetadata, maxResults int, nextToken string) ([]columnMetadata, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return []columnMetadata{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginateRows(values [][]string, maxResults int, nextToken string) ([][]string, string, error) {
	start, err := paginationStart(nextToken)
	if err != nil {
		return nil, "", err
	}
	if start >= len(values) {
		return [][]string{}, "", nil
	}
	end := paginationEnd(start, len(values), maxResults)
	next := ""
	if end < len(values) {
		next = strconv.Itoa(end)
	}
	return values[start:end], next, nil
}

func paginationStart(nextToken string) (int, error) {
	if nextToken == "" {
		return 0, nil
	}
	start, err := strconv.Atoi(nextToken)
	if err != nil || start < 0 {
		return 0, errors.New("NextToken is invalid")
	}
	return start, nil
}

func paginationEnd(start int, total int, maxResults int) int {
	if maxResults <= 0 || start+maxResults > total {
		return total
	}
	return start + maxResults
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func hostFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "127.0.0.1"
	}
	return host
}

func portFromAddr(addr string, fallback int) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fallback
	}
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseCredentialDurationSeconds(value string) int {
	const defaultDurationSeconds = 900
	if value == "" {
		return defaultDurationSeconds
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultDurationSeconds
	}
	return parsed
}

func parseTagMembers(values map[string][]string) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		key := strings.TrimSpace(firstFormValue(values, fmt.Sprintf("Tags.member.%d.Key", i)))
		value := firstFormValue(values, fmt.Sprintf("Tags.member.%d.Value", i))
		if key == "" && value == "" {
			break
		}
		if key == "" {
			continue
		}
		tags = append(tags, Tag{Key: key, Value: value})
	}
	return tags
}

func parseTagKeyMembers(values map[string][]string) []string {
	var keys []string
	for i := 1; ; i++ {
		key := strings.TrimSpace(firstFormValue(values, fmt.Sprintf("TagKeys.member.%d", i)))
		if key == "" {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func firstFormValue(values map[string][]string, key string) string {
	if len(values[key]) == 0 {
		return ""
	}
	return values[key][0]
}

func mergeTags(existing []Tag, updates []Tag) []Tag {
	byKey := make(map[string]string, len(existing)+len(updates))
	for _, tag := range existing {
		byKey[tag.Key] = tag.Value
	}
	for _, tag := range updates {
		byKey[tag.Key] = tag.Value
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, Tag{Key: key, Value: byKey[key]})
	}
	return result
}

func deleteTags(existing []Tag, keys []string) []Tag {
	remove := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		remove[key] = struct{}{}
	}
	result := make([]Tag, 0, len(existing))
	for _, tag := range existing {
		if _, ok := remove[tag.Key]; ok {
			continue
		}
		result = append(result, tag)
	}
	return result
}
