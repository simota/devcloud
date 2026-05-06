package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

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
func decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	if err := json.NewDecoder(r.Body).Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "SerializationException", "invalid json request")
		return false
	}
	return true
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
