package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	bigquerysvc "devcloud/internal/services/bigquery"
	dynamodbsvc "devcloud/internal/services/dynamodb"
	"devcloud/internal/services/mail"
	pubsubsvc "devcloud/internal/services/pubsub"
	redshiftsvc "devcloud/internal/services/redshift"
	s3svc "devcloud/internal/services/s3"
	sqssvc "devcloud/internal/services/sqs"
	"devcloud/internal/storage/mailstore"
)

type Config struct {
	Addr                 string
	MailDisabled         bool
	MailEndpoint         string
	MailStoragePath      string
	S3Endpoint           string
	S3Region             string
	S3AuthMode           string
	S3StoragePath        string
	GCSEndpoint          string
	GCSProject           string
	GCSStoragePath       string
	GCSUploadSessionPath string
	DynamoDBEndpoint     string
	DynamoDBRegion       string
	DynamoDBStoragePath  string
	BigQueryEndpoint     string
	BigQueryProject      string
	BigQueryLocation     string
	BigQueryAuthMode     string
	BigQueryStoragePath  string
	RedshiftSQLEndpoint  string
	RedshiftAPIEndpoint  string
	RedshiftRegion       string
	RedshiftCluster      string
	RedshiftDatabase     string
	RedshiftStoragePath  string
	SQSEndpoint          string
	SQSRegion            string
	SQSAuthMode          string
	SQSStoragePath       string
	PubSubGRPCEndpoint   string
	PubSubRESTEndpoint   string
	PubSubProject        string
	PubSubStoragePath    string
}

type Server struct {
	config   Config
	store    mailstore.Store
	s3       s3svc.BucketStore
	gcs      s3svc.BucketStore
	dynamo   *dynamodbsvc.Server
	bq       *bigquerysvc.Server
	sqs      *sqssvc.Server
	pubsub   *pubsubsvc.Server
	redshift *redshiftsvc.Server
}

func NewServer(cfg Config, store mailstore.Store, objectStores ...s3svc.BucketStore) *Server {
	server := &Server{config: cfg, store: store}
	if len(objectStores) > 0 {
		server.s3 = objectStores[0]
	}
	if len(objectStores) > 1 {
		server.gcs = objectStores[1]
	}
	return server
}

func (s *Server) SetDynamoDB(server *dynamodbsvc.Server) {
	s.dynamo = server
}

func (s *Server) SetBigQuery(server *bigquerysvc.Server) {
	s.bq = server
}

func (s *Server) SetSQS(server *sqssvc.Server) {
	s.sqs = server
}

func (s *Server) SetPubSub(server *pubsubsvc.Server) {
	s.pubsub = server
}

func (s *Server) SetRedshift(server *redshiftsvc.Server) {
	s.redshift = server
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
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleServiceIndex)
	mux.HandleFunc("/dashboard", s.handleReactDashboardAssets)
	mux.HandleFunc("/dashboard/", s.handleReactDashboardAssets)
	mux.HandleFunc("/mail", s.handleMailIndex)
	mux.HandleFunc("/s3", s.handleS3Index)
	mux.HandleFunc("/gcs", s.handleGCSIndex)
	mux.HandleFunc("/dynamodb", s.handleDynamoDBIndex)
	mux.HandleFunc("/bigquery", s.handleBigQueryIndex)
	mux.HandleFunc("/dashboard/redshift", s.handleRedshiftIndex)
	mux.HandleFunc("/api/services", s.handleDashboardServices)
	mux.HandleFunc("/api/dashboard/services", s.handleDashboardServices)
	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/messages/", s.handleMessage)
	mux.HandleFunc("/api/s3/status", s.handleS3Status)
	mux.HandleFunc("/api/s3/buckets", s.handleS3Buckets)
	mux.HandleFunc("/api/s3/buckets/", s.handleS3Bucket)
	mux.HandleFunc("/api/gcs/status", s.handleGCSStatus)
	mux.HandleFunc("/api/gcs/buckets", s.handleGCSBuckets)
	mux.HandleFunc("/api/gcs/buckets/", s.handleGCSBucket)
	mux.HandleFunc("/api/gcs/uploads", s.handleGCSUploadSessions)
	mux.HandleFunc("/api/gcs/uploads/", s.handleGCSUploadSession)
	mux.HandleFunc("/api/gcs/upload-sessions", s.handleGCSUploadSessions)
	mux.HandleFunc("/api/dynamodb/status", s.handleDynamoDBStatus)
	mux.HandleFunc("/api/dynamodb/tables", s.handleDynamoDBTables)
	mux.HandleFunc("/api/dynamodb/tables/", s.handleDynamoDBTable)
	mux.HandleFunc("/api/bigquery/status", s.handleBigQueryStatus)
	mux.HandleFunc("/api/bigquery/projects", s.handleBigQueryProjects)
	mux.HandleFunc("/api/bigquery/projects/", s.handleBigQueryProjectResource)
	mux.HandleFunc("/api/redshift/status", s.handleRedshiftStatus)
	mux.HandleFunc("/api/redshift/clusters", s.handleRedshiftClusters)
	mux.HandleFunc("/api/redshift/catalog", s.handleRedshiftCatalog)
	mux.HandleFunc("/api/redshift/statements", s.handleRedshiftStatements)
	mux.HandleFunc("/api/redshift/tables/", s.handleRedshiftTable)
	mux.HandleFunc("/api/redshift/query", s.handleRedshiftQuery)
	mux.HandleFunc("/api/sqs/status", s.handleSQSStatus)
	mux.HandleFunc("/api/sqs/queues", s.handleSQSQueues)
	mux.HandleFunc("/api/sqs/queues/", s.handleSQSQueue)
	mux.HandleFunc("/api/pubsub/health", s.handlePubSubStatus)
	mux.HandleFunc("/api/pubsub/projects", s.handlePubSubProjects)
	mux.HandleFunc("/api/pubsub/status", s.handlePubSubStatus)
	mux.HandleFunc("/api/pubsub/topics", s.handlePubSubTopics)
	mux.HandleFunc("/api/pubsub/topics/", s.handlePubSubTopic)
	mux.HandleFunc("/api/pubsub/subscriptions", s.handlePubSubSubscriptions)
	mux.HandleFunc("/api/pubsub/subscriptions/", s.handlePubSubSubscription)
	mux.HandleFunc("/api/pubsub/messages/", s.handlePubSubMessage)
	return mux
}

func (s *Server) handleServiceIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, serviceIndexHTML)
}

func (s *Server) handleMailIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, indexHTML)
}

func (s *Server) handleS3Index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, s3IndexHTML)
}

func (s *Server) handleGCSIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, gcsIndexHTML)
}

func (s *Server) handleDynamoDBIndex(w http.ResponseWriter, r *http.Request) {
	serveReactDashboardApp(w, r)
}

func (s *Server) handleBigQueryIndex(w http.ResponseWriter, r *http.Request) {
	serveReactDashboardApp(w, r)
}

func (s *Server) handleRedshiftIndex(w http.ResponseWriter, r *http.Request) {
	serveReactDashboardApp(w, r)
}

func (s *Server) handleDashboardServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	writeJSON(w, dashboardServicesResponse{Services: s.dashboardServices()})
}

func (s *Server) handleRedshiftStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	region := defaultString(s.config.RedshiftRegion, "us-east-1")
	clusterCount := 0
	backendKind := "postgres"
	backendMode := "managed"
	if s.redshift != nil {
		snapshot := s.redshift.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		region = snapshot.Region
		clusterCount = len(snapshot.Clusters)
		backendKind = defaultString(snapshot.BackendKind, backendKind)
		backendMode = defaultString(snapshot.BackendMode, backendMode)
	}
	writeJSON(w, map[string]any{
		"service":      "redshift",
		"status":       status,
		"running":      running,
		"sqlEndpoint":  defaultString(s.config.RedshiftSQLEndpoint, "127.0.0.1:5439"),
		"apiEndpoint":  defaultString(s.config.RedshiftAPIEndpoint, "http://127.0.0.1:9099"),
		"region":       region,
		"clusterCount": clusterCount,
		"storagePath":  defaultString(s.config.RedshiftStoragePath, ".devcloud/data/redshift"),
		"backendKind":  backendKind,
		"backendMode":  backendMode,
	})
}

func (s *Server) handleRedshiftClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.redshift.Snapshot()
	writeJSON(w, map[string]any{
		"clusters": snapshot.Clusters,
	})
}

func (s *Server) handleRedshiftCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{
		"catalog": s.redshift.CatalogSnapshot(),
	})
}

func (s *Server) handleRedshiftStatements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]any{
		"statements": s.redshift.StatementSnapshots(),
	})
}

func (s *Server) handleRedshiftTable(w http.ResponseWriter, r *http.Request) {
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/redshift/tables/")
	if err != nil || len(parts) != 2 {
		http.Error(w, "invalid redshift table path", http.StatusBadRequest)
		return
	}
	limit, ok := positiveLimitFromRequest(w, r, 100)
	if !ok {
		return
	}
	detail, found := s.redshift.TableDetailSnapshot(parts[0], parts[1], limit)
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"schema":  parts[0],
		"table":   parts[1],
		"detail":  detail,
		"columns": detail.Columns,
		"rows":    detail.Rows,
	})
}

type redshiftDashboardQueryRequest struct {
	SQL     string `json:"sql"`
	MaxRows int    `json:"maxRows"`
}

func (s *Server) handleRedshiftQuery(w http.ResponseWriter, r *http.Request) {
	if s.redshift == nil {
		http.Error(w, "redshift service is disabled", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request redshiftDashboardQueryRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid redshift query request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.SQL) == "" {
		http.Error(w, "sql is required", http.StatusBadRequest)
		return
	}
	result, err := s.redshift.ExecuteDashboardSQL(request.SQL, request.MaxRows)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]any{
			"error": "redshift query failed",
		})
		return
	}
	writeJSON(w, map[string]any{
		"result": result,
	})
}

func (s *Server) handleBigQueryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	project := defaultString(s.config.BigQueryProject, "devcloud")
	location := defaultString(s.config.BigQueryLocation, "US")
	datasetCount := 0
	jobCount := 0
	if s.bq != nil {
		snapshot := s.bq.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		project = snapshot.Project
		location = snapshot.Location
		datasetCount = len(snapshot.Datasets)
		jobCount = len(snapshot.Jobs)
	}
	writeJSON(w, map[string]any{
		"service":      "bigquery",
		"status":       status,
		"running":      running,
		"endpoint":     defaultString(s.config.BigQueryEndpoint, "http://127.0.0.1:9050"),
		"project":      project,
		"location":     location,
		"authMode":     defaultString(s.config.BigQueryAuthMode, "relaxed"),
		"storagePath":  defaultString(s.config.BigQueryStoragePath, ".devcloud/data/bigquery"),
		"datasetCount": datasetCount,
		"jobCount":     jobCount,
	})
}

func (s *Server) handleBigQueryProjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.bq == nil {
		http.Error(w, "bigquery service is disabled", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.bq.Snapshot()
	writeJSON(w, map[string]any{
		"projects": []map[string]any{{
			"projectId":    snapshot.Project,
			"location":     snapshot.Location,
			"datasetCount": len(snapshot.Datasets),
			"jobCount":     len(snapshot.Jobs),
			"datasets":     snapshot.Datasets,
			"jobs":         snapshot.Jobs,
		}},
	})
}

func (s *Server) handleBigQueryProjectResource(w http.ResponseWriter, r *http.Request) {
	if s.bq == nil {
		http.Error(w, "bigquery service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/bigquery/projects/")
	if err != nil || len(parts) == 0 {
		http.Error(w, "invalid bigquery path", http.StatusBadRequest)
		return
	}
	projectID := parts[0]
	if len(parts) == 2 && parts[1] == "queries" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		s.forwardBigQueryQuery(w, r, projectID)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	switch {
	case len(parts) == 2 && parts[1] == "datasets":
		snapshot := s.bq.Snapshot()
		if snapshot.Project != projectID {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasets": snapshot.Datasets})
	case len(parts) == 4 && parts[1] == "datasets" && parts[3] == "tables":
		dataset, found := s.bq.DatasetSnapshot(projectID, parts[2])
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tables": dataset.Tables})
	case len(parts) == 5 && parts[1] == "datasets" && parts[3] == "tables":
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], 0)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "table": table})
	case len(parts) == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "schema":
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], 0)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "schema": table.Schema})
	case len(parts) == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "rows":
		limit, ok := positiveLimitFromRequest(w, r, 100)
		if !ok {
			return
		}
		table, found := s.bq.TableSnapshot(projectID, parts[2], parts[4], limit)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "datasetId": parts[2], "tableId": parts[4], "rows": table.Rows})
	case len(parts) == 2 && parts[1] == "jobs":
		snapshot := s.bq.Snapshot()
		if snapshot.Project != projectID {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "jobs": snapshot.Jobs})
	case len(parts) == 3 && parts[1] == "jobs":
		job, found := s.bq.JobSnapshot(projectID, parts[2])
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"projectId": projectID, "jobId": parts[2], "job": job})
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) forwardBigQueryQuery(w http.ResponseWriter, r *http.Request, projectID string) {
	queryURL := &url.URL{
		Path:     "/bigquery/v2/projects/" + url.PathEscape(projectID) + "/queries",
		RawQuery: r.URL.RawQuery,
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPost
	req.URL = queryURL
	req.RequestURI = ""
	req.Body = r.Body
	req.Header = r.Header.Clone()
	s.bq.ServeHTTP(w, req)
}

func (s *Server) handleSQSStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	region := defaultString(s.config.SQSRegion, "us-east-1")
	queueCount := 0
	if s.sqs != nil {
		snapshot := s.sqs.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		region = snapshot.Region
		queueCount = len(snapshot.Queues)
	}
	writeJSON(w, map[string]any{
		"service":     "sqs",
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.SQSEndpoint, "http://127.0.0.1:9324"),
		"region":      region,
		"authMode":    defaultString(s.config.SQSAuthMode, "relaxed"),
		"storagePath": defaultString(s.config.SQSStoragePath, ".devcloud/data/sqs"),
		"queueCount":  queueCount,
	})
}

func (s *Server) handleSQSQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.sqs == nil {
		http.Error(w, "sqs service is disabled", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.sqs.Snapshot()
	writeJSON(w, map[string]any{
		"queues": snapshot.Queues,
	})
}

func (s *Server) handleSQSQueue(w http.ResponseWriter, r *http.Request) {
	if s.sqs == nil {
		http.Error(w, "sqs service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/sqs/queues/")
	if err != nil {
		http.Error(w, "invalid sqs queue path", http.StatusBadRequest)
		return
	}
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	queueName := parts[0]
	detail, found := s.sqs.QueueDetailSnapshot(queueName)
	if !found {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, map[string]any{
			"queue": detail.Queue,
		})
		return
	}
	switch parts[1] {
	case "messages":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, map[string]any{
			"queueName": queueName,
			"messages":  detail.Messages,
		})
	case "leases":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		writeJSON(w, map[string]any{
			"queueName": queueName,
			"leases":    detail.Leases,
		})
	case "dlq":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		dlq, found := s.sqs.DeadLetterSnapshot(queueName)
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{
			"queueName":              queueName,
			"deadLetterQueue":        dlq.DeadLetterQueue,
			"deadLetterSourceQueues": dlq.DeadLetterSourceQueues,
		})
	case "purge":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		if !s.sqs.PurgeQueueByName(queueName) {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handlePubSubStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	project := defaultString(s.config.PubSubProject, "devcloud")
	topicCount := 0
	subscriptionCount := 0
	if s.pubsub != nil {
		snapshot := s.pubsub.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		project = snapshot.Project
		topicCount = len(snapshot.Topics)
		subscriptionCount = len(snapshot.Subscriptions)
	}
	writeJSON(w, map[string]any{
		"service":           "pubsub",
		"status":            status,
		"running":           running,
		"grpcEndpoint":      defaultString(s.config.PubSubGRPCEndpoint, "127.0.0.1:8085"),
		"restEndpoint":      defaultString(s.config.PubSubRESTEndpoint, "http://127.0.0.1:8086"),
		"project":           project,
		"storagePath":       defaultString(s.config.PubSubStoragePath, ".devcloud/data/pubsub"),
		"topicCount":        topicCount,
		"subscriptionCount": subscriptionCount,
	})
}

func (s *Server) handlePubSubProjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project := defaultString(s.config.PubSubProject, "devcloud")
	status := "disabled"
	running := false
	if s.pubsub != nil {
		snapshot := s.pubsub.Snapshot()
		project = snapshot.Project
		status = snapshot.Status
		running = snapshot.Running
	}
	writeJSON(w, map[string]any{
		"projects": []map[string]any{
			{
				"project": project,
				"status":  status,
				"running": running,
			},
		},
	})
}

func (s *Server) handlePubSubTopics(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.pubsub.Snapshot()
		writeJSON(w, map[string]any{
			"project": snapshot.Project,
			"topics":  snapshot.Topics,
		})
	case http.MethodPost:
		var request struct {
			TopicID string `json:"topicId"`
			Name    string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		topicID := dashboardPubSubResourceID(firstNonEmpty(request.TopicID, request.Name))
		if topicID == "" {
			http.Error(w, "topicId is required", http.StatusBadRequest)
			return
		}
		s.forwardPubSubTopicCreate(w, r, topicID)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handlePubSubTopic(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/topics/")
	if err != nil {
		http.Error(w, "invalid pubsub topic path", http.StatusBadRequest)
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "publish":
			s.forwardPubSubTopicAction(w, r, parts[0], "publish")
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	snapshot := s.pubsub.Snapshot()
	name := "projects/" + snapshot.Project + "/topics/" + parts[0]
	for _, topic := range snapshot.Topics {
		if topic.Name == name {
			writeJSON(w, map[string]any{
				"project": snapshot.Project,
				"topic":   topic,
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) forwardPubSubTopicAction(w http.ResponseWriter, r *http.Request, topicID string, action string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	s.forwardPubSubRequest(w, r, http.MethodPost, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/topics/"+url.PathEscape(topicID)+":"+action)
}

func (s *Server) forwardPubSubTopicCreate(w http.ResponseWriter, r *http.Request, topicID string) {
	s.forwardPubSubRequest(w, r, http.MethodPut, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/topics/"+url.PathEscape(topicID))
}

func (s *Server) pubSubProject() string {
	project := defaultString(s.config.PubSubProject, "devcloud")
	if s.pubsub != nil {
		project = s.pubsub.Snapshot().Project
	}
	return project
}

func (s *Server) forwardPubSubRequest(w http.ResponseWriter, r *http.Request, method string, path string) {
	req := r.Clone(r.Context())
	req.Method = method
	req.URL = &url.URL{
		Path:     path,
		RawQuery: r.URL.RawQuery,
	}
	req.RequestURI = ""
	req.Body = r.Body
	req.Header = r.Header.Clone()
	s.pubsub.ServeHTTP(w, req)
}

func (s *Server) handlePubSubSubscriptions(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.pubsub.Snapshot()
		writeJSON(w, map[string]any{
			"project":       snapshot.Project,
			"subscriptions": snapshot.Subscriptions,
		})
	case http.MethodPost:
		var request struct {
			SubscriptionID     string `json:"subscriptionId"`
			Name               string `json:"name"`
			TopicID            string `json:"topicId"`
			Topic              string `json:"topic"`
			AckDeadlineSeconds int    `json:"ackDeadlineSeconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		subscriptionID := dashboardPubSubResourceID(firstNonEmpty(request.SubscriptionID, request.Name))
		topicID := dashboardPubSubResourceID(firstNonEmpty(request.TopicID, request.Topic))
		if subscriptionID == "" || topicID == "" {
			http.Error(w, "subscriptionId and topicId are required", http.StatusBadRequest)
			return
		}
		s.forwardPubSubSubscriptionCreate(w, r, subscriptionID, topicID, request.AckDeadlineSeconds)
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handlePubSubSubscription(w http.ResponseWriter, r *http.Request) {
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/subscriptions/")
	if err != nil {
		http.Error(w, "invalid pubsub subscription path", http.StatusBadRequest)
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "pull":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "pull")
		case "ack":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "acknowledge")
		case "modifyAckDeadline":
			s.forwardPubSubSubscriptionAction(w, r, parts[0], "modifyAckDeadline")
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	snapshot := s.pubsub.Snapshot()
	name := "projects/" + snapshot.Project + "/subscriptions/" + parts[0]
	for _, subscription := range snapshot.Subscriptions {
		if subscription.Name == name {
			writeJSON(w, map[string]any{
				"project":      snapshot.Project,
				"subscription": subscription,
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) forwardPubSubSubscriptionAction(w http.ResponseWriter, r *http.Request, subscriptionID string, action string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	s.forwardPubSubRequest(w, r, http.MethodPost, "/v1/projects/"+url.PathEscape(s.pubSubProject())+"/subscriptions/"+url.PathEscape(subscriptionID)+":"+action)
}

func (s *Server) forwardPubSubSubscriptionCreate(w http.ResponseWriter, r *http.Request, subscriptionID string, topicID string, ackDeadlineSeconds int) {
	project := s.pubSubProject()
	body := map[string]any{
		"topic": "projects/" + project + "/topics/" + topicID,
	}
	if ackDeadlineSeconds > 0 {
		body["ackDeadlineSeconds"] = ackDeadlineSeconds
	}
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "invalid subscription request", http.StatusBadRequest)
		return
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPut
	req.URL = &url.URL{
		Path:     "/v1/projects/" + url.PathEscape(project) + "/subscriptions/" + url.PathEscape(subscriptionID),
		RawQuery: r.URL.RawQuery,
	}
	req.RequestURI = ""
	req.Body = io.NopCloser(strings.NewReader(string(data)))
	req.Header = r.Header.Clone()
	req.Header.Set("Content-Type", "application/json")
	s.pubsub.ServeHTTP(w, req)
}

func dashboardPubSubResourceID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "/") {
		parts := strings.Split(value, "/")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Server) handlePubSubMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.pubsub == nil {
		http.Error(w, "pubsub service is disabled", http.StatusServiceUnavailable)
		return
	}
	parts, err := dashboardPathParts(r.URL.EscapedPath(), "/api/pubsub/messages/")
	if err != nil {
		http.Error(w, "invalid pubsub message path", http.StatusBadRequest)
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	message, found := s.pubsub.MessageSnapshot(parts[0])
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"message": message,
	})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		result, err := s.store.List(r.Context(), mail.ListMessagesInput{Limit: 100})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, result)
	case http.MethodDelete:
		if err := s.store.DeleteAll(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleS3Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	if s.s3 != nil {
		status = "running"
		running = true
	}
	writeJSON(w, map[string]any{
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.S3Endpoint, "http://127.0.0.1:4566"),
		"region":      defaultString(s.config.S3Region, "us-east-1"),
		"authMode":    defaultString(s.config.S3AuthMode, "relaxed"),
		"storagePath": defaultString(s.config.S3StoragePath, ".devcloud/data/s3"),
	})
}

func (s *Server) handleS3Buckets(w http.ResponseWriter, r *http.Request) {
	if s.s3 == nil {
		http.Error(w, "s3 service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		buckets, err := s.s3.ListBuckets(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := struct {
			Buckets []s3BucketSummary `json:"buckets"`
		}{Buckets: make([]s3BucketSummary, 0, len(buckets))}
		for _, bucket := range buckets {
			objects, _, err := s.s3.ListObjects(r.Context(), bucket.Name, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response.Buckets = append(response.Buckets, s3BucketSummary{
				Name:         bucket.Name,
				CreationDate: bucket.CreatedAt,
				ObjectCount:  len(objects),
			})
		}
		writeJSON(w, response)
	case http.MethodPost:
		var request struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		bucket, created, err := s.s3.CreateBucket(r.Context(), request.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(s3BucketSummary{Name: bucket.Name, CreationDate: bucket.CreatedAt})
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleS3Bucket(w http.ResponseWriter, r *http.Request) {
	if s.s3 == nil {
		http.Error(w, "s3 service is disabled", http.StatusServiceUnavailable)
		return
	}
	bucketPath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/s3/buckets/")
	escapedBucket, suffix, ok := strings.Cut(bucketPath, "/")
	bucket, err := url.PathUnescape(escapedBucket)
	if err != nil {
		http.Error(w, "invalid bucket path", http.StatusBadRequest)
		return
	}
	if bucket == "" {
		http.NotFound(w, r)
		return
	}
	if !ok {
		s.handleS3BucketDetail(w, r, bucket)
		return
	}
	if suffix == "objects" {
		s.handleS3Objects(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "objects/") {
		s.handleS3ObjectDownload(w, r, bucket, strings.TrimPrefix(suffix, "objects/"))
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleS3BucketDetail(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		item, ok, err := s.s3.GetBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		objects, _, err := s.s3.ListObjects(r.Context(), bucket, "")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, s3BucketSummary{Name: item.Name, CreationDate: item.CreatedAt, ObjectCount: len(objects)})
	case http.MethodDelete:
		deleted, err := s.s3.DeleteBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if !deleted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleS3Objects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	objects, ok, err := s.s3.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	response := struct {
		Bucket  string            `json:"bucket"`
		Prefix  string            `json:"prefix"`
		Objects []s3ObjectSummary `json:"objects"`
	}{
		Bucket:  bucket,
		Prefix:  prefix,
		Objects: make([]s3ObjectSummary, 0, len(objects)),
	}
	for _, object := range objects {
		response.Objects = append(response.Objects, s3ObjectSummary{
			Key:          object.Key,
			Size:         object.Size,
			ETag:         object.ETag,
			ContentType:  object.ContentType,
			LastModified: object.LastModified,
			Metadata:     object.Metadata,
			S3URI:        "s3://" + bucket + "/" + object.Key,
			DownloadURL:  "/api/s3/buckets/" + url.PathEscape(bucket) + "/objects/" + url.PathEscape(object.Key) + "/download",
		})
	}
	writeJSON(w, response)
}

func (s *Server) handleS3ObjectDownload(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	escapedKey, ok := strings.CutSuffix(path, "/download")
	if !ok || escapedKey == "" {
		http.NotFound(w, r)
		return
	}
	key, err := url.PathUnescape(escapedKey)
	if err != nil {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	object, body, found, err := s.s3.GetObject(r.Context(), bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	contentType := object.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	} else {
		w.Header().Set("Content-Disposition", `attachment; filename="`+downloadFilename(key)+`"`)
	}
	for key, value := range object.Metadata {
		w.Header().Set("x-amz-meta-"+key, value)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) handleGCSStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	if s.gcs != nil {
		status = "running"
		running = true
	}
	writeJSON(w, map[string]any{
		"status":            status,
		"running":           running,
		"endpoint":          defaultString(s.config.GCSEndpoint, "http://127.0.0.1:4443"),
		"project":           defaultString(s.config.GCSProject, "devcloud"),
		"storagePath":       defaultString(s.config.GCSStoragePath, ".devcloud/data/s3"),
		"uploadSessionPath": defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions"),
	})
}

func (s *Server) handleDynamoDBStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	status := "disabled"
	running := false
	tableCount := 0
	if s.dynamo != nil {
		snapshot := s.dynamo.Snapshot()
		status = snapshot.Status
		running = snapshot.Running
		tableCount = len(snapshot.Tables)
	}
	writeJSON(w, map[string]any{
		"status":      status,
		"running":     running,
		"endpoint":    defaultString(s.config.DynamoDBEndpoint, "http://127.0.0.1:8000"),
		"region":      defaultString(s.config.DynamoDBRegion, "us-east-1"),
		"storagePath": defaultString(s.config.DynamoDBStoragePath, ".devcloud/data/dynamodb"),
		"tableCount":  tableCount,
	})
}

func (s *Server) handleDynamoDBTables(w http.ResponseWriter, r *http.Request) {
	if s.dynamo == nil {
		http.Error(w, "dynamodb service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		snapshot := s.dynamo.Snapshot()
		writeJSON(w, map[string]any{
			"tables": snapshot.Tables,
		})
	case http.MethodPost:
		s.forwardDynamoDBDashboardOperation(w, r, "CreateTable", "")
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleDynamoDBTable(w http.ResponseWriter, r *http.Request) {
	if s.dynamo == nil {
		http.Error(w, "dynamodb service is disabled", http.StatusServiceUnavailable)
		return
	}
	tablePath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/dynamodb/tables/")
	escapedTable, suffix, hasSuffix := strings.Cut(tablePath, "/")
	tableName, err := url.PathUnescape(escapedTable)
	if err != nil {
		http.Error(w, "invalid table path", http.StatusBadRequest)
		return
	}
	if tableName == "" {
		http.NotFound(w, r)
		return
	}
	if hasSuffix {
		switch suffix {
		case "items":
			if r.Method == http.MethodPost {
				s.forwardDynamoDBDashboardOperation(w, r, "PutItem", tableName)
				return
			}
		case "items/update":
			s.forwardDynamoDBDashboardOperation(w, r, "UpdateItem", tableName)
			return
		case "items/delete":
			s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, "DeleteItem", tableName, tableName)
			return
		case "ttl":
			if r.Method == http.MethodPost {
				s.forwardDynamoDBDashboardOperation(w, r, "UpdateTimeToLive", tableName)
				return
			}
		case "query":
			s.forwardDynamoDBDashboardOperation(w, r, "Query", tableName)
			return
		case "scan":
			s.forwardDynamoDBDashboardOperation(w, r, "Scan", tableName)
			return
		case "delete":
			s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, "DeleteTable", tableName, tableName)
			return
		}
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	table, found := s.dynamo.TableSnapshot(tableName)
	if !found {
		http.NotFound(w, r)
		return
	}
	if !hasSuffix {
		writeJSON(w, map[string]any{
			"table": table,
		})
		return
	}
	switch suffix {
	case "indexes":
		writeJSON(w, map[string]any{
			"tableName":              tableName,
			"globalSecondaryIndexes": table.GlobalSecondaryIndexes,
			"localSecondaryIndexes":  table.LocalSecondaryIndexes,
		})
		return
	case "ttl":
		writeJSON(w, map[string]any{
			"tableName":             tableName,
			"timeToLiveDescription": table.TimeToLiveDescription,
		})
		return
	case "streams":
		streamEnabled := table.StreamSpecification != nil && table.StreamSpecification.StreamEnabled
		writeJSON(w, map[string]any{
			"tableName":           tableName,
			"streamEnabled":       streamEnabled,
			"latestStreamArn":     table.LatestStreamArn,
			"latestStreamLabel":   table.LatestStreamLabel,
			"streamSpecification": table.StreamSpecification,
		})
		return
	case "items":
	default:
		http.NotFound(w, r)
		return
	}
	limit := 100
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	items, found := s.dynamo.TableItems(tableName, limit)
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"tableName": tableName,
		"items":     items,
	})
}

type dashboardDynamoDBOperationRequest struct {
	Input        json.RawMessage `json:"input"`
	Confirmation string          `json:"confirmation"`
}

func (s *Server) forwardDynamoDBDashboardOperation(w http.ResponseWriter, r *http.Request, operation string, tableName string) {
	s.forwardDynamoDBDashboardOperationWithConfirmation(w, r, operation, tableName, "")
}

func (s *Server) forwardDynamoDBDashboardOperationWithConfirmation(w http.ResponseWriter, r *http.Request, operation string, tableName string, requiredConfirmation string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	var request dashboardDynamoDBOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid json request", http.StatusBadRequest)
		return
	}
	if requiredConfirmation != "" && request.Confirmation != requiredConfirmation {
		http.Error(w, "confirmation must match table name", http.StatusBadRequest)
		return
	}
	input, err := normalizeDynamoDBDashboardInput(request.Input, tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := r.Clone(r.Context())
	req.Method = http.MethodPost
	req.URL = &url.URL{Path: "/"}
	req.RequestURI = ""
	req.Body = io.NopCloser(bytes.NewReader(input))
	req.ContentLength = int64(len(input))
	req.Header = make(http.Header)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+operation)
	s.dynamo.ServeHTTP(w, req)
}

func normalizeDynamoDBDashboardInput(raw json.RawMessage, tableName string) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("input is required")
	}
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, errors.New("input must be valid JSON")
	}
	if input == nil {
		return nil, errors.New("input must be a JSON object")
	}
	if tableName != "" {
		if existing, ok := input["TableName"]; ok {
			if existingName, ok := existing.(string); !ok || existingName != tableName {
				return nil, errors.New("input TableName must match the selected table")
			}
		} else {
			input["TableName"] = tableName
		}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, errors.New("input could not be encoded")
	}
	return encoded, nil
}

func (s *Server) handleGCSUploadSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	sessions, err := listGCSUploadSessions(defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, struct {
		Sessions []gcsUploadSessionSummary `json:"sessions"`
	}{Sessions: sessions})
}

func (s *Server) handleGCSUploadSession(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	id, ok := gcsUploadSessionIDFromPath(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		root := defaultString(s.config.GCSUploadSessionPath, ".devcloud/data/gcs/upload_sessions")
		if err := os.RemoveAll(filepath.Join(root, id)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "DELETE")
	}
}

func (s *Server) handleGCSBuckets(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		buckets, err := s.gcs.ListBuckets(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := struct {
			Buckets []gcsBucketSummary `json:"buckets"`
		}{Buckets: make([]gcsBucketSummary, 0, len(buckets))}
		for _, bucket := range buckets {
			objects, _, err := s.gcs.ListObjects(r.Context(), bucket.Name, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response.Buckets = append(response.Buckets, gcsBucketSummary{
				Name:        bucket.Name,
				TimeCreated: bucket.CreatedAt,
				ObjectCount: len(objects),
				GCSURI:      "gs://" + bucket.Name,
			})
		}
		writeJSON(w, response)
	case http.MethodPost:
		var request struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json request", http.StatusBadRequest)
			return
		}
		bucket, created, err := s.gcs.CreateBucket(r.Context(), request.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !created {
			http.Error(w, "bucket already exists", http.StatusConflict)
			return
		}
		writeJSONStatus(w, http.StatusCreated, gcsBucketSummary{Name: bucket.Name, TimeCreated: bucket.CreatedAt, GCSURI: "gs://" + bucket.Name})
	default:
		methodNotAllowed(w, "GET, POST")
	}
}

func (s *Server) handleGCSBucket(w http.ResponseWriter, r *http.Request) {
	if s.gcs == nil {
		http.Error(w, "gcs service is disabled", http.StatusServiceUnavailable)
		return
	}
	bucketPath := strings.TrimPrefix(r.URL.EscapedPath(), "/api/gcs/buckets/")
	escapedBucket, suffix, ok := strings.Cut(bucketPath, "/")
	bucket, err := url.PathUnescape(escapedBucket)
	if err != nil {
		http.Error(w, "invalid bucket path", http.StatusBadRequest)
		return
	}
	if bucket == "" {
		http.NotFound(w, r)
		return
	}
	if !ok {
		s.handleGCSBucketDetail(w, r, bucket)
		return
	}
	if suffix == "objects" {
		s.handleGCSObjects(w, r, bucket)
		return
	}
	if strings.HasPrefix(suffix, "objects/") {
		s.handleGCSObjectDownload(w, r, bucket, strings.TrimPrefix(suffix, "objects/"))
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleGCSBucketDetail(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		item, ok, err := s.gcs.GetBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		objects, _, err := s.gcs.ListObjects(r.Context(), bucket, "")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, gcsBucketSummary{Name: item.Name, TimeCreated: item.CreatedAt, ObjectCount: len(objects), GCSURI: "gs://" + item.Name})
	case http.MethodDelete:
		deleted, err := s.gcs.DeleteBucket(r.Context(), bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if !deleted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleGCSObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	prefix := r.URL.Query().Get("prefix")
	objects, ok, err := s.gcs.ListObjects(r.Context(), bucket, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	response := struct {
		Bucket  string             `json:"bucket"`
		Prefix  string             `json:"prefix"`
		Objects []gcsObjectSummary `json:"objects"`
	}{
		Bucket:  bucket,
		Prefix:  prefix,
		Objects: make([]gcsObjectSummary, 0, len(objects)),
	}
	for _, object := range objects {
		response.Objects = append(response.Objects, gcsObjectSummaryFromObject(bucket, object))
	}
	writeJSON(w, response)
}

func (s *Server) handleGCSObjectDownload(w http.ResponseWriter, r *http.Request, bucket string, path string) {
	escapedName, download := strings.CutSuffix(path, "/download")
	if download {
		s.handleGCSObjectMediaDownload(w, r, bucket, escapedName)
		return
	}

	name, err := url.PathUnescape(path)
	if err != nil || name == "" {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		object, _, found, err := s.gcs.GetObject(r.Context(), bucket, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, gcsObjectSummaryFromObject(bucket, object))
	case http.MethodDelete:
		deleted, err := s.gcs.DeleteObject(r.Context(), bucket, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !deleted {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, DELETE")
	}
}

func (s *Server) handleGCSObjectMediaDownload(w http.ResponseWriter, r *http.Request, bucket string, escapedName string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	if escapedName == "" {
		http.NotFound(w, r)
		return
	}
	name, err := url.PathUnescape(escapedName)
	if err != nil {
		http.Error(w, "invalid object path", http.StatusBadRequest)
		return
	}
	object, body, found, err := s.gcs.GetObject(r.Context(), bucket, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	contentType := object.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", object.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Disposition", `attachment; filename="`+downloadFilename(name)+`"`)
	for key, value := range object.Metadata {
		w.Header().Set("x-goog-meta-"+key, value)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	id, raw, ok := parseMessagePath(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if id == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if raw {
			rc, ok, err := s.store.GetRaw(r.Context(), id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				http.NotFound(w, r)
				return
			}
			defer rc.Close()
			w.Header().Set("Content-Type", "message/rfc822")
			io.Copy(w, rc)
			return
		}
		message, ok, err := s.store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, message)
	case http.MethodDelete:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		if err := s.store.Delete(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		if raw {
			methodNotAllowed(w, "GET")
			return
		}
		methodNotAllowed(w, "GET, DELETE")
	}
}

func parseMessagePath(path string) (id string, raw bool, ok bool) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", false, false
	}
	if strings.Contains(path, "/") {
		id, suffix, found := strings.Cut(path, "/")
		return id, suffix == "raw", found && suffix == "raw"
	}
	return path, false, true
}

func dashboardPathParts(escapedPath string, prefix string) ([]string, error) {
	suffix := strings.TrimPrefix(escapedPath, prefix)
	if suffix == escapedPath {
		return nil, nil
	}
	rawParts := strings.Split(strings.Trim(suffix, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		if raw == "" {
			continue
		}
		part, err := url.PathUnescape(raw)
		if err != nil {
			return nil, err
		}
		if part == "." || part == ".." || strings.ContainsAny(part, `/\`) {
			return nil, errors.New("invalid path segment")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func positiveLimitFromRequest(w http.ResponseWriter, r *http.Request, fallback int) (int, bool) {
	limit := fallback
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return 0, false
		}
		limit = parsed
	}
	return limit, true
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func downloadFilename(key string) string {
	name := key
	if index := strings.LastIndex(name, "/"); index >= 0 {
		name = name[index+1:]
	}
	if name == "" {
		return "object"
	}
	return strings.Map(func(r rune) rune {
		if r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			return '_'
		}
		return r
	}, name)
}

type s3BucketSummary struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creationDate"`
	ObjectCount  int       `json:"objectCount"`
}

type s3ObjectSummary struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"contentType"`
	LastModified time.Time         `json:"lastModified"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	S3URI        string            `json:"s3Uri"`
	DownloadURL  string            `json:"downloadUrl"`
}

type gcsBucketSummary struct {
	Name        string    `json:"name"`
	TimeCreated time.Time `json:"timeCreated"`
	ObjectCount int       `json:"objectCount"`
	GCSURI      string    `json:"gcsUri"`
}

type gcsObjectSummary struct {
	Name           string            `json:"name"`
	Size           int64             `json:"size"`
	ETag           string            `json:"etag"`
	ContentType    string            `json:"contentType"`
	CRC32C         string            `json:"crc32c,omitempty"`
	StorageClass   string            `json:"storageClass"`
	Updated        time.Time         `json:"updated"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Generation     string            `json:"generation"`
	Metageneration string            `json:"metageneration"`
	GCSURI         string            `json:"gcsUri"`
	DownloadURL    string            `json:"downloadUrl"`
}

type gcsUploadSessionSummary struct {
	ID            string    `json:"id"`
	Bucket        string    `json:"bucket"`
	Name          string    `json:"name"`
	ContentType   string    `json:"contentType,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	ReceivedBytes int64     `json:"receivedBytes"`
}

func normalizedMetageneration(object s3svc.Object) int64 {
	if object.Metageneration < 1 {
		return 1
	}
	return object.Metageneration
}

func gcsObjectSummaryFromObject(bucket string, object s3svc.Object) gcsObjectSummary {
	return gcsObjectSummary{
		Name:           object.Key,
		Size:           object.Size,
		ETag:           object.ETag,
		ContentType:    object.ContentType,
		CRC32C:         object.CRC32C,
		StorageClass:   "STANDARD",
		Updated:        object.LastModified,
		Metadata:       object.Metadata,
		Generation:     strconv.FormatInt(object.LastModified.UTC().UnixNano(), 10),
		Metageneration: strconv.FormatInt(normalizedMetageneration(object), 10),
		GCSURI:         "gs://" + bucket + "/" + object.Key,
		DownloadURL:    "/api/gcs/buckets/" + url.PathEscape(bucket) + "/objects/" + url.PathEscape(object.Key) + "/download",
	}
}

func listGCSUploadSessions(root string) ([]gcsUploadSessionSummary, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []gcsUploadSessionSummary{}, nil
		}
		return nil, err
	}
	sessions := make([]gcsUploadSessionSummary, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name(), "session.json"))
		if err != nil {
			continue
		}
		var session struct {
			Bucket        string    `json:"Bucket"`
			Name          string    `json:"Name"`
			ContentType   string    `json:"ContentType"`
			CreatedAt     time.Time `json:"CreatedAt"`
			ReceivedBytes int64     `json:"ReceivedBytes"`
		}
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}
		sessions = append(sessions, gcsUploadSessionSummary{
			ID:            entry.Name(),
			Bucket:        session.Bucket,
			Name:          session.Name,
			ContentType:   session.ContentType,
			CreatedAt:     session.CreatedAt,
			ReceivedBytes: session.ReceivedBytes,
		})
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].CreatedAt.Equal(sessions[j].CreatedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].CreatedAt.Before(sessions[j].CreatedAt)
	})
	return sessions, nil
}

func gcsUploadSessionIDFromPath(escapedPath string) (string, bool) {
	escapedID := strings.TrimPrefix(escapedPath, "/api/gcs/uploads/")
	if escapedID == "" || escapedID == escapedPath || strings.Contains(escapedID, "/") {
		return "", false
	}
	id, err := url.PathUnescape(escapedID)
	if err != nil || id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return "", false
	}
	return id, true
}
