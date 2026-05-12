package dashboard

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	bigquerysvc "devcloud/internal/services/bigquery"
	dynamodbsvc "devcloud/internal/services/dynamodb"
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
	mux.HandleFunc("/mail", redirectToDashboard("/dashboard/mail"))
	mux.HandleFunc("/s3", redirectToDashboard("/dashboard/s3"))
	mux.HandleFunc("/gcs", redirectToDashboard("/dashboard/gcs"))
	mux.HandleFunc("/dynamodb", redirectToDashboard("/dashboard/dynamodb"))
	mux.HandleFunc("/bigquery", redirectToDashboard("/dashboard/bigquery"))
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

func redirectToDashboard(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			methodNotAllowed(w, "GET, HEAD")
			return
		}
		http.Redirect(w, r, target, http.StatusMovedPermanently)
	}
}

func (s *Server) handleDashboardServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	writeJSON(w, dashboardServicesResponse{Services: s.dashboardServices()})
}
