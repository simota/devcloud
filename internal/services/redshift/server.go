package redshift

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"devcloud/internal/events"
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
	eventPublisher   events.Publisher
}

func (s *Server) SetEventPublisher(p events.Publisher) {
	s.eventPublisher = p
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
