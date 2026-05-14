package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"devcloud/internal/dashboard"
	bigquerysvc "devcloud/internal/services/bigquery"
	dynamodbsvc "devcloud/internal/services/dynamodb"
	gcssvc "devcloud/internal/services/gcs"
	"devcloud/internal/services/mail"
	pubsubsvc "devcloud/internal/services/pubsub"
	redissvc "devcloud/internal/services/redis"
	redshiftsvc "devcloud/internal/services/redshift"
	"devcloud/internal/services/redshift/backend"
	redshiftpostgres "devcloud/internal/services/redshift/backend/postgres"
	"devcloud/internal/services/redshift/translator"
	s3svc "devcloud/internal/services/s3"
	sqssvc "devcloud/internal/services/sqs"
	"devcloud/internal/storage/blob"
	"devcloud/internal/storage/mailstore"
)

type Daemon struct {
	config Config
}

func NewDaemon(cfg Config) *Daemon {
	return &Daemon{config: cfg}
}

func (d *Daemon) Run(ctx context.Context) error {
	if err := InitWorkspace(d.config); err != nil {
		return err
	}

	blobStore := blob.NewFileStore(filepath.Join(d.config.Storage.Path, "blobs"))
	store := mailstore.NewFileStore(filepath.Join(d.config.Storage.Path, "mail"), blobStore)
	mailService := mail.NewService(store)
	var s3Store s3svc.BucketStore
	var objectStore s3svc.BucketStore
	if d.config.Services.S3.Enabled {
		s3Store = s3svc.NewFileBucketStore(filepath.Join(d.config.Storage.Path, "s3", "buckets"))
		objectStore = s3Store
	}
	if objectStore == nil && d.config.Services.GCS.Enabled {
		objectStore = s3svc.NewFileBucketStore(filepath.Join(d.config.Storage.Path, "s3", "buckets"))
	}

	smtpServer := mail.NewSMTPServer(mail.SMTPConfig{
		Addr:            loopbackAddr(d.config.Server.SMTPPort),
		MaxMessageBytes: d.config.Services.Mail.MaxMessageBytes,
		AuthMode:        d.config.Auth.SMTP.Mode,
		Username:        d.config.Auth.SMTP.Username,
		Password:        d.config.Auth.SMTP.Password,
	}, mailService)
	s3Server := s3svc.NewServer(s3svc.Config{
		Addr:            loopbackAddr(d.config.Server.S3Port),
		Region:          d.config.Services.S3.Region,
		MaxObjectBytes:  d.config.Services.S3.MaxObjectBytes,
		AuthMode:        d.config.Auth.S3.Mode,
		AccessKeyID:     d.config.Auth.S3.AccessKeyID,
		SecretAccessKey: d.config.Auth.S3.SecretAccessKey,
	}, s3Store)
	gcsServer := gcssvc.NewServer(gcssvc.Config{
		Addr:              loopbackAddr(d.config.Server.GCSPort),
		Project:           defaultString(d.config.Services.GCS.Project, d.config.Auth.GCS.Project),
		Location:          d.config.Services.GCS.Location,
		AuthMode:          d.config.Auth.GCS.Mode,
		BearerToken:       d.config.Auth.GCS.BearerToken,
		UploadSessionPath: filepath.Join(d.config.Storage.Path, "gcs", "upload_sessions"),
	}, objectStore)
	dynamoDBServer := dynamodbsvc.NewServer(dynamodbsvc.Config{
		Addr:            loopbackAddr(d.config.Server.DynamoDBPort),
		Region:          d.config.Services.DynamoDB.Region,
		AuthMode:        d.config.Auth.DynamoDB.Mode,
		AccessKeyID:     d.config.Auth.DynamoDB.AccessKeyID,
		SecretAccessKey: d.config.Auth.DynamoDB.SecretAccessKey,
		StoragePath:     filepath.Join(d.config.Storage.Path, "dynamodb"),
		MaxItemBytes:    d.config.Services.DynamoDB.MaxItemBytes,
		MaxTables:       d.config.Services.DynamoDB.MaxTables,
	})
	bigQueryServer := bigquerysvc.NewServer(bigquerysvc.Config{
		Addr:             loopbackAddr(d.config.Server.BigQueryPort),
		Project:          defaultString(d.config.Services.BigQuery.Project, d.config.Auth.BigQuery.Project),
		Location:         d.config.Services.BigQuery.Location,
		AuthMode:         d.config.Auth.BigQuery.Mode,
		BearerToken:      d.config.Auth.BigQuery.BearerToken,
		StoragePath:      filepath.Join(d.config.Storage.Path, "bigquery"),
		MaxRowsPerTable:  d.config.Services.BigQuery.MaxRowsPerTable,
		MaxRequestBytes:  d.config.Services.BigQuery.MaxRequestBytes,
		MaxResultRows:    d.config.Services.BigQuery.Query.MaxResultRows,
		DefaultLegacySQL: d.config.Services.BigQuery.Query.DefaultUseLegacySQL,
		ObjectStore:      objectStore,
	})
	sqsServer := sqssvc.NewServer(sqssvc.Config{
		Addr:                            loopbackAddr(d.config.Server.SQSPort),
		Region:                          d.config.Services.SQS.Region,
		AccountID:                       d.config.Auth.SQS.AccountID,
		QueueURLHost:                    d.config.Services.SQS.QueueURLHost,
		AuthMode:                        d.config.Auth.SQS.Mode,
		AccessKeyID:                     d.config.Auth.SQS.AccessKeyID,
		SecretAccessKey:                 d.config.Auth.SQS.SecretAccessKey,
		StoragePath:                     filepath.Join(d.config.Storage.Path, "sqs"),
		MaxQueues:                       d.config.Services.SQS.MaxQueues,
		MaxMessageBytes:                 d.config.Services.SQS.MaxMessageBytes,
		MaxReceiveBatchSize:             d.config.Services.SQS.MaxReceiveBatchSize,
		DefaultVisibilityTimeoutSeconds: d.config.Services.SQS.DefaultVisibilityTimeoutSeconds,
		DefaultDelaySeconds:             d.config.Services.SQS.DefaultDelaySeconds,
		DefaultMessageRetentionSeconds:  d.config.Services.SQS.DefaultMessageRetentionSeconds,
		DefaultReceiveWaitTimeSeconds:   d.config.Services.SQS.DefaultReceiveWaitTimeSeconds,
	})
	pubSubServer := pubsubsvc.NewServer(pubsubsvc.Config{
		GRPCAddr:                  loopbackAddr(d.config.Server.PubSubGRPCPort),
		RESTAddr:                  loopbackAddr(d.config.Server.PubSubRESTPort),
		Project:                   defaultString(d.config.Services.PubSub.Project, d.config.Auth.PubSub.ProjectID),
		AuthMode:                  d.config.Auth.PubSub.Mode,
		BearerToken:               d.config.Auth.PubSub.BearerToken,
		StoragePath:               pubsubDataDir(d.config),
		MessageStoragePath:        pubsubMessageDataDir(d.config),
		RESTEnabled:               d.config.Services.PubSub.EnableREST,
		DefaultAckDeadlineSeconds: d.config.Services.PubSub.DefaultAckDeadlineSeconds,
		MessageRetentionSeconds:   d.config.Services.PubSub.MessageRetentionSeconds,
		MaxAckDeadlineSeconds:     d.config.Services.PubSub.MaxAckDeadlineSeconds,
		MaxPullMessages:           d.config.Services.PubSub.MaxPullMessages,
		PullWaitTimeout:           time.Duration(d.config.Services.PubSub.PullWaitTimeoutSeconds) * time.Second,
		StreamingPullDisabled:     !d.config.Services.PubSub.EnableStreamingPull,
		EnablePush:                d.config.Services.PubSub.EnablePush,
	})
	var redshiftBackend backend.SQLBackend
	if d.config.Services.Redshift.Enabled {
		var err error
		redshiftBackend, err = redshiftSQLBackend(ctx, d.config)
		if err != nil {
			return err
		}
	}
	if redshiftBackend != nil {
		defer redshiftBackend.Close()
	}
	var redisLifecycle managedRedisLifecycle
	redisServiceEnabled := d.config.Services.Redis.Enabled
	if redisServiceEnabled {
		var err error
		redisLifecycle, err = startRedisBackend(ctx, d.config)
		if err != nil {
			if !errors.Is(err, errManagedRedisServerMissing) {
				return fmt.Errorf("start redis: %w", err)
			}
			fmt.Fprintf(os.Stderr, "devcloud: Redis disabled: %v\n", err)
			redisServiceEnabled = false
		}
	}
	if redisLifecycle != nil {
		defer redisLifecycle.Close()
	}
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           loopbackAddr(d.config.Server.RedshiftPort),
		APIAddr:           loopbackAddr(d.config.Server.RedshiftAPIPort),
		Region:            d.config.Services.Redshift.Region,
		ClusterIdentifier: d.config.Services.Redshift.ClusterIdentifier,
		Database:          d.config.Services.Redshift.Database,
		NodeType:          d.config.Services.Redshift.NodeType,
		NumberOfNodes:     d.config.Services.Redshift.NumberOfNodes,
		StoragePath:       redshiftDataDir(d.config),
		MaxStatementBytes: d.config.Services.Redshift.MaxStatementBytes,
		MaxCopyInputBytes: d.config.Services.Redshift.CopyUnload.MaxInputRowBytes,
		AuthMode:          d.config.Auth.Redshift.Mode,
		User:              d.config.Auth.Redshift.User,
		Password:          d.config.Auth.Redshift.Password,
		AccountID:         d.config.Auth.Redshift.AccountID,
		ObjectStore:       objectStore,
		SQLBackend:        redshiftBackend,
		BackendKind:       redshiftBackendKind(d.config.Services.Redshift.Backend),
		BackendMode:       redshiftBackendMode(d.config.Services.Redshift.Backend),
		Translator:        redshiftTranslator(d.config),
	})
	redisServer := redissvc.NewServer(redisServerConfig(d.config, redisLifecycle))
	dashboardConfig := dashboard.Config{
		Addr:                 loopbackAddr(d.config.Server.DashboardPort),
		MailDisabled:         !d.config.Services.Mail.Enabled,
		MailEndpoint:         "smtp://" + loopbackAddr(d.config.Server.SMTPPort),
		MailStoragePath:      filepath.Join(d.config.Storage.Path, "mail"),
		S3Endpoint:           "http://" + loopbackAddr(d.config.Server.S3Port),
		S3Region:             d.config.Services.S3.Region,
		S3AuthMode:           d.config.Auth.S3.Mode,
		S3StoragePath:        filepath.Join(d.config.Storage.Path, "s3"),
		GCSEndpoint:          "http://" + loopbackAddr(d.config.Server.GCSPort),
		GCSProject:           defaultString(d.config.Services.GCS.Project, d.config.Auth.GCS.Project),
		GCSStoragePath:       filepath.Join(d.config.Storage.Path, "s3"),
		GCSUploadSessionPath: filepath.Join(d.config.Storage.Path, "gcs", "upload_sessions"),
		DynamoDBEndpoint:     "http://" + loopbackAddr(d.config.Server.DynamoDBPort),
		DynamoDBRegion:       d.config.Services.DynamoDB.Region,
		DynamoDBStoragePath:  filepath.Join(d.config.Storage.Path, "dynamodb"),
		BigQueryEndpoint:     "http://" + loopbackAddr(d.config.Server.BigQueryPort),
		BigQueryProject:      defaultString(d.config.Services.BigQuery.Project, d.config.Auth.BigQuery.Project),
		BigQueryLocation:     d.config.Services.BigQuery.Location,
		BigQueryAuthMode:     d.config.Auth.BigQuery.Mode,
		BigQueryStoragePath:  filepath.Join(d.config.Storage.Path, "bigquery"),
		RedshiftSQLEndpoint:  loopbackAddr(d.config.Server.RedshiftPort),
		RedshiftAPIEndpoint:  "http://" + loopbackAddr(d.config.Server.RedshiftAPIPort),
		RedshiftRegion:       d.config.Services.Redshift.Region,
		RedshiftCluster:      d.config.Services.Redshift.ClusterIdentifier,
		RedshiftDatabase:     d.config.Services.Redshift.Database,
		RedshiftStoragePath:  redshiftDataDir(d.config),
		RedisEndpoint:        redisEndpointForDisplay(d.config),
		RedisStoragePath:     redisDataDir(d.config),
		RedisEnabled:         redisServiceEnabled,
		SQSEndpoint:          "http://" + loopbackAddr(d.config.Server.SQSPort),
		SQSRegion:            d.config.Services.SQS.Region,
		SQSAuthMode:          d.config.Auth.SQS.Mode,
		SQSStoragePath:       filepath.Join(d.config.Storage.Path, "sqs"),
		PubSubGRPCEndpoint:   loopbackAddr(d.config.Server.PubSubGRPCPort),
		PubSubRESTEndpoint:   "http://" + loopbackAddr(d.config.Server.PubSubRESTPort),
		PubSubProject:        defaultString(d.config.Services.PubSub.Project, d.config.Auth.PubSub.ProjectID),
		PubSubStoragePath:    pubsubDataDir(d.config),
	}
	dashboardServer := dashboard.NewServer(dashboardConfig, store, s3Store, objectStoreForDashboard(d.config.Services.GCS.Enabled, objectStore))
	if d.config.Services.DynamoDB.Enabled {
		dashboardServer.SetDynamoDB(dynamoDBServer)
	}
	if d.config.Services.BigQuery.Enabled {
		dashboardServer.SetBigQuery(bigQueryServer)
	}
	if d.config.Services.SQS.Enabled {
		dashboardServer.SetSQS(sqsServer)
	}
	if d.config.Services.PubSub.Enabled {
		dashboardServer.SetPubSub(pubSubServer)
	}
	if d.config.Services.Redshift.Enabled {
		dashboardServer.SetRedshift(redshiftServer)
	}
	if redisServiceEnabled {
		dashboardServer.SetRedis(redisServer)
	}

	errCh := make(chan error, d.enabledServerCount())
	if d.config.Services.Mail.Enabled {
		go func() { errCh <- smtpServer.Run(ctx) }()
	}
	if d.config.Services.S3.Enabled {
		go func() { errCh <- s3Server.Run(ctx) }()
	}
	if d.config.Services.GCS.Enabled {
		go func() { errCh <- gcsServer.Run(ctx) }()
	}
	if d.config.Services.DynamoDB.Enabled {
		go func() { errCh <- dynamoDBServer.Run(ctx) }()
	}
	if d.config.Services.BigQuery.Enabled {
		go func() { errCh <- bigQueryServer.Run(ctx) }()
	}
	if d.config.Services.SQS.Enabled {
		go func() { errCh <- sqsServer.Run(ctx) }()
	}
	if d.config.Services.PubSub.Enabled {
		go func() { errCh <- pubSubServer.Run(ctx) }()
	}
	if d.config.Services.Redshift.Enabled {
		go func() { errCh <- redshiftServer.Run(ctx) }()
	}
	if redisServiceEnabled {
		go func() { errCh <- redisServer.Run(ctx) }()
	}
	go func() { errCh <- dashboardServer.Run(ctx) }()

	endpointConfig := d.config
	endpointConfig.Services.Redis.Enabled = redisServiceEnabled
	printEndpoints(os.Stdout, endpointConfig)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

func loopbackAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func objectStoreForDashboard(enabled bool, store s3svc.BucketStore) s3svc.BucketStore {
	if !enabled {
		return nil
	}
	return store
}

func (d *Daemon) enabledServerCount() int {
	count := 1 // dashboard
	if d.config.Services.Mail.Enabled {
		count++
	}
	if d.config.Services.S3.Enabled {
		count++
	}
	if d.config.Services.GCS.Enabled {
		count++
	}
	if d.config.Services.DynamoDB.Enabled {
		count++
	}
	if d.config.Services.BigQuery.Enabled {
		count++
	}
	if d.config.Services.SQS.Enabled {
		count++
	}
	if d.config.Services.PubSub.Enabled {
		count++
	}
	if d.config.Services.Redshift.Enabled {
		count++
	}
	if d.config.Services.Redis.Enabled {
		count++
	}
	return count
}

func redshiftSQLBackend(ctx context.Context, cfg Config) (backend.SQLBackend, error) {
	return redshiftSQLBackendWithDriver(ctx, cfg, "")
}

func redshiftSQLBackendWithDriver(ctx context.Context, cfg Config, driverName string) (backend.SQLBackend, error) {
	backendCfg := cfg.Services.Redshift.Backend
	kind := redshiftBackendKind(backendCfg)
	switch kind {
	case "", "memory":
		return nil, nil
	case "postgres", "postgresql":
		mode := redshiftBackendMode(backendCfg)
		if mode == "managed" || (backendCfg.Managed && backendCfg.Mode == "" && backendCfg.ExternalDSN == "") {
			managed, err := startManagedRedshiftPostgres(ctx, cfg)
			if err != nil {
				return nil, err
			}
			pgBackend, err := redshiftpostgres.Open(ctx, redshiftpostgres.Config{
				DriverName: driverName,
				DSN:        managed.DSN(),
			})
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("open redshift postgres backend with managed dsn %s: %s", redactedManagedPostgresDSN(managed.DSN()), redactPostgresConnectionError(err, managed.DSN()))
			}
			return &managedRedshiftPostgresBackend{SQLBackend: pgBackend, managed: managed}, nil
		}
		if mode != "external" {
			return nil, fmt.Errorf("unsupported redshift postgres backend mode: %s", backendCfg.Mode)
		}
		pgBackend, err := redshiftpostgres.Open(ctx, redshiftpostgres.Config{
			DriverName: driverName,
			DSN:        backendCfg.ExternalDSN,
		})
		if err != nil {
			return nil, fmt.Errorf("open redshift postgres backend with external dsn %s: %s", redactedManagedPostgresDSN(backendCfg.ExternalDSN), redactPostgresConnectionError(err, backendCfg.ExternalDSN))
		}
		return pgBackend, nil
	default:
		return nil, fmt.Errorf("unsupported redshift backend kind: %s", backendCfg.Kind)
	}
}

type managedRedshiftPostgresBackend struct {
	backend.SQLBackend
	managed managedPostgresLifecycle
}

func (b *managedRedshiftPostgresBackend) Close() error {
	var closeErr error
	if b.SQLBackend != nil {
		closeErr = b.SQLBackend.Close()
	}
	if b.managed != nil {
		if err := b.managed.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func redshiftTranslator(cfg Config) translator.RedshiftTranslator {
	kind := redshiftBackendKind(cfg.Services.Redshift.Backend)
	switch kind {
	case "postgres", "postgresql":
		return translator.NewRedshiftToPostgres()
	default:
		return nil
	}
}

func redshiftBackendKind(cfg RedshiftBackendConfig) string {
	return strings.ToLower(defaultString(cfg.Kind, "postgres"))
}

func redshiftBackendMode(cfg RedshiftBackendConfig) string {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode != "" {
		return mode
	}
	if strings.TrimSpace(cfg.ExternalDSN) != "" {
		return "external"
	}
	if redshiftBackendKind(cfg) == "memory" {
		return "memory"
	}
	return "managed"
}

func startRedisBackend(ctx context.Context, cfg Config) (managedRedisLifecycle, error) {
	switch redisMode(cfg.Services.Redis) {
	case "managed":
		return startManagedRedisFromConfig(ctx, cfg)
	case "external":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported redis mode: %s", cfg.Services.Redis.Mode)
	}
}

func redisServerConfig(cfg Config, lifecycle managedRedisLifecycle) redissvc.Config {
	mode := redisMode(cfg.Services.Redis)
	addr := loopbackAddr(cfg.Server.RedisPort)
	if lifecycle != nil {
		addr = lifecycle.Addr()
	}
	return redissvc.Config{
		Mode:        mode,
		Addr:        addr,
		ExternalURL: cfg.Services.Redis.ExternalURL,
		AuthMode:    cfg.Auth.Redis.Mode,
		Password:    cfg.Auth.Redis.Password,
		DataDir:     redisDataDir(cfg),
	}
}

func redisMode(cfg RedisServiceConfig) string {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode != "" {
		return mode
	}
	if strings.TrimSpace(cfg.ExternalURL) != "" {
		return "external"
	}
	return "managed"
}

func pubsubDataDir(cfg Config) string {
	return defaultString(cfg.Services.PubSub.DataDir, filepath.Join(cfg.Storage.Path, "pubsub"))
}

func pubsubMessageDataDir(cfg Config) string {
	return defaultString(cfg.Services.PubSub.MessageDataDir, filepath.Join(cfg.Storage.Path, "message"))
}

func redshiftDataDir(cfg Config) string {
	dataDir := cfg.Services.Redshift.DataDir
	if dataDir == "" {
		return filepath.Join(cfg.Storage.Path, "redshift")
	}
	clean := filepath.Clean(dataDir)
	if clean == ".devcloud" || strings.HasPrefix(clean, ".devcloud"+string(filepath.Separator)) {
		return clean
	}
	return filepath.Join(cfg.Storage.Path, clean)
}

func redisDataDir(cfg Config) string {
	dataDir := cfg.Services.Redis.DataDir
	if dataDir == "" {
		return filepath.Join(cfg.Storage.Path, "redis")
	}
	clean := filepath.Clean(dataDir)
	if clean == ".devcloud" || strings.HasPrefix(clean, ".devcloud"+string(filepath.Separator)) {
		return clean
	}
	return filepath.Join(cfg.Storage.Path, clean)
}
