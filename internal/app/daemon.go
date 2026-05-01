package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"devcloud/internal/dashboard"
	dynamodbsvc "devcloud/internal/services/dynamodb"
	gcssvc "devcloud/internal/services/gcs"
	"devcloud/internal/services/mail"
	s3svc "devcloud/internal/services/s3"
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
	}
	dashboardServer := dashboard.NewServer(dashboardConfig, store, s3Store, objectStoreForDashboard(d.config.Services.GCS.Enabled, objectStore))
	if d.config.Services.DynamoDB.Enabled {
		dashboardServer.SetDynamoDB(dynamoDBServer)
	}

	errCh := make(chan error, 5)
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
	go func() { errCh <- dashboardServer.Run(ctx) }()

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
