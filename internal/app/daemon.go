package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"devcloud/internal/dashboard"
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
	if d.config.Services.S3.Enabled {
		s3Store = s3svc.NewFileBucketStore(filepath.Join(d.config.Storage.Path, "s3", "buckets"))
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
	dashboardConfig := dashboard.Config{
		Addr:          loopbackAddr(d.config.Server.DashboardPort),
		S3Endpoint:    "http://" + loopbackAddr(d.config.Server.S3Port),
		S3Region:      d.config.Services.S3.Region,
		S3AuthMode:    d.config.Auth.S3.Mode,
		S3StoragePath: filepath.Join(d.config.Storage.Path, "s3"),
	}
	var dashboardServer *dashboard.Server
	if d.config.Services.S3.Enabled {
		dashboardServer = dashboard.NewServer(dashboardConfig, store, s3Store)
	} else {
		dashboardServer = dashboard.NewServer(dashboardConfig, store)
	}

	errCh := make(chan error, 3)
	if d.config.Services.Mail.Enabled {
		go func() { errCh <- smtpServer.Run(ctx) }()
	}
	if d.config.Services.S3.Enabled {
		go func() { errCh <- s3Server.Run(ctx) }()
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
