package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"devcloud/internal/dashboard"
	"devcloud/internal/services/mail"
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

	smtpServer := mail.NewSMTPServer(mail.SMTPConfig{
		Addr:            loopbackAddr(d.config.Server.SMTPPort),
		MaxMessageBytes: d.config.Services.Mail.MaxMessageBytes,
	}, mailService)
	dashboardServer := dashboard.NewServer(dashboard.Config{
		Addr: loopbackAddr(d.config.Server.DashboardPort),
	}, store)

	errCh := make(chan error, 2)
	go func() { errCh <- smtpServer.Run(ctx) }()
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
