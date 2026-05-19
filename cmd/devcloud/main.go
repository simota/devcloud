package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"devcloud/internal/app"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}

	switch args[0] {
	case "init":
		cfg := app.DefaultConfig()
		return app.InitWorkspace(cfg)
	case "up":
		cfg, err := app.LoadConfig(".devcloud/config.yaml")
		if err != nil {
			return err
		}
		cfg, err = app.ApplyServiceSelection(cfg, args[1:])
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return app.NewDaemon(cfg).Run(ctx)
	case "reset":
		cfg, err := app.LoadConfig(".devcloud/config.yaml")
		if err != nil {
			return err
		}
		return app.ResetWorkspace(cfg)
	case "dashboard":
		cfg, err := app.LoadConfig(".devcloud/config.yaml")
		if err != nil {
			return err
		}
		fmt.Printf("Dashboard: http://localhost:%d\n", cfg.Server.DashboardPort)
		return nil
	case "help", "-h", "--help":
		return usage()
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usageText())
	}
}

func usage() error {
	fmt.Print(usageText())
	return nil
}

func usageText() string {
	return `Usage:
  devcloud init
  devcloud up [service ...]
  devcloud reset
  devcloud dashboard

When one or more service names are passed to "up", only those services are
started (overriding services.*.enabled in .devcloud/config.yaml). The
dashboard always starts.

Known services: ` + strings.Join(app.ServiceNames(), ", ") + `
`
}
