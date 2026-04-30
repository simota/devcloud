package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"devcloud/internal/app"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "help", "-h", "--help":
			fmt.Print("Usage:\n  devcloudd\n")
			return
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := app.LoadConfig(".devcloud/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	if err := app.NewDaemon(cfg).Run(ctx); err != nil {
		log.Fatal(err)
	}
}
