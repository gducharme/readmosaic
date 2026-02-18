package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
	"mosaic-terminal/internal/server"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	runtime, err := server.New(cfg, router.DefaultChain())
	if err != nil {
		log.Fatalf("build ssh server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := runtime.Run(ctx); err != nil {
		log.Fatalf("run ssh server: %v", err)
	}
}
