package main

import (
	"context"
	"log"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
	"mosaic-terminal/internal/server"
)

func main() {
	cfg := config.LoadFromEnv()

	wishServer, err := server.Build(cfg, router.DefaultMiddleware())
	if err != nil {
		log.Fatalf("build ssh server: %v", err)
	}

	if err := server.Start(context.Background(), wishServer); err != nil {
		log.Fatalf("start ssh server: %v", err)
	}
}
