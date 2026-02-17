package main

import (
	"context"
	"log"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
	"mosaic-terminal/internal/server"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	runtime, err := server.New(cfg, router.DefaultChain(cfg.RateLimitPerSecond))
	if err != nil {
		log.Fatalf("build ssh server: %v", err)
	}

	if err := runtime.Run(context.Background()); err != nil {
		log.Fatalf("run ssh server: %v", err)
	}
}
