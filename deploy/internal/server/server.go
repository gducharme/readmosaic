package server

import (
	"context"
	"fmt"
	"log"

	"github.com/charmbracelet/wish"

	"mosaic-terminal/internal/config"
)

func Build(cfg config.Config, middleware []wish.Middleware) (*wish.Server, error) {
	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	wishServer, err := wish.NewServer(
		wish.WithAddress(address),
		wish.WithMiddleware(middleware...),
	)
	if err != nil {
		return nil, err
	}

	return wishServer, nil
}

func Start(ctx context.Context, wishServer *wish.Server) error {
	go func() {
		<-ctx.Done()
		_ = wishServer.Shutdown(context.Background())
	}()

	log.Printf("starting wish ssh server on %s", wishServer.Addr)
	return wishServer.ListenAndServe()
}
