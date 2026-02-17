package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
)

const version = "dev"

// Runtime wires config + middleware + Wish server as a testable unit.
type Runtime struct {
	cfg           config.Config
	middlewareIDs []string
	server        *wish.Server
}

func New(cfg config.Config, chain []router.Descriptor) (*Runtime, error) {
	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	middleware := router.MiddlewareFromDescriptors(chain)

	wishServer, err := wish.NewServer(
		wish.WithAddress(address),
		wish.WithHostKeyPath(cfg.HostKeyPath),
		wish.WithIdleTimeout(cfg.IdleTimeout),
		wish.WithMaxSessions(cfg.MaxSessions),
		wish.WithMiddleware(middleware...),
		wish.WithHandler(defaultHandler),
	)
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(chain))
	for _, descriptor := range chain {
		ids = append(ids, descriptor.Name)
	}

	return &Runtime{cfg: cfg, middlewareIDs: ids, server: wishServer}, nil
}

func (r *Runtime) MiddlewareIDs() []string {
	out := make([]string, len(r.middlewareIDs))
	copy(out, r.middlewareIDs)
	return out
}

func (r *Runtime) Address() string {
	return r.server.Addr
}

func (r *Runtime) Run(ctx context.Context) error {
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	go func() {
		<-ctx.Done()
		_ = r.server.Shutdown(context.Background())
	}()

	log.Printf("level=info event=startup version=%s host=%s port=%d middleware=%v host_key_path=%s idle_timeout=%s max_sessions=%d", version, r.cfg.Host, r.cfg.Port, r.middlewareIDs, r.cfg.HostKeyPath, r.cfg.IdleTimeout, r.cfg.MaxSessions)
	err := r.server.ListenAndServe()
	if errors.Is(err, wish.ErrServerClosed) || err == nil {
		return nil
	}

	return err
}

func defaultHandler(s ssh.Session) {
	_, _ = s.Write([]byte("Mosaic terminal session ready\n"))
}
