package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
)

const (
	version         = "dev"
	shutdownTimeout = 5 * time.Second
	serverMode      = "shim"
)

// Runtime wires config + middleware + Wish server as a testable unit.
type Runtime struct {
	cfg           config.Config
	middlewareIDs []string
	server        *wish.Server
}

// New creates a runtime instance from config and middleware descriptors.
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

// MiddlewareIDs returns middleware names in configured execution order.
func (r *Runtime) MiddlewareIDs() []string {
	out := make([]string, len(r.middlewareIDs))
	copy(out, r.middlewareIDs)
	return out
}

// Address returns the configured (and once running, resolved) listener address.
func (r *Runtime) Address() string {
	return r.server.Addr
}

// Run starts the server and exits after shutdown or fatal listen errors.
func (r *Runtime) Run(ctx context.Context) error {
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = r.server.Shutdown(shutdownCtx)
	}()

	log.Printf("level=info event=startup version=%s mode=%s listen=%s middleware=%v", version, serverMode, r.server.Addr, r.middlewareIDs)
	err := r.server.ListenAndServe()
	if errors.Is(err, wish.ErrServerClosed) || err == nil {
		<-shutdownDone
		return nil
	}

	<-shutdownDone
	return err
}

func defaultHandler(s ssh.Session) {
	_, _ = s.Write([]byte("Mosaic terminal session ready\n"))
}
