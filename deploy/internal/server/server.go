package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
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
	serverMode      = "wish"
)

// Runtime wires config + middleware + Wish server as a testable unit.
type Runtime struct {
	cfg           config.Config
	middlewareIDs []string
	server        *ssh.Server
	resolvedAddr  string
}

// New creates a runtime instance from config and middleware descriptors.
func New(cfg config.Config, chain []router.Descriptor) (*Runtime, error) {
	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	middleware := router.MiddlewareFromDescriptors(chain)
	// Order is security/resource critical:
	//   rate-limit -> max-sessions -> routing/metadata.
	// We throttle abusive clients before they can consume scarce session slots.
	middleware = append([]wish.Middleware{RateLimitMiddleware(cfg.RateLimitMaxAttempts, cfg.RateLimitWindow, cfg.RateLimitBurst, cfg.RateLimitBanDuration, cfg.RateLimitMaxTrackedIPs, cfg.RateLimitTrustProxyHeaders, cfg.RateLimitEnabled)}, middleware...)
	middleware = append([]wish.Middleware{MaxSessionsMiddleware(cfg.MaxSessions)}, middleware...)

	wishServer, err := wish.NewServer(
		wish.WithAddress(address),
		wish.WithHostKeyPath(cfg.HostKeyPath),
		wish.WithIdleTimeout(cfg.IdleTimeout),
		wish.WithMiddleware(middleware...),
	)
	if err != nil {
		return nil, err
	}

	wishServer.Handle(defaultHandler)

	ids := make([]string, 0, len(chain)+2)
	ids = append(ids, "rate-limit", "max-sessions")
	for _, descriptor := range chain {
		ids = append(ids, descriptor.Name)
	}

	return &Runtime{cfg: cfg, middlewareIDs: ids, server: wishServer, resolvedAddr: address}, nil
}

// MiddlewareIDs returns middleware names in configured execution order.
func (r *Runtime) MiddlewareIDs() []string {
	out := make([]string, len(r.middlewareIDs))
	copy(out, r.middlewareIDs)
	return out
}

// Address returns the configured (and once running, resolved) listener address.
func (r *Runtime) Address() string {
	return r.resolvedAddr
}

// Run starts the server and exits after shutdown or fatal listen errors.
func (r *Runtime) Run(ctx context.Context) error {
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	ln, err := net.Listen("tcp", r.server.Addr)
	if err != nil {
		return err
	}
	r.resolvedAddr = ln.Addr().String()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = r.server.Shutdown(shutdownCtx)
		_ = ln.Close()
	}()

	log.Printf("level=info event=startup version=%s mode=%s listen=%s middleware=%v max_sessions=%d", version, serverMode, r.resolvedAddr, r.middlewareIDs, r.cfg.MaxSessions)
	err = r.server.Serve(ln)
	if errors.Is(err, ssh.ErrServerClosed) || err == nil {
		<-shutdownDone
		return nil
	}

	<-shutdownDone
	return err
}

func MaxSessionsMiddleware(maxSessions int) wish.Middleware {
	if maxSessions <= 0 {
		maxSessions = 1
	}
	sem := make(chan struct{}, maxSessions)

	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			select {
			case sem <- struct{}{}:
				release := make(chan struct{})
				go func() {
					select {
					case <-s.Context().Done():
					case <-release:
					}
					<-sem
				}()

				defer close(release)
				defer func() {
					if recovered := recover(); recovered != nil {
						log.Printf("level=error event=max_sessions_handler_panic recovered=%v", recovered)
					}
				}()
				next(s)
			default:
				_, _ = s.Write([]byte("max sessions exceeded\n"))
				_ = s.Exit(1)
				_ = s.Close()
			}
		}
	}
}

func defaultHandler(s ssh.Session) {
	_, _ = s.Write([]byte("Mosaic terminal session ready\n"))
}
