package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
	"mosaic-terminal/internal/theme"
	"mosaic-terminal/internal/tui"
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
	)
	if err != nil {
		return nil, err
	}

	handler := ssh.Handler(defaultHandler)
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	wishServer.Handler = handler

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
				var releaseOnce sync.Once
				releaseSlot := func() {
					releaseOnce.Do(func() {
						<-sem
					})
				}

				go func() {
					<-s.Context().Done()
					releaseSlot()
				}()

				defer releaseSlot()
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
	acceptedAt := time.Now()
	traceID := router.SessionTraceID(s)
	user := s.User()
	route := "missing"
	vector := "missing"
	runtimeStart := time.Time{}
	exitCode := 0
	status := "rejected"

	defer func() {
		var duration int64
		if runtimeStart.IsZero() {
			duration = time.Since(acceptedAt).Milliseconds()
		} else {
			duration = time.Since(runtimeStart).Milliseconds()
		}
		log.Printf("level=info event=session_end user=%s route=%s vector=%s duration_ms=%d exit_code=%d status=%s session=%s", user, route, vector, duration, exitCode, status, traceID)
	}()

	pty, windowChanges, hasPTY := s.Pty()
	term := "missing"
	cols := 0
	rows := 0
	if hasPTY {
		term = pty.Term
		cols = pty.Window.Width
		rows = pty.Window.Height
	}
	log.Printf("level=info event=session_accepted user=%s remote_addr=%s pty_present=%t term=%s cols=%d rows=%d session=%s", user, s.RemoteAddr().String(), hasPTY, term, cols, rows, traceID)

	if !hasPTY {
		exitCode = 1
		status = "rejected"
		log.Printf("level=error event=session_rejected user=%s class=missing_pty session=%s", user, traceID)
		_, _ = s.Write([]byte("interactive terminal requires an attached PTY\n"))
		_ = s.Exit(1)
		return
	}

	identity, ok := router.SessionIdentity(s)
	if !ok {
		exitCode = 1
		status = "rejected"
		log.Printf("level=error event=session_rejected user=%s class=missing_identity session=%s", user, traceID)
		_, _ = s.Write([]byte("session identity unavailable; routing middleware is required\n"))
		_ = s.Exit(1)
		return
	}

	user = identity.Username
	route = identity.Route
	vector = identity.Vector

	flow, err := resolveFlow(identity)
	if err != nil {
		exitCode = 1
		status = "rejected"
		log.Printf("level=error event=session_rejected user=%s class=resolve_flow error=%v route=%s vector=%s session=%s", user, err, route, vector, traceID)
		_, _ = s.Write([]byte(err.Error() + "\n"))
		_ = s.Exit(1)
		return
	}

	runtimeStart = time.Now()
	status = "normal"
	log.Printf("level=info event=session_runtime_start user=%s route=%s vector=%s selected_flow=%s session=%s", user, route, vector, flow, traceID)

	width := pty.Window.Width
	height := pty.Window.Height
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}

	variant := theme.Variant(identity.Username)
	resolvedVariant, bundle, err := theme.ResolveFromEnv(variant, pty.Term)
	var themeBundle *theme.Bundle
	if err != nil {
		log.Printf("level=warn event=theme_resolve_failed user=%s route=%s vector=%s requested_variant=%s term=%q error=%v session=%s", user, route, vector, variant, pty.Term, err, traceID)
	} else {
		themeBundle = &bundle
		log.Printf("level=info event=theme_resolved user=%s route=%s vector=%s requested_variant=%s resolved_variant=%s term=%q session=%s", user, route, vector, variant, resolvedVariant, pty.Term, traceID)
	}

	model := tui.NewModelWithOptions(s.RemoteAddr().String(), tui.Options{
		Width:       width,
		Height:      height,
		IsTTY:       true,
		ThemeBundle: themeBundle,
	})

	switch flow {
	case "vector":
		model = model.Update(tui.AppendLineMsg{Line: fmt.Sprintf("VECTOR FLOW ACTIVE [%s]", identity.Username)})
	case "triage":
		model = model.Update(tui.AppendLineMsg{Line: fmt.Sprintf("TRIAGE FLOW ACTIVE [%s]", identity.Username)})
	}

	render := func() {
		_, _ = s.Write([]byte("\x1b[2J\x1b[H" + model.View() + "\n"))
	}
	render()

	keys := make(chan string, 8)
	eof := make(chan struct{}, 1)
	go streamKeys(s.Context(), s, keys, eof)

	statusTicker := time.NewTicker(safeTickerDuration(model.NextStatusTick(), 450*time.Millisecond))
	cursorTicker := time.NewTicker(safeTickerDuration(model.NextCursorTick(), 530*time.Millisecond))
	typewriterTicker := time.NewTicker(safeTickerDuration(model.NextTypewriterTick(), 32*time.Millisecond))
	defer statusTicker.Stop()
	defer cursorTicker.Stop()
	defer typewriterTicker.Stop()

	for {
		select {
		case <-s.Context().Done():
			exitCode = 0
			status = "disconnected"
			log.Printf("level=info event=session_disconnected user=%s route=%s vector=%s session=%s", user, route, vector, traceID)
			_ = s.CloseWrite()
			return
		case <-eof:
			_ = s.Exit(0)
			exitCode = 0
			status = "normal"
			return
		case key := <-keys:
			model = model.Update(tui.KeyMsg{Key: key})
			render()
			if key == "ctrl+d" {
				_ = s.Exit(0)
				exitCode = 0
				status = "normal"
				return
			}
		case win := <-windowChanges:
			model = model.Update(tui.ResizeMsg{Width: win.Width, Height: win.Height})
			render()
		case <-statusTicker.C:
			model = model.Update(tui.TickMsg{})
			render()
		case <-cursorTicker.C:
			model = model.Update(tui.CursorTickMsg{})
			render()
		case <-typewriterTicker.C:
			model = model.Update(tui.TypewriterTickMsg{})
			render()
		}
	}
}

func resolveFlow(identity router.Identity) (string, error) {
	switch strings.ToLower(identity.Username) {
	case "west", "fitra", "root":
		return "vector", nil
	case "read", "archive":
		return "triage", nil
	default:
		return "", fmt.Errorf("unsupported identity %q", identity.Username)
	}
}

// safeTickerDuration defends ticker creation from non-positive model cadence values.
func safeTickerDuration(candidate, fallback time.Duration) time.Duration {
	if candidate > 0 {
		return candidate
	}
	return fallback
}

func streamKeys(ctx context.Context, r io.Reader, keys chan<- string, eof chan<- struct{}) {
	reader := bufio.NewReader(r)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		b, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				select {
				case eof <- struct{}{}:
				default:
				}
			}
			return
		}

		var key string
		switch b {
		case '\r', '\n':
			key = "enter"
		case 0x04:
			key = "ctrl+d"
		case 0x7f, 0x08:
			key = "backspace"
		case 0x1b:
			// NOTE: ANSI escape sequences (e.g. arrow keys) are treated as plain ESC in this MVP decoder.
			key = "esc"
		default:
			if b >= 0x20 {
				key = string([]byte{b})
			}
		}

		if key == "" {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case keys <- key:
		default:
		}
	}
}
