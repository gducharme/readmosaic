// Package wish provides a local shim for offline integration testing.
//
// Intentionally fake behavior:
//   - no SSH handshake/authentication
//   - host key file contains placeholder bytes, not a real private key
//   - sessions are plain TCP wrappers
//
// Contract we keep stable with upstream expectations:
//   - option-driven server construction
//   - middleware wrapping order
//   - ListenAndServe / Shutdown lifecycle and server-closed signaling
package wish

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/ssh"
)

var ErrServerClosed = net.ErrClosed

type Middleware func(ssh.Handler) ssh.Handler

type Option func(*Server)

type Server struct {
	Addr        string
	hostKeyPath string
	idleTimeout time.Duration
	maxSessions int
	handler     ssh.Handler
	middlewares []Middleware

	listener net.Listener
	mu       sync.Mutex
	closed   bool
	sem      chan struct{}
}

func NewServer(opts ...Option) (*Server, error) {
	s := &Server{handler: func(ssh.Session) {}}
	for _, opt := range opts {
		opt(s)
	}
	if s.Addr == "" {
		return nil, errors.New("address is required")
	}
	if s.maxSessions <= 0 {
		s.maxSessions = 16
	}
	s.sem = make(chan struct{}, s.maxSessions)
	if s.hostKeyPath != "" {
		if err := os.MkdirAll(filepath.Dir(s.hostKeyPath), 0o755); err != nil {
			return nil, fmt.Errorf("create host key dir: %w", err)
		}
		if _, err := os.Stat(s.hostKeyPath); errors.Is(err, os.ErrNotExist) {
			if writeErr := os.WriteFile(s.hostKeyPath, []byte("ephemeral-placeholder-host-key"), 0o600); writeErr != nil {
				return nil, fmt.Errorf("write host key: %w", writeErr)
			}
		}
	}
	return s, nil
}

func WithAddress(addr string) Option {
	return func(s *Server) {
		s.Addr = addr
	}
}

func WithHostKeyPath(path string) Option {
	return func(s *Server) {
		s.hostKeyPath = path
	}
}

func WithIdleTimeout(timeout time.Duration) Option {
	return func(s *Server) {
		s.idleTimeout = timeout
	}
}

func WithMaxSessions(max int) Option {
	return func(s *Server) {
		s.maxSessions = max
	}
}

func WithHandler(handler ssh.Handler) Option {
	return func(s *Server) {
		s.handler = handler
	}
}

func WithMiddleware(middleware ...Middleware) Option {
	return func(s *Server) {
		s.middlewares = append(s.middlewares, middleware...)
	}
}

func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Addr
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.closed = false
	s.Addr = ln.Addr().String()
	s.mu.Unlock()

	handler := s.handler
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		handler = s.middlewares[i](handler)
	}

	for {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return ErrServerClosed
			}
			return acceptErr
		}

		s.sem <- struct{}{}
		go func(c net.Conn) {
			defer func() {
				<-s.sem
				_ = c.Close()
			}()
			if s.idleTimeout > 0 {
				_ = c.SetDeadline(time.Now().Add(s.idleTimeout))
			}
			sess := &session{ctx: context.Background(), conn: c, values: map[string]any{}, user: "guest"}
			reader := bufio.NewReader(c)
			if line, lineErr := reader.ReadString('\n'); lineErr == nil && line != "" {
				sess.user = strings.TrimSpace(line)
			}
			handler(sess)
		}(conn)
	}
}

func (s *Server) Shutdown(context.Context) error {
	s.mu.Lock()
	s.closed = true
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

type session struct {
	ctx    context.Context
	conn   net.Conn
	values map[string]any
	user   string
}

func (s *session) User() string {
	return s.user
}

func (s *session) Context() context.Context {
	return s.ctx
}

func (s *session) SetValue(key string, value any) {
	s.values[key] = value
}

func (s *session) Write(p []byte) (n int, err error) {
	return s.conn.Write(p)
}
