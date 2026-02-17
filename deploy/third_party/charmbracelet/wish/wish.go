package wish

import (
	"context"
	"errors"

	"github.com/charmbracelet/ssh"
)

type Middleware func(ssh.Handler) ssh.Handler

type Option func(*Server)

type Server struct {
	Addr        string
	middlewares []Middleware
}

func NewServer(opts ...Option) (*Server, error) {
	s := &Server{}
	for _, opt := range opts {
		opt(s)
	}
	if s.Addr == "" {
		return nil, errors.New("address is required")
	}
	return s, nil
}

func WithAddress(addr string) Option {
	return func(s *Server) {
		s.Addr = addr
	}
}

func WithMiddleware(middleware ...Middleware) Option {
	return func(s *Server) {
		s.middlewares = append(s.middlewares, middleware...)
	}
}

func (s *Server) ListenAndServe() error {
	return nil
}

func (s *Server) Shutdown(context.Context) error {
	return nil
}
