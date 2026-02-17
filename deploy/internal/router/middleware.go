package router

import (
	"context"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

type contextKey string

const sessionContextKey contextKey = "session"

// DefaultMiddleware wires the startup middleware chain in order:
// rate limiting, username routing, and session context.
func DefaultMiddleware() []wish.Middleware {
	return []wish.Middleware{
		rateLimiting(),
		usernameRouting(),
		sessionContext(),
	}
}

func rateLimiting() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			// Placeholder limiter hook.
			time.Sleep(0)
			next(s)
		}
	}
}

func usernameRouting() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			_ = s.User() // Placeholder for user-based route dispatch.
			next(s)
		}
	}
}

func sessionContext() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			ctx := context.WithValue(s.Context(), sessionContextKey, s)
			s.SetValue(string(sessionContextKey), ctx)
			next(s)
		}
	}
}
