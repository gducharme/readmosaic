package router

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

type contextKey string

const sessionContextKey contextKey = "session"

// Descriptor keeps middleware metadata for deterministic startup wiring.
type Descriptor struct {
	Name       string
	Middleware wish.Middleware
}

// DefaultChain wires middleware in order: rate limiting, username routing, session context.
func DefaultChain(rateLimitPerSec int) []Descriptor {
	return []Descriptor{
		{Name: "rate-limiting", Middleware: rateLimiting(rateLimitPerSec)},
		{Name: "username-routing", Middleware: usernameRouting()},
		{Name: "session-context", Middleware: sessionContext()},
	}
}

// MiddlewareFromDescriptors maps a descriptor chain to Wish middlewares.
func MiddlewareFromDescriptors(chain []Descriptor) []wish.Middleware {
	result := make([]wish.Middleware, 0, len(chain))
	for _, descriptor := range chain {
		result = append(result, descriptor.Middleware)
	}

	return result
}

func rateLimiting(limitPerSec int) wish.Middleware {
	var active int32
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)

			if int(current) > limitPerSec {
				log.Printf("level=warn event=rate_limit user=%s active=%d", s.User(), current)
				_, _ = s.Write([]byte("rate limit exceeded\n"))
				return
			}

			next(s)
		}
	}
}

func usernameRouting() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			log.Printf("level=info event=username_route user=%s", s.User())
			next(s)
		}
	}
}

func sessionContext() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			ctx := context.WithValue(s.Context(), sessionContextKey, s)
			s.SetValue(string(sessionContextKey), ctx)
			started := time.Now()
			log.Printf("level=info event=session_start user=%s", s.User())
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d", s.User(), time.Since(started).Milliseconds())
		}
	}
}
