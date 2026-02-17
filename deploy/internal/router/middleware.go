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

const sessionContextKey contextKey = "mosaic.session"

// Descriptor keeps middleware metadata for deterministic startup wiring.
type Descriptor struct {
	Name       string
	Middleware wish.Middleware
}

// DefaultChain wires middleware in order: concurrency limiting, username routing, session metadata.
func DefaultChain(concurrencyLimit int) []Descriptor {
	return []Descriptor{
		{Name: "concurrency-limit", Middleware: concurrencyLimitMiddleware(concurrencyLimit)},
		{Name: "username-routing", Middleware: usernameRouting()},
		{Name: "session-metadata", Middleware: sessionMetadata()},
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

func concurrencyLimitMiddleware(limit int) wish.Middleware {
	var active int32
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)

			if int(current) > limit {
				log.Printf("level=warn event=concurrency_limit user=%s active=%d", s.User(), current)
				_, _ = s.Write([]byte("concurrency limit exceeded\n"))
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

func sessionMetadata() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			ctx := context.WithValue(s.Context(), sessionContextKey, s)
			s.SetContext(ctx)
			s.SetValue(string(sessionContextKey), map[string]any{
				"user": s.User(),
			})
			started := time.Now()
			log.Printf("level=info event=session_start user=%s", s.User())
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d", s.User(), time.Since(started).Milliseconds())
		}
	}
}
