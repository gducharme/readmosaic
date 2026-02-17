package router

import (
	"log"
	"sync"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

const sessionMetadataKey = "mosaic.session"

// SessionInfo stores stable metadata for downstream consumers.
type SessionInfo struct {
	User      string
	StartedAt time.Time
}

// Descriptor keeps middleware metadata for deterministic startup wiring.
type Descriptor struct {
	Name       string
	Middleware wish.Middleware
}

// DefaultChain wires middleware in order: rate limiting, username routing, session metadata.
func DefaultChain(rateLimitPerSecond int) []Descriptor {
	return []Descriptor{
		{Name: "rate-limit", Middleware: rateLimitMiddleware(rateLimitPerSecond)},
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

func rateLimitMiddleware(limitPerSecond int) wish.Middleware {
	var mu sync.Mutex
	windowStart := time.Now()
	count := 0

	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			now := time.Now()

			mu.Lock()
			if now.Sub(windowStart) >= time.Second {
				windowStart = now
				count = 0
			}
			count++
			current := count
			mu.Unlock()

			if current > limitPerSecond {
				log.Printf("level=warn event=rate_limit user=%s count=%d window=1s", s.User(), current)
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

func sessionMetadata() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			info := SessionInfo{User: s.User(), StartedAt: time.Now().UTC()}
			s.SetValue(sessionMetadataKey, info)
			log.Printf("level=info event=session_start user=%s", s.User())
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d", s.User(), time.Since(info.StartedAt).Milliseconds())
		}
	}
}
