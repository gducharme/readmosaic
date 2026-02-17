package router

import (
	"log"
	"sync"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

const (
	sessionMetadataKey = "mosaic.session"
	sessionIdentityKey = "mosaic.identity"
)

// SessionInfo stores stable metadata for downstream consumers.
type SessionInfo struct {
	User      string
	Identity  Identity
	StartedAt time.Time
}

// Identity describes the authenticated user and routing vector selected for the session.
type Identity struct {
	Username string
	Route    string
	Vector   string
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
			identity, ok := resolveIdentity(s.User())
			if !ok {
				log.Printf("level=warn event=username_rejected user=%s", s.User())
				_, _ = s.Write([]byte("SIGNAL UNRECOGNIZED. RETURN TO AGGREGATE.\n"))
				return
			}

			s.SetValue(sessionIdentityKey, identity)
			log.Printf("level=info event=username_route user=%s route=%s vector=%s", identity.Username, identity.Route, identity.Vector)
			next(s)
		}
	}
}

func sessionMetadata() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			identity, _ := resolveIdentity(s.User())
			info := SessionInfo{User: s.User(), Identity: identity, StartedAt: time.Now().UTC()}
			s.SetValue(sessionMetadataKey, info)
			if identity.Username != "" {
				s.SetValue(sessionIdentityKey, identity)
			}

			log.Printf("level=info event=session_start user=%s route=%s vector=%s", s.User(), identity.Route, identity.Vector)
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d", s.User(), time.Since(info.StartedAt).Milliseconds())
		}
	}
}

func resolveIdentity(username string) (Identity, bool) {
	switch username {
	case "west", "fitra", "root":
		return Identity{Username: username, Route: "vector", Vector: username}, true
	case "read", "archive":
		return Identity{Username: username, Route: "triage", Vector: "triage"}, true
	default:
		return Identity{}, false
	}
}
