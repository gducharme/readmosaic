package router

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

type sessionKey string

const (
	sessionMetadataKey sessionKey = "mosaic.session"
	sessionIdentityKey sessionKey = "mosaic.identity"

	routeVector = "vector"
	routeTriage = "triage"
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

// IsVector reports whether this identity should route to vector-specific content/theme.
func (i Identity) IsVector() bool {
	return i.Route == routeVector
}

// IsTriage reports whether this identity should route to the shared triage flow.
func (i Identity) IsTriage() bool {
	return i.Route == routeTriage
}

// IsPrivileged reports whether this identity has privileged operator permissions.
func (i Identity) IsPrivileged() bool {
	return i.Username == "root"
}

var identityPolicy = map[string]Identity{
	"west":    {Username: "west", Route: routeVector, Vector: "west"},
	"fitra":   {Username: "fitra", Route: routeVector, Vector: "fitra"},
	"root":    {Username: "root", Route: routeVector, Vector: "root"},
	"read":    {Username: "read", Route: routeTriage, Vector: routeTriage},
	"archive": {Username: "archive", Route: routeTriage, Vector: routeTriage},
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

// SessionIdentity returns the resolved identity attached to this session.
func SessionIdentity(s ssh.Session) (Identity, bool) {
	identityValue := s.Value(sessionIdentityKey)
	identity, ok := identityValue.(Identity)
	return identity, ok
}

// SessionMetadata returns immutable per-session metadata attached by middleware.
func SessionMetadata(s ssh.Session) (SessionInfo, bool) {
	infoValue := s.Value(sessionMetadataKey)
	info, ok := infoValue.(SessionInfo)
	return info, ok
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
				log.Printf("level=warn event=username_rejected user=%q reason=unknown_identity session=%s", s.User(), sessionTraceID(s))
				_, _ = s.Write([]byte("SIGNAL UNRECOGNIZED. RETURN TO AGGREGATE.\n"))
				return
			}

			s.SetValue(sessionIdentityKey, identity)
			log.Printf("level=info event=username_route user=%s route=%s vector=%s session=%s", identity.Username, identity.Route, identity.Vector, sessionTraceID(s))
			next(s)
		}
	}
}

func sessionMetadata() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			identity, ok := SessionIdentity(s)
			if !ok {
				identity, _ = resolveIdentity(s.User())
			}
			info := SessionInfo{User: s.User(), Identity: identity, StartedAt: time.Now().UTC()}
			s.SetValue(sessionMetadataKey, info)

			log.Printf("level=info event=session_start user=%s route=%s vector=%s session=%s", s.User(), identity.Route, identity.Vector, sessionTraceID(s))
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d session=%s", s.User(), time.Since(info.StartedAt).Milliseconds(), sessionTraceID(s))
		}
	}
}

func resolveIdentity(username string) (Identity, bool) {
	identity, ok := identityPolicy[username]
	return identity, ok
}

func sessionTraceID(s ssh.Session) string {
	return fmt.Sprintf("%p", s)
}
