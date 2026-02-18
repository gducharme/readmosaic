package router

import (
	"fmt"
	"log"
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

type policyRoute struct {
	Route  string
	Vector string
}

var identityPolicy = map[string]policyRoute{
	"west":    {Route: routeVector, Vector: "west"},
	"fitra":   {Route: routeVector, Vector: "fitra"},
	"root":    {Route: routeVector, Vector: "root"},
	"read":    {Route: routeTriage, Vector: routeTriage},
	"archive": {Route: routeTriage, Vector: routeTriage},
}

// Descriptor keeps middleware metadata for deterministic startup wiring.
type Descriptor struct {
	Name       string
	Middleware wish.Middleware
}

// DefaultChain wires middleware in order: username routing, session metadata.
func DefaultChain() []Descriptor {
	return []Descriptor{
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
//
// Contract: this value is populated by usernameRouting middleware. Callers should
// treat a false return value as "middleware has not run yet" (or rejected session).
func SessionIdentity(s ssh.Session) (Identity, bool) {
	identityValue := s.Context().Value(sessionIdentityKey)
	identity, ok := identityValue.(Identity)
	return identity, ok
}

// SessionMetadata returns immutable per-session metadata attached by middleware.
//
// Contract: this value is populated by sessionMetadata middleware. Callers should
// treat a false return value as "middleware has not run yet".
func SessionMetadata(s ssh.Session) (SessionInfo, bool) {
	infoValue := s.Context().Value(sessionMetadataKey)
	info, ok := infoValue.(SessionInfo)
	return info, ok
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

			s.Context().SetValue(sessionIdentityKey, identity)
			log.Printf("level=info event=username_route user=%s route=%s vector=%s session=%s", identity.Username, identity.Route, identity.Vector, sessionTraceID(s))
			next(s)
		}
	}
}

func sessionMetadata() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			identity, fromSession := SessionIdentity(s)
			if !fromSession {
				if resolvedIdentity, resolved := resolveIdentity(s.User()); resolved {
					identity = resolvedIdentity
				}
			}
			info := SessionInfo{User: s.User(), Identity: identity, StartedAt: time.Now()}
			s.Context().SetValue(sessionMetadataKey, info)

			route := identity.Route
			vector := identity.Vector
			if route == "" {
				route = "missing"
			}
			if vector == "" {
				vector = "missing"
			}

			log.Printf("level=info event=session_start user=%s route=%s vector=%s identity_present=%t session=%s", s.User(), route, vector, fromSession, sessionTraceID(s))
			next(s)
			log.Printf("level=info event=session_end user=%s duration_ms=%d identity_present=%t session=%s", s.User(), time.Since(info.StartedAt).Milliseconds(), fromSession, sessionTraceID(s))
		}
	}
}

func resolveIdentity(username string) (Identity, bool) {
	policy, ok := identityPolicy[username]
	if !ok {
		return Identity{}, false
	}

	return Identity{Username: username, Route: policy.Route, Vector: policy.Vector}, true
}

func SessionTraceID(s ssh.Session) string {
	return sessionTraceID(s)
}

func sessionTraceID(s ssh.Session) string {
	return fmt.Sprintf("%p", s)
}
