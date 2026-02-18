package router

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/charmbracelet/ssh"
)

type fakeContext struct {
	context.Context
	mu     sync.Mutex
	values map[any]any
	user   string
	remote net.Addr
	local  net.Addr
}

func newFakeContext(user string, remote net.Addr) *fakeContext {
	return &fakeContext{
		Context: context.Background(),
		values:  map[any]any{},
		user:    user,
		remote:  remote,
		local:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222},
	}
}

func (f *fakeContext) Lock()                         { f.mu.Lock() }
func (f *fakeContext) Unlock()                       { f.mu.Unlock() }
func (f *fakeContext) User() string                  { return f.user }
func (f *fakeContext) SessionID() string             { return "test-session" }
func (f *fakeContext) ClientVersion() string         { return "ssh-test-client" }
func (f *fakeContext) ServerVersion() string         { return "ssh-test-server" }
func (f *fakeContext) RemoteAddr() net.Addr          { return f.remote }
func (f *fakeContext) LocalAddr() net.Addr           { return f.local }
func (f *fakeContext) Permissions() *ssh.Permissions { return &ssh.Permissions{} }
func (f *fakeContext) SetValue(key, value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = value
}
func (f *fakeContext) Value(key interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

type fakeSession struct {
	user   string
	ctx    *fakeContext
	remote net.Addr
	writes []string
}

func newFakeSession(user string) *fakeSession {
	remote := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}
	return &fakeSession{user: user, ctx: newFakeContext(user, remote), remote: remote}
}

func (f *fakeSession) Read(_ []byte) (int, error) { return 0, io.EOF }
func (f *fakeSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
}
func (f *fakeSession) Close() error                                   { return nil }
func (f *fakeSession) CloseWrite() error                              { return nil }
func (f *fakeSession) SendRequest(string, bool, []byte) (bool, error) { return false, nil }
func (f *fakeSession) Stderr() io.ReadWriter                          { return &bytes.Buffer{} }
func (f *fakeSession) User() string                                   { return f.user }
func (f *fakeSession) RemoteAddr() net.Addr                           { return f.remote }
func (f *fakeSession) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222}
}
func (f *fakeSession) Environ() []string                       { return nil }
func (f *fakeSession) Exit(int) error                          { return nil }
func (f *fakeSession) Command() []string                       { return nil }
func (f *fakeSession) RawCommand() string                      { return "" }
func (f *fakeSession) Subsystem() string                       { return "" }
func (f *fakeSession) PublicKey() ssh.PublicKey                { return nil }
func (f *fakeSession) Context() ssh.Context                    { return f.ctx }
func (f *fakeSession) Permissions() ssh.Permissions            { return ssh.Permissions{} }
func (f *fakeSession) EmulatedPty() bool                       { return false }
func (f *fakeSession) Pty() (ssh.Pty, <-chan ssh.Window, bool) { return ssh.Pty{}, nil, false }
func (f *fakeSession) Signals(chan<- ssh.Signal)               {}
func (f *fakeSession) Break(chan<- bool)                       {}

func TestDefaultChainKeepsIdentityBeforeSessionMetadata(t *testing.T) {
	chain := DefaultChain()
	if len(chain) != 2 {
		t.Fatalf("chain length = %d, want 2", len(chain))
	}

	s := newFakeSession("west")
	middleware := MiddlewareFromDescriptors(chain)
	h := ssh.Handler(func(sess ssh.Session) {
		identity, ok := SessionIdentity(sess)
		if !ok {
			t.Fatalf("expected identity metadata before handler execution")
		}
		info, ok := SessionMetadata(sess)
		if !ok {
			t.Fatalf("expected session metadata before handler execution")
		}
		if info.Identity != identity {
			t.Fatalf("session metadata identity = %+v, want %+v", info.Identity, identity)
		}
	})
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	h(s)
}

func TestUsernameRoutingKnownVectorUsers(t *testing.T) {
	for _, user := range []string{"west", "fitra", "root"} {
		t.Run(user, func(t *testing.T) {
			s := newFakeSession(user)
			called := false

			h := usernameRouting()(func(ssh.Session) {
				called = true
			})
			h(s)

			if !called {
				t.Fatalf("expected next handler to be called")
			}

			identityValue, ok := s.Context().Value(sessionIdentityKey).(Identity)
			if !ok {
				t.Fatalf("expected %v to be set", sessionIdentityKey)
			}

			if identityValue.Route != routeVector || identityValue.Vector != user {
				t.Fatalf("identity = %+v, expected vector route for user %q", identityValue, user)
			}
		})
	}
}

func TestUsernameRoutingTriageUsers(t *testing.T) {
	for _, user := range []string{"read", "archive"} {
		t.Run(user, func(t *testing.T) {
			s := newFakeSession(user)
			called := false

			h := usernameRouting()(func(ssh.Session) {
				called = true
			})
			h(s)

			if !called {
				t.Fatalf("expected next handler to be called")
			}

			identity := s.Context().Value(sessionIdentityKey).(Identity)
			if identity.Route != routeTriage || identity.Vector != routeTriage {
				t.Fatalf("identity = %+v, want triage metadata", identity)
			}
		})
	}
}

func TestUsernameRoutingUnknownUserTerminatesSession(t *testing.T) {
	s := newFakeSession("WEST")
	called := false

	h := usernameRouting()(func(ssh.Session) {
		called = true
	})
	h(s)

	if called {
		t.Fatalf("unexpected next handler call for unknown username")
	}

	if len(s.writes) != 1 || s.writes[0] != "SIGNAL UNRECOGNIZED. RETURN TO AGGREGATE.\n" {
		t.Fatalf("writes = %#v", s.writes)
	}

	if _, ok := SessionMetadata(s); ok {
		t.Fatalf("session metadata should not be set for rejected user")
	}
}

func TestSessionMetadataStoresIdentity(t *testing.T) {
	s := newFakeSession("fitra")
	called := false

	h := sessionMetadata()(func(ssh.Session) {
		called = true
	})
	h(s)

	if !called {
		t.Fatalf("expected next handler to be called")
	}

	infoValue, ok := s.Context().Value(sessionMetadataKey).(SessionInfo)
	if !ok {
		t.Fatalf("expected %v to be set", sessionMetadataKey)
	}

	if infoValue.Identity.Username != "fitra" || infoValue.Identity.Route != routeVector || infoValue.Identity.Vector != "fitra" {
		t.Fatalf("info.Identity = %+v", infoValue.Identity)
	}
}

func TestIdentityPolicyCoverage(t *testing.T) {
	if len(identityPolicy) != 5 {
		t.Fatalf("identityPolicy size = %d, want 5", len(identityPolicy))
	}
	for _, user := range []string{"west", "fitra", "root", "read", "archive"} {
		if _, ok := identityPolicy[user]; !ok {
			t.Fatalf("identityPolicy missing user %q", user)
		}
	}
}

func TestUsernameRoutingRejectsSecurityBoundaryEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		user string
	}{
		{name: "different casing", user: "West"},
		{name: "leading whitespace", user: " west"},
		{name: "trailing whitespace", user: "west "},
		{name: "utf8 homoglyph", user: "we\u0455t"},
		{name: "empty username", user: ""},
		{name: "very long username", user: strings.Repeat("west", 128)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeSession(tc.user)
			called := false

			h := usernameRouting()(func(ssh.Session) {
				called = true
			})
			h(s)

			if called {
				t.Fatalf("unexpected next handler call for user %q", tc.user)
			}

			if _, ok := s.Context().Value(sessionIdentityKey).(Identity); ok {
				t.Fatalf("identity metadata should not be set for rejected user %q", tc.user)
			}

			if len(s.writes) != 1 || s.writes[0] != "SIGNAL UNRECOGNIZED. RETURN TO AGGREGATE.\n" {
				t.Fatalf("writes = %#v", s.writes)
			}
		})
	}
}

func TestUsernameRoutingRepeatedRejectionDoesNotBypass(t *testing.T) {
	s := newFakeSession("WEST")
	called := 0

	h := usernameRouting()(func(ssh.Session) {
		called++
	})

	h(s)
	h(s)

	if called != 0 {
		t.Fatalf("unexpected next handler calls = %d", called)
	}

	if _, ok := s.Context().Value(sessionIdentityKey).(Identity); ok {
		t.Fatalf("identity metadata should not be set for rejected session")
	}
	if _, ok := SessionMetadata(s); ok {
		t.Fatalf("session metadata should not be set for rejected session")
	}

	if len(s.writes) != 2 {
		t.Fatalf("writes length = %d, want 2", len(s.writes))
	}
	for _, write := range s.writes {
		if write != "SIGNAL UNRECOGNIZED. RETURN TO AGGREGATE.\n" {
			t.Fatalf("unexpected write %q", write)
		}
	}
}

func TestIdentityHelpers(t *testing.T) {
	root := Identity{Username: "root", Route: routeVector, Vector: "root"}
	if !root.IsVector() || root.IsTriage() || !root.IsPrivileged() {
		t.Fatalf("unexpected helper classification for root identity: %+v", root)
	}

	triage := Identity{Username: "archive", Route: routeTriage, Vector: routeTriage}
	if triage.IsVector() || !triage.IsTriage() || triage.IsPrivileged() {
		t.Fatalf("unexpected helper classification for triage identity: %+v", triage)
	}
}

func TestSessionAccessors(t *testing.T) {
	s := newFakeSession("west")
	identity := Identity{Username: "west", Route: routeVector, Vector: "west"}
	info := SessionInfo{User: "west", Identity: identity}
	s.Context().SetValue(sessionIdentityKey, identity)
	s.Context().SetValue(sessionMetadataKey, info)

	gotIdentity, ok := SessionIdentity(s)
	if !ok || gotIdentity != identity {
		t.Fatalf("SessionIdentity() = (%+v,%v), want (%+v,true)", gotIdentity, ok, identity)
	}

	gotInfo, ok := SessionMetadata(s)
	if !ok || gotInfo != info {
		t.Fatalf("SessionMetadata() = (%+v,%v), want (%+v,true)", gotInfo, ok, info)
	}
}
