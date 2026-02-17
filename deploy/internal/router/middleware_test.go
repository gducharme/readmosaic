package router

import (
	"context"
	"strings"
	"testing"

	"github.com/charmbracelet/ssh"
)

type fakeSession struct {
	user   string
	ctx    context.Context
	values map[string]any
	writes []string
}

func newFakeSession(user string) *fakeSession {
	return &fakeSession{user: user, ctx: context.Background(), values: map[string]any{}}
}

func (f *fakeSession) User() string                   { return f.user }
func (f *fakeSession) Context() context.Context       { return f.ctx }
func (f *fakeSession) SetValue(key string, value any) { f.values[key] = value }
func (f *fakeSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
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

			identityValue, ok := s.values[sessionIdentityKey]
			if !ok {
				t.Fatalf("expected %q to be set", sessionIdentityKey)
			}

			identity, ok := identityValue.(Identity)
			if !ok {
				t.Fatalf("identity type = %T, want Identity", identityValue)
			}

			if identity.Route != "vector" || identity.Vector != user {
				t.Fatalf("identity = %+v, expected vector route for user %q", identity, user)
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

			identity := s.values[sessionIdentityKey].(Identity)
			if identity.Route != "triage" || identity.Vector != "triage" {
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

	infoValue, ok := s.values[sessionMetadataKey]
	if !ok {
		t.Fatalf("expected %q to be set", sessionMetadataKey)
	}

	info, ok := infoValue.(SessionInfo)
	if !ok {
		t.Fatalf("session info type = %T, want SessionInfo", infoValue)
	}

	if info.Identity.Username != "fitra" || info.Identity.Route != "vector" || info.Identity.Vector != "fitra" {
		t.Fatalf("info.Identity = %+v", info.Identity)
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

			if _, ok := s.values[sessionIdentityKey]; ok {
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

	if _, ok := s.values[sessionIdentityKey]; ok {
		t.Fatalf("identity metadata should not be set for rejected session")
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
