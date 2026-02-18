package server

import (
	"context"
	"net"
	"testing"

	"github.com/charmbracelet/ssh"
)

type fakeRateLimitSession struct {
	remote net.Addr
	writes []string
}

func (f *fakeRateLimitSession) User() string             { return "guest" }
func (f *fakeRateLimitSession) Context() context.Context { return context.Background() }
func (f *fakeRateLimitSession) SetValue(any, any)        {}
func (f *fakeRateLimitSession) Value(any) any            { return nil }
func (f *fakeRateLimitSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
}
func (f *fakeRateLimitSession) RemoteAddr() net.Addr { return f.remote }

func TestRateLimitMiddlewareThrottlesByIP(t *testing.T) {
	middleware := RateLimitMiddleware(60, 2)
	called := 0
	handler := middleware(func(ssh.Session) { called++ })

	session := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 2222}}
	handler(session)
	handler(session)
	handler(session)

	if called != 2 {
		t.Fatalf("handler calls = %d, want 2", called)
	}
	if len(session.writes) != 1 || session.writes[0] != "rate limit exceeded\n" {
		t.Fatalf("writes = %#v", session.writes)
	}
}

func TestRateLimitMiddlewareIsolatedPerIP(t *testing.T) {
	middleware := RateLimitMiddleware(60, 1)
	called := 0
	handler := middleware(func(ssh.Session) { called++ })

	a := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 1}}
	b := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.11"), Port: 1}}

	handler(a)
	handler(a)
	handler(b)

	if called != 2 {
		t.Fatalf("handler calls = %d, want 2", called)
	}
	if len(a.writes) != 1 {
		t.Fatalf("writes for session a = %#v, want one throttle write", a.writes)
	}
	if len(b.writes) != 0 {
		t.Fatalf("writes for session b = %#v, want none", b.writes)
	}
}

func TestRemoteIPFallbacks(t *testing.T) {
	session := &fakeRateLimitSession{}
	if got := remoteIP(session); got != "unknown" {
		t.Fatalf("remoteIP(nil) = %q, want unknown", got)
	}

	session.remote = testAddr("opaque")
	if got := remoteIP(session); got != "opaque" {
		t.Fatalf("remoteIP(opaque) = %q, want opaque", got)
	}
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }
