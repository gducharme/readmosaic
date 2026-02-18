package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/ssh"
)

type fakeRateLimitContext struct {
	context.Context
	mu     sync.Mutex
	values map[any]any
	remote net.Addr
	local  net.Addr
}

func newFakeRateLimitContext(remote net.Addr) *fakeRateLimitContext {
	return &fakeRateLimitContext{
		Context: context.Background(),
		values:  map[any]any{},
		remote:  remote,
		local:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222},
	}
}

func (f *fakeRateLimitContext) Lock()                         { f.mu.Lock() }
func (f *fakeRateLimitContext) Unlock()                       { f.mu.Unlock() }
func (f *fakeRateLimitContext) User() string                  { return "guest" }
func (f *fakeRateLimitContext) SessionID() string             { return "test-session" }
func (f *fakeRateLimitContext) ClientVersion() string         { return "ssh-test-client" }
func (f *fakeRateLimitContext) ServerVersion() string         { return "ssh-test-server" }
func (f *fakeRateLimitContext) RemoteAddr() net.Addr          { return f.remote }
func (f *fakeRateLimitContext) LocalAddr() net.Addr           { return f.local }
func (f *fakeRateLimitContext) Permissions() *ssh.Permissions { return &ssh.Permissions{} }
func (f *fakeRateLimitContext) SetValue(key, value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = value
}
func (f *fakeRateLimitContext) Value(key interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

type fakeRateLimitSession struct {
	remote net.Addr
	ctx    *fakeRateLimitContext
	writes []string
}

func newFakeRateLimitSession(remote net.Addr) *fakeRateLimitSession {
	return &fakeRateLimitSession{remote: remote, ctx: newFakeRateLimitContext(remote)}
}

func (f *fakeRateLimitSession) Read(_ []byte) (int, error) { return 0, io.EOF }
func (f *fakeRateLimitSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
}
func (f *fakeRateLimitSession) Close() error                                   { return nil }
func (f *fakeRateLimitSession) CloseWrite() error                              { return nil }
func (f *fakeRateLimitSession) SendRequest(string, bool, []byte) (bool, error) { return false, nil }
func (f *fakeRateLimitSession) Stderr() io.ReadWriter                          { return &bytes.Buffer{} }
func (f *fakeRateLimitSession) User() string                                   { return "guest" }
func (f *fakeRateLimitSession) RemoteAddr() net.Addr                           { return f.remote }
func (f *fakeRateLimitSession) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222}
}
func (f *fakeRateLimitSession) Environ() []string                       { return nil }
func (f *fakeRateLimitSession) Exit(int) error                          { return nil }
func (f *fakeRateLimitSession) Command() []string                       { return nil }
func (f *fakeRateLimitSession) RawCommand() string                      { return "" }
func (f *fakeRateLimitSession) Subsystem() string                       { return "" }
func (f *fakeRateLimitSession) PublicKey() ssh.PublicKey                { return nil }
func (f *fakeRateLimitSession) Context() ssh.Context                    { return f.ctx }
func (f *fakeRateLimitSession) Permissions() ssh.Permissions            { return ssh.Permissions{} }
func (f *fakeRateLimitSession) EmulatedPty() bool                       { return false }
func (f *fakeRateLimitSession) Pty() (ssh.Pty, <-chan ssh.Window, bool) { return ssh.Pty{}, nil, false }
func (f *fakeRateLimitSession) Signals(chan<- ssh.Signal)               {}
func (f *fakeRateLimitSession) Break(chan<- bool)                       {}

func TestRateLimitMiddlewareRapidConnectionsFromSameIP(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 2, enabled: true, maxTrackedIPs: 100})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })
	session := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 2222})

	handler(session)
	handler(session)
	handler(session)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
	if len(session.writes) != 1 || session.writes[0] != "rate limit exceeded\n" {
		t.Fatalf("writes = %#v", session.writes)
	}
}

func TestRateLimitMiddlewareManyIPsIndependentBuckets(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 1, enabled: true, maxTrackedIPs: 200})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })

	for i := 1; i <= 50; i++ {
		s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113." + itoa(i)), Port: 22})
		handler(s)
	}
	if handlerCalls != 50 {
		t.Fatalf("handler calls = %d, want 50", handlerCalls)
	}
}

func TestRateLimitMiddlewareLimitResetsAfterWindow(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	l := newRateLimiter(rateLimiterOptions{
		maxAttempts:   60,
		window:        time.Minute,
		burst:         1,
		enabled:       true,
		maxTrackedIPs: 100,
		now:           func() time.Time { return now },
	})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })
	s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.44"), Port: 22})

	handler(s)
	handler(s)
	now = now.Add(time.Minute)
	handler(s)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
}

func TestExtractRemoteIPNormalizesIPv6AndIPv4(t *testing.T) {
	ipv4Mapped := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("::ffff:203.0.113.9"), Port: 22})
	if got := extractRemoteIP(ipv4Mapped, false); got != "203.0.113.9" {
		t.Fatalf("extractRemoteIP(mapped) = %q", got)
	}

	ipv6 := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("2001:db8::10"), Port: 22})
	if got := extractRemoteIP(ipv6, false); got != "2001:db8::10" {
		t.Fatalf("extractRemoteIP(v6) = %q", got)
	}
}

func TestRateLimitMiddlewareSharedNATSharesLimit(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 2, enabled: true, maxTrackedIPs: 100})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })

	a := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("198.51.100.50"), Port: 1111})
	b := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("198.51.100.50"), Port: 2222})

	handler(a)
	handler(b)
	handler(a)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
}

func TestRateLimitMiddlewareConnectionReuseCountsPerAttempt(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 1, enabled: true, maxTrackedIPs: 100})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })

	s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.90"), Port: 4444})
	handler(s)
	handler(s)

	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
}

func TestRateLimitMiddlewareClockSkewBackwardsDoesNotCreateTokens(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	l := newRateLimiter(rateLimiterOptions{
		maxAttempts:   60,
		window:        time.Minute,
		burst:         1,
		enabled:       true,
		maxTrackedIPs: 100,
		now:           func() time.Time { return now },
	})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })
	s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.91"), Port: 22})

	handler(s)
	now = now.Add(-10 * time.Second)
	handler(s)

	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
}

func TestRateLimitMiddlewareCleanupPreventsUnboundedMapGrowth(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	l := newRateLimiter(rateLimiterOptions{
		maxAttempts:   60,
		window:        time.Second,
		burst:         1,
		enabled:       true,
		maxTrackedIPs: 100,
		now:           func() time.Time { return now },
	})
	handler := l.middleware()(func(ssh.Session) {})

	for i := 1; i <= 20; i++ {
		handler(newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.114." + itoa(i)), Port: 22}))
	}
	if len(l.buckets) != 20 {
		t.Fatalf("buckets = %d, want 20", len(l.buckets))
	}

	now = now.Add(6 * time.Minute)
	handler(newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.114.200"), Port: 22}))
	if len(l.buckets) >= 20 {
		t.Fatalf("cleanup should evict stale entries, buckets = %d", len(l.buckets))
	}
}

func TestRateLimitMiddlewareProcessRestartResetsState(t *testing.T) {
	s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.111"), Port: 22})

	l1 := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 1, enabled: true, maxTrackedIPs: 100})
	h1calls := 0
	h1 := l1.middleware()(func(ssh.Session) { h1calls++ })
	h1(s)
	h1(s)
	if h1calls != 1 {
		t.Fatalf("first limiter calls = %d, want 1", h1calls)
	}

	l2 := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 1, enabled: true, maxTrackedIPs: 100})
	h2calls := 0
	h2 := l2.middleware()(func(ssh.Session) { h2calls++ })
	h2(s)
	if h2calls != 1 {
		t.Fatalf("second limiter calls = %d, want 1", h2calls)
	}
}

func TestRateLimitMiddlewareCapacityLimitBlocksNewIPs(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 1, enabled: true, maxTrackedIPs: 2})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })

	handler(newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.1"), Port: 22}))
	handler(newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.2"), Port: 22}))
	blocked := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("203.0.113.3"), Port: 22})
	handler(blocked)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
	if len(blocked.writes) != 1 {
		t.Fatalf("expected blocked write, got %#v", blocked.writes)
	}
}

func TestExtractRemoteIPTrustProxyFlag(t *testing.T) {
	s := newFakeRateLimitSession(&net.TCPAddr{IP: net.ParseIP("198.51.100.8"), Port: 22})
	s.Context().SetValue(proxyIPSessionKey, "203.0.113.8")
	if got := extractRemoteIP(s, false); got != "198.51.100.8" {
		t.Fatalf("without trust proxy expected remote addr, got %q", got)
	}
	if got := extractRemoteIP(s, true); got != "203.0.113.8" {
		t.Fatalf("with trust proxy expected proxy addr, got %q", got)
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	digits := [12]byte{}
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + (v % 10))
		v /= 10
	}
	return string(digits[i:])
}
