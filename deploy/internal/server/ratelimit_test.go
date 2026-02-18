package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/charmbracelet/ssh"
)

type fakeRateLimitSession struct {
	remote net.Addr
	values map[any]any
	writes []string
}

func (f *fakeRateLimitSession) User() string             { return "guest" }
func (f *fakeRateLimitSession) Context() context.Context { return context.Background() }
func (f *fakeRateLimitSession) SetValue(key any, value any) {
	if f.values == nil {
		f.values = map[any]any{}
	}
	f.values[key] = value
}
func (f *fakeRateLimitSession) Value(key any) any {
	if f.values == nil {
		return nil
	}
	return f.values[key]
}
func (f *fakeRateLimitSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
}
func (f *fakeRateLimitSession) RemoteAddr() net.Addr { return f.remote }

func TestRateLimitMiddlewareRapidConnectionsFromSameIP(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 2, enabled: true, maxTrackedIPs: 100})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })
	session := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 2222}}

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
		s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113." + itoa(i)), Port: 22}}
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
	s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.44"), Port: 22}}

	handler(s)
	handler(s)
	now = now.Add(time.Minute)
	handler(s)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
}

func TestExtractRemoteIPNormalizesIPv6AndIPv4(t *testing.T) {
	ipv4Mapped := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("::ffff:203.0.113.9"), Port: 22}}
	if got := extractRemoteIP(ipv4Mapped, false); got != "203.0.113.9" {
		t.Fatalf("extractRemoteIP(mapped) = %q", got)
	}

	ipv6 := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::10"), Port: 22}}
	if got := extractRemoteIP(ipv6, false); got != "2001:db8::10" {
		t.Fatalf("extractRemoteIP(v6) = %q", got)
	}
}

func TestRateLimitMiddlewareSharedNATSharesLimit(t *testing.T) {
	l := newRateLimiter(rateLimiterOptions{maxAttempts: 60, window: time.Minute, burst: 2, enabled: true, maxTrackedIPs: 100})
	handlerCalls := 0
	handler := l.middleware()(func(ssh.Session) { handlerCalls++ })

	a := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("198.51.100.50"), Port: 1111}}
	b := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("198.51.100.50"), Port: 2222}}

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

	s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.90"), Port: 4444}}
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
	s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.91"), Port: 22}}

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
		handler(&fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.114." + itoa(i)), Port: 22}})
	}
	if len(l.buckets) != 20 {
		t.Fatalf("buckets = %d, want 20", len(l.buckets))
	}

	now = now.Add(6 * time.Minute)
	handler(&fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.114.200"), Port: 22}})
	if len(l.buckets) >= 20 {
		t.Fatalf("cleanup should evict stale entries, buckets = %d", len(l.buckets))
	}
}

func TestRateLimitMiddlewareProcessRestartResetsState(t *testing.T) {
	s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.111"), Port: 22}}

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

	handler(&fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.1"), Port: 22}})
	handler(&fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.2"), Port: 22}})
	blocked := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.3"), Port: 22}}
	handler(blocked)

	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
	if len(blocked.writes) != 1 {
		t.Fatalf("expected blocked write, got %#v", blocked.writes)
	}
}

func TestExtractRemoteIPTrustProxyFlag(t *testing.T) {
	s := &fakeRateLimitSession{remote: &net.TCPAddr{IP: net.ParseIP("198.51.100.8"), Port: 22}, values: map[any]any{proxyIPSessionKey: "203.0.113.8"}}
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
