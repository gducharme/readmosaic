package server

import (
	"log"
	"math"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

const proxyIPSessionKey = "mosaic.proxy_ip"

type rateLimiterOptions struct {
	maxAttempts       int
	window            time.Duration
	burst             int
	banDuration       time.Duration
	maxTrackedIPs     int
	trustProxyHeaders bool
	enabled           bool
	now               func() time.Time
}

type ipBucket struct {
	tokens       float64
	last         time.Time
	lastSeen     time.Time
	blockedUntil time.Time
}

type rateLimiter struct {
	mu            sync.Mutex
	buckets       map[string]ipBucket
	nextCleanup   time.Time
	ratePerSecond float64
	opts          rateLimiterOptions

	totalBlockedConnections atomic.Uint64
	rateLimitHits           atomic.Uint64
}

// RateLimitMiddleware enforces per-IP connection limits using a token bucket.
func RateLimitMiddleware(maxAttempts int, window time.Duration, burst int, banDuration time.Duration, maxTrackedIPs int, trustProxyHeaders, enabled bool) wish.Middleware {
	r := newRateLimiter(rateLimiterOptions{
		maxAttempts:       maxAttempts,
		window:            window,
		burst:             burst,
		banDuration:       banDuration,
		maxTrackedIPs:     maxTrackedIPs,
		trustProxyHeaders: trustProxyHeaders,
		enabled:           enabled,
		now:               time.Now,
	})
	return r.middleware()
}

func newRateLimiter(opts rateLimiterOptions) *rateLimiter {
	if opts.maxAttempts <= 0 {
		opts.maxAttempts = 30
	}
	if opts.window <= 0 {
		opts.window = time.Minute
	}
	if opts.burst <= 0 {
		opts.burst = 10
	}
	if opts.maxTrackedIPs <= 0 {
		opts.maxTrackedIPs = 10000
	}
	if opts.now == nil {
		opts.now = time.Now
	}

	return &rateLimiter{
		buckets:       make(map[string]ipBucket),
		ratePerSecond: float64(opts.maxAttempts) / opts.window.Seconds(),
		opts:          opts,
		nextCleanup:   opts.now().UTC().Add(opts.window),
	}
}

func (r *rateLimiter) middleware() wish.Middleware {
	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			if !r.opts.enabled {
				next(s)
				return
			}

			now := r.opts.now().UTC()
			ip := extractRemoteIP(s, r.opts.trustProxyHeaders)
			allowed, reason, activeIPs, hits, totalBlocked := r.allow(ip, now)
			if !allowed {
				log.Printf("level=warn event=rate_limit_throttled remote_ip=%s timestamp=%s reason=%s rate_limit_hits=%d total_blocked_connections=%d active_tracked_ips=%d", ip, now.Format(time.RFC3339), reason, hits, totalBlocked, activeIPs)
				_, _ = s.Write([]byte("rate limit exceeded\n"))
				return
			}

			next(s)
		}
	}
}

func (r *rateLimiter) allow(ip string, now time.Time) (allowed bool, reason string, activeIPs int, rateLimitHits uint64, totalBlocked uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if now.After(r.nextCleanup) {
		r.cleanup(now)
		r.nextCleanup = now.Add(r.opts.window)
	}

	bucket, exists := r.buckets[ip]
	if !exists {
		if len(r.buckets) >= r.opts.maxTrackedIPs {
			rateLimitHits = r.rateLimitHits.Add(1)
			totalBlocked = r.totalBlockedConnections.Add(1)
			return false, "capacity", len(r.buckets), rateLimitHits, totalBlocked
		}
		bucket = ipBucket{tokens: float64(r.opts.burst), last: now, lastSeen: now}
	}

	bucket.lastSeen = now
	if !bucket.blockedUntil.IsZero() && now.Before(bucket.blockedUntil) {
		r.buckets[ip] = bucket
		rateLimitHits = r.rateLimitHits.Add(1)
		totalBlocked = r.totalBlockedConnections.Add(1)
		return false, "ban_active", len(r.buckets), rateLimitHits, totalBlocked
	}
	if !bucket.blockedUntil.IsZero() && !now.Before(bucket.blockedUntil) {
		bucket.blockedUntil = time.Time{}
	}

	elapsed := now.Sub(bucket.last).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	bucket.tokens = math.Min(float64(r.opts.burst), bucket.tokens+(elapsed*r.ratePerSecond))
	bucket.last = now
	if bucket.tokens < 1 {
		if r.opts.banDuration > 0 {
			bucket.blockedUntil = now.Add(r.opts.banDuration)
		}
		r.buckets[ip] = bucket
		rateLimitHits = r.rateLimitHits.Add(1)
		totalBlocked = r.totalBlockedConnections.Add(1)
		return false, "token_exhausted", len(r.buckets), rateLimitHits, totalBlocked
	}

	bucket.tokens--
	r.buckets[ip] = bucket
	return true, "", len(r.buckets), r.rateLimitHits.Load(), r.totalBlockedConnections.Load()
}

func (r *rateLimiter) cleanup(now time.Time) {
	ttl := r.opts.window * 2
	if ttl < 5*time.Minute {
		ttl = 5 * time.Minute
	}
	if r.opts.banDuration > ttl {
		ttl = r.opts.banDuration
	}

	for ip, bucket := range r.buckets {
		if now.Sub(bucket.lastSeen) > ttl && (bucket.blockedUntil.IsZero() || now.After(bucket.blockedUntil)) {
			delete(r.buckets, ip)
		}
	}
}

func extractRemoteIP(s ssh.Session, trustProxyHeaders bool) string {
	if trustProxyHeaders {
		if proxyValue, ok := s.Context().Value(proxyIPSessionKey).(string); ok {
			if normalized := normalizeIP(proxyValue); normalized != "" {
				return normalized
			}
		}
	}

	remote := s.RemoteAddr()
	if remote == nil {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		host = remote.String()
	}
	if normalized := normalizeIP(host); normalized != "" {
		return normalized
	}
	if host == "" {
		return "unknown"
	}
	return host
}

func normalizeIP(raw string) string {
	raw = trimZone(raw)
	if ip, err := netip.ParseAddr(raw); err == nil {
		return ip.Unmap().String()
	}
	return ""
}

func trimZone(host string) string {
	for i := 0; i < len(host); i++ {
		if host[i] == '%' {
			return host[:i]
		}
	}
	return host
}
