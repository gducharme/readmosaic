package server

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
)

type ipBucket struct {
	tokens float64
	last   time.Time
}

// RateLimitMiddleware enforces per-IP connection limits using a token bucket.
func RateLimitMiddleware(limitPerMinute, burst int) wish.Middleware {
	if limitPerMinute <= 0 {
		limitPerMinute = 30
	}
	if burst <= 0 {
		burst = 10
	}

	ratePerSecond := float64(limitPerMinute) / 60.0
	var mu sync.Mutex
	buckets := make(map[string]ipBucket)

	allow := func(ip string, now time.Time) bool {
		mu.Lock()
		defer mu.Unlock()

		bucket := buckets[ip]
		if bucket.last.IsZero() {
			bucket = ipBucket{tokens: float64(burst), last: now}
		}

		elapsed := now.Sub(bucket.last).Seconds()
		if elapsed > 0 {
			bucket.tokens += elapsed * ratePerSecond
			if bucket.tokens > float64(burst) {
				bucket.tokens = float64(burst)
			}
			bucket.last = now
		}

		if bucket.tokens < 1 {
			buckets[ip] = bucket
			return false
		}

		bucket.tokens--
		buckets[ip] = bucket
		return true
	}

	return func(next ssh.Handler) ssh.Handler {
		return func(s ssh.Session) {
			now := time.Now().UTC()
			ip := remoteIP(s)
			if !allow(ip, now) {
				log.Printf("level=warn event=rate_limit_throttled remote_ip=%s timestamp=%s", ip, now.Format(time.RFC3339))
				_, _ = s.Write([]byte("rate limit exceeded\n"))
				return
			}
			next(s)
		}
	}
}

func remoteIP(s ssh.Session) string {
	remote := s.RemoteAddr()
	if remote == nil {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return remote.String()
	}

	if host == "" {
		return "unknown"
	}
	return host
}
