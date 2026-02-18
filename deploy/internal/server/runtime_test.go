package server

import (
	"context"
	"net"
	"testing"
	"time"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
)

func TestNewRuntimeStartupPipeline(t *testing.T) {
	cfg := config.Config{
		Host:                   "127.0.0.1",
		Port:                   2222,
		HostKeyPath:            ".data/host_ed25519",
		IdleTimeout:            30 * time.Second,
		RateLimitPerSecond:     4,
		RateLimitPerMin:        30,
		RateLimitBurst:         10,
		RateLimitMaxAttempts:   30,
		RateLimitWindow:        time.Minute,
		RateLimitBanDuration:   0,
		RateLimitEnabled:       true,
		RateLimitMaxTrackedIPs: 10000,
		MaxSessions:            4,
	}

	chain := router.DefaultChain()
	runtime, err := New(cfg, chain)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got := runtime.Address(); got != "127.0.0.1:2222" {
		t.Fatalf("Address() = %q, want %q", got, "127.0.0.1:2222")
	}

	want := []string{"max-sessions", "rate-limit", "username-routing", "session-metadata"}
	got := runtime.MiddlewareIDs()
	if len(got) != len(want) {
		t.Fatalf("middleware length = %d, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("middleware[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRuntimeRunAcceptsTCPConnection(t *testing.T) {
	cfg := config.Config{
		Host:                   "127.0.0.1",
		Port:                   0,
		HostKeyPath:            ".data/host_ed25519",
		IdleTimeout:            30 * time.Second,
		RateLimitPerSecond:     100,
		RateLimitPerMin:        30,
		RateLimitBurst:         10,
		RateLimitMaxAttempts:   30,
		RateLimitWindow:        time.Minute,
		RateLimitBanDuration:   0,
		RateLimitEnabled:       true,
		RateLimitMaxTrackedIPs: 10000,
		MaxSessions:            4,
	}

	runtime, err := New(cfg, router.DefaultChain())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runtime.Run(runCtx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime did not expose listener address in time")
		}
		if runtime.Address() != "127.0.0.1:0" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	conn, dialErr := net.DialTimeout("tcp", runtime.Address(), time.Second)
	if dialErr != nil {
		cancel()
		t.Fatalf("DialTimeout() error = %v", dialErr)
	}
	_ = conn.Close()

	cancel()
	select {
	case runErr := <-errCh:
		if runErr != nil {
			t.Fatalf("Run() error = %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not exit after cancel")
	}
}
