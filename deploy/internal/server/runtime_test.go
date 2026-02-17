package server

import (
	"testing"
	"time"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
)

func TestNewRuntimeStartupPipeline(t *testing.T) {
	cfg := config.Config{
		Host:             "127.0.0.1",
		Port:             2222,
		HostKeyPath:      ".data/host_ed25519",
		IdleTimeout:      30 * time.Second,
		ConcurrencyLimit: 4,
		MaxSessions:      4,
	}

	chain := router.DefaultChain(cfg.ConcurrencyLimit)
	runtime, err := New(cfg, chain)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got := runtime.Address(); got != "127.0.0.1:2222" {
		t.Fatalf("Address() = %q, want %q", got, "127.0.0.1:2222")
	}

	want := []string{"concurrency-limit", "username-routing", "session-metadata"}
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
