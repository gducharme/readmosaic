package server

import (
	"testing"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/router"
)

func TestNewRuntimeStartupPipeline(t *testing.T) {
	cfg := config.Config{
		Host:            "127.0.0.1",
		Port:            2222,
		HostKeyPath:     ".data/host_ed25519",
		RateLimitPerSec: 10,
		MaxSessions:     4,
	}

	chain := router.DefaultChain(cfg.RateLimitPerSec)
	runtime, err := New(cfg, chain)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got := runtime.Address(); got != "127.0.0.1:2222" {
		t.Fatalf("Address() = %q, want %q", got, "127.0.0.1:2222")
	}

	want := []string{"rate-limiting", "username-routing", "session-context"}
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
