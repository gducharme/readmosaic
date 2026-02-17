package config

import (
	"strings"
	"testing"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "manifesto-en")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_AR", "manifesto-ar")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_ZH", "manifesto-zh")
	t.Setenv("ARWEAVE_TXID_GENESIS", "genesis")
	t.Setenv("BTC_ANCHOR_HEIGHT", "840000")
}

func TestLoadFromEnvInvalidPort(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_PORT", "not-a-number")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid port")
	}
}

func TestLoadFromEnvPortOutOfRange(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_PORT", "70000")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for out-of-range port")
	}
}

func TestLoadFromEnvEmptyHost(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_HOST", "")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for empty host")
	}
}

func TestLoadFromEnvWhitespaceHost(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_HOST", "   ")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for whitespace host")
	}
}

func TestLoadFromEnvEmptyHostKeyPath(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_HOST_KEY_PATH", "")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for empty host key path")
	}
}

func TestLoadFromEnvInvalidHostKeyPath(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_HOST_KEY_PATH", ".")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for host key path resolving to current directory")
	}
}

func TestLoadFromEnvInvalidIdleTimeout(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_IDLE_TIMEOUT", "not-duration")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid duration")
	}
}

func TestLoadFromEnvNegativeIdleTimeout(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_IDLE_TIMEOUT", "-1s")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for negative duration")
	}
}

func TestLoadFromEnvInvalidMaxSessions(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_MAX_SESSIONS", "0")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid max sessions")
	}
}

func TestLoadFromEnvInvalidRateLimit(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_RATE_LIMIT_PER_SECOND", "0")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid rate limit")
	}
}

func TestLoadFromEnvMissingManifestoTxID(t *testing.T) {
	t.Setenv("ARWEAVE_TXID_MANIFESTO_AR", "manifesto-ar")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_ZH", "manifesto-zh")
	t.Setenv("ARWEAVE_TXID_GENESIS", "genesis")
	t.Setenv("BTC_ANCHOR_HEIGHT", "840000")

	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for missing ARWEAVE_TXID_MANIFESTO_EN")
	}
}

func TestLoadFromEnvWhitespaceManifestoTxID(t *testing.T) {
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "   ")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_AR", "manifesto-ar")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_ZH", "manifesto-zh")
	t.Setenv("ARWEAVE_TXID_GENESIS", "genesis")
	t.Setenv("BTC_ANCHOR_HEIGHT", "840000")

	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for whitespace ARWEAVE_TXID_MANIFESTO_EN")
	}
}

func TestLoadFromEnvNeo4jOptional(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}

	if cfg.Neo4jURI != "" || cfg.Neo4jUser != "" || cfg.Neo4jPassword != "" || cfg.Neo4jEnabled {
		t.Fatal("expected Neo4j fields to default to empty strings and disabled")
	}
}

func TestLoadFromEnvNeo4jEnabledFromURI(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO4J_URI", "  neo4j://localhost:7687  ")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}
	if !cfg.Neo4jEnabled {
		t.Fatal("expected Neo4jEnabled when NEO4J_URI is set")
	}
	if cfg.Neo4jURI != "neo4j://localhost:7687" {
		t.Fatalf("expected trimmed NEO4J_URI, got %q", cfg.Neo4jURI)
	}
}

func TestLoadFromEnvMissingBTCAnchorHeight(t *testing.T) {
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "manifesto-en")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_AR", "manifesto-ar")
	t.Setenv("ARWEAVE_TXID_MANIFESTO_ZH", "manifesto-zh")
	t.Setenv("ARWEAVE_TXID_GENESIS", "genesis")

	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for missing BTC_ANCHOR_HEIGHT")
	}
}

func TestLoadFromEnvNonIntegerBTCAnchorHeight(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BTC_ANCHOR_HEIGHT", "height")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for non-integer BTC_ANCHOR_HEIGHT")
	}
}

func TestLoadFromEnvNegativeRateLimitPerMinute(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_PER_MIN", "-1")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for negative RATE_LIMIT_PER_MIN")
	}
}

func TestLoadFromEnvNegativeRateLimitBurst(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_BURST", "-1")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for negative RATE_LIMIT_BURST")
	}
}

func TestLoadFromEnvBurstLowerThanPerMinute(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_PER_MIN", "50")
	t.Setenv("RATE_LIMIT_BURST", "20")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}
	if cfg.RateLimitPerMin != 50 || cfg.RateLimitBurst != 20 {
		t.Fatal("expected custom per-minute and burst limits to load as configured")
	}
}

func TestLoadFromEnvVeryLargeIntError(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_PER_MIN", "999999999999999999999999")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("LoadFromEnv() expected error for very large integer")
	}
	if !strings.Contains(err.Error(), "RATE_LIMIT_PER_MIN") {
		t.Fatalf("expected RATE_LIMIT_PER_MIN error context, got: %v", err)
	}
}
