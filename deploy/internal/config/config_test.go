package config

import (
	"strings"
	"testing"
	"time"
)

const sampleTxID = "abcdefghijklmnopqrstuvwxyzABCDEF1234567890_"

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", sampleTxID)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_AR", sampleTxID)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_ZH", sampleTxID)
	t.Setenv("ARWEAVE_TXID_GENESIS", sampleTxID)
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

func TestLoadFromEnvWhitespaceHost(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("MOSAIC_SSH_HOST", "   ")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for whitespace host")
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

func TestLoadFromEnvMissingRequiredVariablesAggregated(t *testing.T) {
	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("LoadFromEnv() expected error for missing required variables")
	}

	msg := err.Error()
	for _, key := range []string{
		"ARWEAVE_TXID_GENESIS",
		"ARWEAVE_TXID_MANIFESTO_AR",
		"ARWEAVE_TXID_MANIFESTO_EN",
		"ARWEAVE_TXID_MANIFESTO_ZH",
		"BTC_ANCHOR_HEIGHT",
	} {
		if !strings.Contains(msg, key) {
			t.Fatalf("expected aggregated error to include %s; got %q", key, msg)
		}
	}
}

func TestLoadFromEnvWhitespaceManifestoTxID(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "   ")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for whitespace ARWEAVE_TXID_MANIFESTO_EN")
	}
}

func TestLoadFromEnvQuotedManifestoTxID(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", " \""+sampleTxID+"\" ")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}
	if cfg.ManifestoENTxID != sampleTxID {
		t.Fatalf("expected ARWEAVE_TXID_MANIFESTO_EN to be normalized, got %q", cfg.ManifestoENTxID)
	}
}

func TestLoadFromEnvPlaceholderManifestoTxIDRejected(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "changeme")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected placeholder manifesto txid to be rejected")
	}
}

func TestLoadFromEnvInvalidManifestoTxIDFormat(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ARWEAVE_TXID_MANIFESTO_EN", "invalid-txid")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected invalid manifesto txid format to be rejected")
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

func TestLoadFromEnvNeo4jEnabledFromURIAndCredentials(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO4J_URI", "  neo4j://localhost:7687  ")
	t.Setenv("NEO4J_USER", " neo4j ")
	t.Setenv("NEO4J_PASSWORD", " secret ")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}
	if !cfg.Neo4jEnabled {
		t.Fatal("expected Neo4jEnabled when NEO4J_URI is set")
	}
	if cfg.Neo4jURI != "neo4j://localhost:7687" || cfg.Neo4jUser != "neo4j" || cfg.Neo4jPassword != "secret" {
		t.Fatal("expected Neo4j settings to be normalized")
	}
}

func TestLoadFromEnvNeo4jURIMissingCredentials(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO4J_URI", "neo4j://localhost:7687")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("LoadFromEnv() expected neo4j credential validation error")
	}
	if !strings.Contains(err.Error(), "NEO4J_URI requires both NEO4J_USER and NEO4J_PASSWORD") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFromEnvNeo4jCredentialsWithoutURI(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO4J_USER", "neo4j")
	t.Setenv("NEO4J_PASSWORD", "dontlogme")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("LoadFromEnv() expected neo4j URI requirement error")
	}
	if strings.Contains(err.Error(), "dontlogme") {
		t.Fatalf("error should not include password value: %v", err)
	}
}

func TestLoadFromEnvMissingBTCAnchorHeight(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BTC_ANCHOR_HEIGHT", "")
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

func TestLoadFromEnvBurstLowerThanPerMinuteAllowed(t *testing.T) {
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

func TestLoadFromEnvBurstGreaterThanPerMinuteAllowed(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_PER_MIN", "10")
	t.Setenv("RATE_LIMIT_BURST", "100")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}
	if cfg.RateLimitPerMin != 10 || cfg.RateLimitBurst != 100 {
		t.Fatal("expected burst > per-minute to be accepted")
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

func TestLoadFromEnvRateLimitExtendedConfig(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_MAX_ATTEMPTS", "45")
	t.Setenv("RATE_LIMIT_WINDOW", "2m")
	t.Setenv("RATE_LIMIT_BAN_DURATION", "30s")
	t.Setenv("RATE_LIMIT_MAX_TRACKED_IPS", "1234")
	t.Setenv("RATE_LIMIT_TRUST_PROXY_HEADERS", "true")
	t.Setenv("RATE_LIMIT_ENABLED", "false")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}

	if cfg.RateLimitMaxAttempts != 45 || cfg.RateLimitWindow != 2*time.Minute || cfg.RateLimitBanDuration != 30*time.Second {
		t.Fatalf("unexpected extended rate limit values: %+v", cfg)
	}
	if cfg.RateLimitMaxTrackedIPs != 1234 || !cfg.RateLimitTrustProxyHeaders || cfg.RateLimitEnabled {
		t.Fatalf("unexpected flags/capacity values: %+v", cfg)
	}
}

func TestLoadFromEnvRateLimitInvalidBoolean(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_ENABLED", "definitely")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid RATE_LIMIT_ENABLED")
	}
}

func TestLoadFromEnvRateLimitNegativeBanDuration(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_BAN_DURATION", "-1s")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for negative RATE_LIMIT_BAN_DURATION")
	}
}
