package config

import "testing"

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

func TestLoadFromEnvNeo4jOptional(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() unexpected error: %v", err)
	}

	if cfg.Neo4jURI != "" || cfg.Neo4jUser != "" || cfg.Neo4jPassword != "" {
		t.Fatal("expected Neo4j fields to default to empty strings")
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
