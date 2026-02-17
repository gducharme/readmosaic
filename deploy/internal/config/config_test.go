package config

import "testing"

func TestLoadFromEnvInvalidPort(t *testing.T) {
	t.Setenv("MOSAIC_SSH_PORT", "not-a-number")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid port")
	}
}

func TestLoadFromEnvPortOutOfRange(t *testing.T) {
	t.Setenv("MOSAIC_SSH_PORT", "70000")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for out-of-range port")
	}
}

func TestLoadFromEnvEmptyHost(t *testing.T) {
	t.Setenv("MOSAIC_SSH_HOST", "")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for empty host")
	}
}

func TestLoadFromEnvEmptyHostKeyPath(t *testing.T) {
	t.Setenv("MOSAIC_SSH_HOST_KEY_PATH", "")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for empty host key path")
	}
}

func TestLoadFromEnvInvalidHostKeyPath(t *testing.T) {
	t.Setenv("MOSAIC_SSH_HOST_KEY_PATH", ".")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for host key path resolving to current directory")
	}
}

func TestLoadFromEnvInvalidIdleTimeout(t *testing.T) {
	t.Setenv("MOSAIC_SSH_IDLE_TIMEOUT", "not-duration")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid duration")
	}
}

func TestLoadFromEnvNegativeIdleTimeout(t *testing.T) {
	t.Setenv("MOSAIC_SSH_IDLE_TIMEOUT", "-1s")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for negative duration")
	}
}

func TestLoadFromEnvInvalidMaxSessions(t *testing.T) {
	t.Setenv("MOSAIC_SSH_MAX_SESSIONS", "0")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid max sessions")
	}
}

func TestLoadFromEnvInvalidRateLimit(t *testing.T) {
	t.Setenv("MOSAIC_SSH_RATE_LIMIT_PER_SECOND", "0")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid rate limit")
	}
}
