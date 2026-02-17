package config

import "testing"

func TestLoadFromEnvInvalidPort(t *testing.T) {
	t.Setenv("MOSAIC_SSH_PORT", "not-a-number")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid port")
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

func TestLoadFromEnvInvalidIdleTimeout(t *testing.T) {
	t.Setenv("MOSAIC_SSH_IDLE_TIMEOUT", "not-duration")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid duration")
	}
}

func TestLoadFromEnvInvalidMaxSessions(t *testing.T) {
	t.Setenv("MOSAIC_SSH_MAX_SESSIONS", "0")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid max sessions")
	}
}

func TestLoadFromEnvInvalidConcurrencyLimit(t *testing.T) {
	t.Setenv("MOSAIC_SSH_MAX_SESSIONS", "4")
	t.Setenv("MOSAIC_SSH_CONCURRENCY_LIMIT", "9")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() expected error for invalid concurrency limit")
	}
}
