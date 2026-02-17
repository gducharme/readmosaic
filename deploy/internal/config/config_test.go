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
