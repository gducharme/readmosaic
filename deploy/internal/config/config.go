package config

import (
	"os"
	"strconv"
)

// Config captures startup settings for the deploy entrypoint.
type Config struct {
	Host string
	Port int
}

// LoadFromEnv loads runtime configuration from environment variables.
func LoadFromEnv() Config {
	cfg := Config{
		Host: readEnv("MOSAIC_SSH_HOST", "0.0.0.0"),
		Port: readEnvInt("MOSAIC_SSH_PORT", 2222),
	}

	return cfg
}

func readEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func readEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return parsed
}
