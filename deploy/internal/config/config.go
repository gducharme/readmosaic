package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	defaultHost               = "0.0.0.0"
	defaultPort               = 2222
	defaultHostKeyPath        = ".data/host_ed25519"
	defaultIdleTimeout        = 120 * time.Second
	defaultMaxSessions        = 32
	defaultRateLimitPerSecond = 20
	minimumRateLimit          = 1
	maximumConfiguredSessions = 1024
)

// Config captures startup settings for the deploy entrypoint.
type Config struct {
	Host               string
	Port               int
	HostKeyPath        string
	IdleTimeout        time.Duration
	MaxSessions        int
	RateLimitPerSecond int
}

// LoadFromEnv loads runtime configuration from environment variables.
func LoadFromEnv() (Config, error) {
	host, err := readRequiredOrDefault("MOSAIC_SSH_HOST", defaultHost)
	if err != nil {
		return Config{}, err
	}

	port, err := readInt("MOSAIC_SSH_PORT", defaultPort, 1, 65535)
	if err != nil {
		return Config{}, err
	}

	hostKeyPath, err := readRequiredOrDefault("MOSAIC_SSH_HOST_KEY_PATH", defaultHostKeyPath)
	if err != nil {
		return Config{}, err
	}
	cleanHostKeyPath := filepath.Clean(hostKeyPath)
	if cleanHostKeyPath == "." {
		return Config{}, fmt.Errorf("MOSAIC_SSH_HOST_KEY_PATH must not resolve to current directory")
	}

	idleTimeout, err := readDuration("MOSAIC_SSH_IDLE_TIMEOUT", defaultIdleTimeout)
	if err != nil {
		return Config{}, err
	}

	maxSessions, err := readInt("MOSAIC_SSH_MAX_SESSIONS", defaultMaxSessions, 1, maximumConfiguredSessions)
	if err != nil {
		return Config{}, err
	}

	rateLimitPerSecond, err := readInt("MOSAIC_SSH_RATE_LIMIT_PER_SECOND", defaultRateLimitPerSecond, minimumRateLimit, 10000)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Host:               host,
		Port:               port,
		HostKeyPath:        cleanHostKeyPath,
		IdleTimeout:        idleTimeout,
		MaxSessions:        maxSessions,
		RateLimitPerSecond: rateLimitPerSecond,
	}, nil
}

func readRequiredOrDefault(key, fallback string) (string, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	if raw == "" {
		return "", fmt.Errorf("%s must not be empty", key)
	}

	return raw, nil
}

func readInt(key string, fallback, min, max int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed < min || parsed > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}

	return parsed, nil
}

func readDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}

	return parsed, nil
}
