package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHost               = "0.0.0.0"
	defaultPort               = 2222
	defaultHostKeyPath        = ".data/host_ed25519"
	defaultIdleTimeout        = 120 * time.Second
	defaultMaxSessions        = 32
	defaultRateLimitPerSecond = 20
	defaultListenAddr         = ":2222"
	defaultRateLimitPerMin    = 30
	defaultRateLimitBurst     = 10
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

	ListenAddr      string
	ManifestoENTxID string
	ManifestoARTxID string
	ManifestoZHTxID string
	GenesisTxID     string
	BTCAnchorHeight int

	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	Neo4jEnabled  bool

	RateLimitPerMin int
	RateLimitBurst  int
}

// LoadFromEnv loads runtime configuration from environment variables.
func LoadFromEnv() (Config, error) {
	cfg := Config{}
	var err error

	cfg.Host, err = readRequiredOrDefault("MOSAIC_SSH_HOST", defaultHost)
	if err != nil {
		return Config{}, err
	}

	cfg.Port, err = readInt("MOSAIC_SSH_PORT", defaultPort, 1, 65535)
	if err != nil {
		return Config{}, err
	}

	hostKeyPath, err := readRequiredOrDefault("MOSAIC_SSH_HOST_KEY_PATH", defaultHostKeyPath)
	if err != nil {
		return Config{}, err
	}
	cfg.HostKeyPath = filepath.Clean(hostKeyPath)

	cfg.IdleTimeout, err = readDuration("MOSAIC_SSH_IDLE_TIMEOUT", defaultIdleTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.MaxSessions, err = readInt("MOSAIC_SSH_MAX_SESSIONS", defaultMaxSessions, 1, maximumConfiguredSessions)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitPerSecond, err = readInt("MOSAIC_SSH_RATE_LIMIT_PER_SECOND", defaultRateLimitPerSecond, minimumRateLimit, 10_000)
	if err != nil {
		return Config{}, err
	}

	cfg.ListenAddr, err = readRequiredOrDefault("LISTEN_ADDR", defaultListenAddr)
	if err != nil {
		return Config{}, err
	}

	cfg.ManifestoENTxID, err = readRequired("ARWEAVE_TXID_MANIFESTO_EN")
	if err != nil {
		return Config{}, err
	}

	cfg.ManifestoARTxID, err = readRequired("ARWEAVE_TXID_MANIFESTO_AR")
	if err != nil {
		return Config{}, err
	}

	cfg.ManifestoZHTxID, err = readRequired("ARWEAVE_TXID_MANIFESTO_ZH")
	if err != nil {
		return Config{}, err
	}

	cfg.GenesisTxID, err = readRequired("ARWEAVE_TXID_GENESIS")
	if err != nil {
		return Config{}, err
	}

	cfg.BTCAnchorHeight, err = readRequiredInt("BTC_ANCHOR_HEIGHT", 0, 10_000_000)
	if err != nil {
		return Config{}, err
	}

	cfg.Neo4jURI = readOptional("NEO4J_URI")
	cfg.Neo4jUser = readOptional("NEO4J_USER")
	cfg.Neo4jPassword = readOptional("NEO4J_PASSWORD")
	cfg.Neo4jEnabled = cfg.Neo4jURI != ""

	cfg.RateLimitPerMin, err = readInt("RATE_LIMIT_PER_MIN", defaultRateLimitPerMin, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitBurst, err = readInt("RATE_LIMIT_BURST", defaultRateLimitBurst, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate performs cross-field checks once config values are loaded.
func (c Config) Validate() error {
	if c.HostKeyPath == "." {
		return fmt.Errorf("MOSAIC_SSH_HOST_KEY_PATH must not resolve to current directory")
	}
	return nil
}

func readRequired(key string) (string, error) {
	raw, ok := os.LookupEnv(key)
	trimmed := strings.TrimSpace(raw)
	if !ok || trimmed == "" {
		return "", fmt.Errorf("%s is required", key)
	}

	return trimmed, nil
}

func readRequiredInt(key string, min, max int) (int, error) {
	raw, ok := os.LookupEnv(key)
	trimmed := strings.TrimSpace(raw)
	if !ok || trimmed == "" {
		return 0, fmt.Errorf("%s is required", key)
	}

	parsed64, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if parsed64 < int64(min) || parsed64 > int64(max) || parsed64 > math.MaxInt {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}

	return int(parsed64), nil
}

func readOptional(key string) string {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return ""
	}

	return strings.TrimSpace(raw)
}

func readRequiredOrDefault(key, fallback string) (string, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%s must not be empty", key)
	}

	return trimmed, nil
}

func readInt(key string, fallback, min, max int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}

	parsed64, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if parsed64 < int64(min) || parsed64 > int64(max) || parsed64 > math.MaxInt {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}

	return int(parsed64), nil
}

func readDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}

	return parsed, nil
}
