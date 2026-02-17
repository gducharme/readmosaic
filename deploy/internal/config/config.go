package config

import (
	"fmt"
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
	ListenAddr         string
	ManifestoENTxID    string
	ManifestoARTxID    string
	ManifestoZHTxID    string
	GenesisTxID        string
	BTCAnchorHeight    int
	Neo4jURI           string
	Neo4jUser          string
	Neo4jPassword      string
	RateLimitPerMin    int
	RateLimitBurst     int
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

	listenAddr, err := readRequiredOrDefault("LISTEN_ADDR", defaultListenAddr)
	if err != nil {
		return Config{}, err
	}

	manifestoENTxID, err := readRequired("ARWEAVE_TXID_MANIFESTO_EN")
	if err != nil {
		return Config{}, err
	}

	manifestoARTxID, err := readRequired("ARWEAVE_TXID_MANIFESTO_AR")
	if err != nil {
		return Config{}, err
	}

	manifestoZHTxID, err := readRequired("ARWEAVE_TXID_MANIFESTO_ZH")
	if err != nil {
		return Config{}, err
	}

	genesisTxID, err := readRequired("ARWEAVE_TXID_GENESIS")
	if err != nil {
		return Config{}, err
	}

	btcAnchorHeight, err := readRequiredInt("BTC_ANCHOR_HEIGHT", 0, 10_000_000)
	if err != nil {
		return Config{}, err
	}

	neo4jURI := readOptional("NEO4J_URI")
	neo4jUser := readOptional("NEO4J_USER")
	neo4jPassword := readOptional("NEO4J_PASSWORD")

	rateLimitPerMin, err := readInt("RATE_LIMIT_PER_MIN", defaultRateLimitPerMin, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}

	rateLimitBurst, err := readInt("RATE_LIMIT_BURST", defaultRateLimitBurst, minimumRateLimit, 1_000_000)
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
		ListenAddr:         listenAddr,
		ManifestoENTxID:    manifestoENTxID,
		ManifestoARTxID:    manifestoARTxID,
		ManifestoZHTxID:    manifestoZHTxID,
		GenesisTxID:        genesisTxID,
		BTCAnchorHeight:    btcAnchorHeight,
		Neo4jURI:           neo4jURI,
		Neo4jUser:          neo4jUser,
		Neo4jPassword:      neo4jPassword,
		RateLimitPerMin:    rateLimitPerMin,
		RateLimitBurst:     rateLimitBurst,
	}, nil
}

func readRequired(key string) (string, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("%s is required", key)
	}

	return raw, nil
}

func readRequiredInt(key string, min, max int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, fmt.Errorf("%s is required", key)
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
