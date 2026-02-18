package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHost                   = "0.0.0.0"
	defaultPort                   = 2222
	defaultHostKeyPath            = ".data/host_ed25519"
	defaultIdleTimeout            = 120 * time.Second
	defaultMaxSessions            = 32
	defaultRateLimitPerSecond     = 20
	defaultListenAddr             = ":2222"
	defaultRateLimitMaxAttempts   = 30
	defaultRateLimitBurst         = 10
	defaultRateLimitWindow        = time.Minute
	defaultRateLimitBanDuration   = 0 * time.Second
	defaultRateLimitEnabled       = true
	defaultRateLimitTrustProxy    = false
	defaultRateLimitMaxTrackedIPs = 10000
	minimumRateLimit              = 1
	maximumConfiguredSessions     = 1024
)

var arweaveTxIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

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

	RateLimitPerMin int // legacy alias retained for compatibility
	RateLimitBurst  int

	RateLimitMaxAttempts       int
	RateLimitWindow            time.Duration
	RateLimitBanDuration       time.Duration
	RateLimitEnabled           bool
	RateLimitTrustProxyHeaders bool
	RateLimitMaxTrackedIPs     int
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

	missingRequired := make([]string, 0, 5)
	cfg.ManifestoENTxID = readRequiredCollect("ARWEAVE_TXID_MANIFESTO_EN", &missingRequired)
	cfg.ManifestoARTxID = readRequiredCollect("ARWEAVE_TXID_MANIFESTO_AR", &missingRequired)
	cfg.ManifestoZHTxID = readRequiredCollect("ARWEAVE_TXID_MANIFESTO_ZH", &missingRequired)
	cfg.GenesisTxID = readRequiredCollect("ARWEAVE_TXID_GENESIS", &missingRequired)
	btcAnchorHeightRaw := readRequiredCollect("BTC_ANCHOR_HEIGHT", &missingRequired)
	if len(missingRequired) > 0 {
		sort.Strings(missingRequired)
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missingRequired, ", "))
	}

	if err := validateArweaveTxID("ARWEAVE_TXID_MANIFESTO_EN", cfg.ManifestoENTxID); err != nil {
		return Config{}, err
	}
	if err := validateArweaveTxID("ARWEAVE_TXID_MANIFESTO_AR", cfg.ManifestoARTxID); err != nil {
		return Config{}, err
	}
	if err := validateArweaveTxID("ARWEAVE_TXID_MANIFESTO_ZH", cfg.ManifestoZHTxID); err != nil {
		return Config{}, err
	}
	if err := validateArweaveTxID("ARWEAVE_TXID_GENESIS", cfg.GenesisTxID); err != nil {
		return Config{}, err
	}

	cfg.BTCAnchorHeight, err = parseRequiredInt("BTC_ANCHOR_HEIGHT", btcAnchorHeightRaw, 0, 10_000_000)
	if err != nil {
		return Config{}, err
	}

	cfg.Neo4jURI = readOptional("NEO4J_URI")
	cfg.Neo4jUser = readOptional("NEO4J_USER")
	cfg.Neo4jPassword = readOptional("NEO4J_PASSWORD")
	cfg.Neo4jEnabled = cfg.Neo4jURI != ""

	legacyPerMin, err := readInt("RATE_LIMIT_PER_MIN", defaultRateLimitMaxAttempts, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}
	cfg.RateLimitPerMin = legacyPerMin

	cfg.RateLimitMaxAttempts, err = readInt("RATE_LIMIT_MAX_ATTEMPTS", legacyPerMin, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitBurst, err = readInt("RATE_LIMIT_BURST", defaultRateLimitBurst, minimumRateLimit, 1_000_000)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitWindow, err = readDuration("RATE_LIMIT_WINDOW", defaultRateLimitWindow)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitBanDuration, err = readNonNegativeDuration("RATE_LIMIT_BAN_DURATION", defaultRateLimitBanDuration)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitEnabled, err = readBool("RATE_LIMIT_ENABLED", defaultRateLimitEnabled)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitTrustProxyHeaders, err = readBool("RATE_LIMIT_TRUST_PROXY_HEADERS", defaultRateLimitTrustProxy)
	if err != nil {
		return Config{}, err
	}

	cfg.RateLimitMaxTrackedIPs, err = readInt("RATE_LIMIT_MAX_TRACKED_IPS", defaultRateLimitMaxTrackedIPs, 1, 10_000_000)
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
	if c.Neo4jURI == "" && (c.Neo4jUser != "" || c.Neo4jPassword != "") {
		return fmt.Errorf("NEO4J_USER and NEO4J_PASSWORD require NEO4J_URI")
	}
	if c.Neo4jURI != "" && (c.Neo4jUser == "" || c.Neo4jPassword == "") {
		return fmt.Errorf("NEO4J_URI requires both NEO4J_USER and NEO4J_PASSWORD")
	}

	return nil
}

func readRequiredCollect(key string, missing *[]string) string {
	raw, ok := os.LookupEnv(key)
	trimmed := normalizeEnvValue(raw)
	if !ok || trimmed == "" {
		*missing = append(*missing, key)
		return ""
	}

	return trimmed
}

func readOptional(key string) string {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return ""
	}

	return normalizeEnvValue(raw)
}

func readRequiredOrDefault(key, fallback string) (string, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	trimmed := normalizeEnvValue(raw)
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

	return parseRequiredInt(key, normalizeEnvValue(raw), min, max)
}

func parseRequiredInt(key, raw string, min, max int) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("%s is required", key)
	}

	parsed64, err := strconv.ParseInt(raw, 10, 64)
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

	parsed, err := time.ParseDuration(normalizeEnvValue(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}

	return parsed, nil
}

func readNonNegativeDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(normalizeEnvValue(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be greater than or equal to 0", key)
	}

	return parsed, nil
}

func readBool(key string, fallback bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	value := strings.ToLower(normalizeEnvValue(raw))
	switch value {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", key)
	}
}

func validateArweaveTxID(key, txID string) error {
	lower := strings.ToLower(txID)
	if lower == "todo" || lower == "changeme" {
		return fmt.Errorf("%s must be set to a real Arweave transaction ID", key)
	}
	if !arweaveTxIDPattern.MatchString(txID) {
		return fmt.Errorf("%s must be a valid Arweave transaction ID", key)
	}

	return nil
}

func normalizeEnvValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= 2 {
		if (trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"') || (trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'') {
			return strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		}
	}

	return trimmed
}
