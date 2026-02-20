package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrInvalidRequest  = errors.New("invalid request")
	ErrUnauthorized    = errors.New("unauthorized")
	ErrSessionExpired  = errors.New("session expired")
	ErrSessionClosed   = errors.New("session closed")
)

var (
	validUserPattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
	validHostPattern = regexp.MustCompile(`^[a-zA-Z0-9.-]{1,255}$`)
)

const (
	sessionTokenTTL        = 12 * time.Hour
	sessionIdleLimit       = 30 * time.Minute
	minSecretBytes         = 32
	envGatewayHMACKey      = "GATEWAY_HMAC_SECRET"
	envGatewayHostList     = "GATEWAY_HOST_ALLOWLIST"
	envGatewayEnv          = "GATEWAY_ENV"
	defaultGatewayEnv      = "development"
	maxStdinBytesPerSecond = 256 * 1024
)

type SessionLimits struct {
	CPUSeconds         int    `json:"cpu_seconds"`
	MemoryBytes        uint64 `json:"memory_bytes"`
	MaxDurationSeconds int    `json:"max_duration_seconds"`
}

type OpenSessionRequest struct {
	User    string            `json:"user"`
	Host    string            `json:"host"`
	Port    int               `json:"port"`
	Command []string          `json:"command"`
	Env     map[string]string `json:"env"`
	Limits  SessionLimits     `json:"limits"`
}

type SessionMetadata struct {
	SessionID       string        `json:"session_id"`
	ResumeToken     string        `json:"resume_token,omitempty"`
	ResumeTokenHash string        `json:"-"`
	User            string        `json:"user"`
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	StartedAt       time.Time     `json:"started_at"`
	LastSeenAt      time.Time     `json:"last_seen_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
	Connected       bool          `json:"connected"`
	Limits          SessionLimits `json:"limits"`
}

type Process interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Resize(cols, rows uint16) error
	Close() error
	Done() <-chan error
}

type Launcher interface {
	Launch(context.Context, SessionMetadata, []string, map[string]string) (Process, error)
}

type Service struct {
	launcher      Launcher
	store         MetadataStore
	now           func() time.Time
	secret        []byte
	hostAllowlist []string
	environment   string

	mu       sync.RWMutex
	sessions map[string]*sessionState
	tokens   map[string]string
}

type sessionState struct {
	meta             SessionMetadata
	proc             Process
	cancel           context.CancelFunc
	writeWindowStart time.Time
	bytesInWindow    int
	subscribers      map[int]chan []byte
	nextSubscriberID int
}

func NewService(launcher Launcher, store MetadataStore) (*Service, error) {
	secret := os.Getenv(envGatewayHMACKey)
	if len(secret) < minSecretBytes {
		return nil, fmt.Errorf("%s must be set to at least %d bytes", envGatewayHMACKey, minSecretBytes)
	}
	allow := parseHostAllowlist(os.Getenv(envGatewayHostList))
	env := strings.ToLower(strings.TrimSpace(os.Getenv(envGatewayEnv)))
	if env == "" {
		env = defaultGatewayEnv
	}
	if env == "production" && len(allow) == 0 {
		return nil, fmt.Errorf("%s must be set in production", envGatewayHostList)
	}
	return NewServiceWithSecret(launcher, store, []byte(secret), allow)
}

func NewServiceWithSecret(launcher Launcher, store MetadataStore, secret []byte, hostAllowlist []string) (*Service, error) {
	if len(secret) < minSecretBytes {
		return nil, fmt.Errorf("gateway hmac secret must be at least %d bytes", minSecretBytes)
	}
	return &Service{
		launcher:      launcher,
		store:         store,
		now:           time.Now,
		secret:        append([]byte(nil), secret...),
		hostAllowlist: hostAllowlist,
		environment:   defaultGatewayEnv,
		sessions:      map[string]*sessionState{},
		tokens:        map[string]string{},
	}, nil
}

func (s *Service) OpenSession(ctx context.Context, req OpenSessionRequest) (SessionMetadata, error) {
	if req.User == "" || req.Host == "" || !validUserPattern.MatchString(req.User) || !validHostPattern.MatchString(req.Host) {
		return SessionMetadata{}, ErrInvalidRequest
	}
	if !s.hostAllowed(req.Host) {
		return SessionMetadata{}, ErrUnauthorized
	}
	if len(req.Command) > 0 {
		return SessionMetadata{}, ErrInvalidRequest
	}
	if err := validateInputEnv(req.Env); err != nil {
		return SessionMetadata{}, ErrInvalidRequest
	}
	if req.Port == 0 {
		req.Port = 22
	}
	if req.Port < 1 || req.Port > 65535 {
		return SessionMetadata{}, ErrInvalidRequest
	}

	now := s.now().UTC()
	sessionID, err := randomID()
	if err != nil {
		return SessionMetadata{}, fmt.Errorf("session id: %w", err)
	}
	token, err := randomID()
	if err != nil {
		return SessionMetadata{}, fmt.Errorf("resume token: %w", err)
	}
	tokenHash := s.tokenHash(token)
	meta := SessionMetadata{SessionID: sessionID, ResumeToken: token, ResumeTokenHash: tokenHash, User: req.User, Host: req.Host, Port: req.Port, StartedAt: now, LastSeenAt: now, ExpiresAt: now.Add(sessionTokenTTL), Connected: true, Limits: req.Limits}

	procCtx, cancel := context.WithCancel(ctx)
	proc, err := s.launcher.Launch(procCtx, meta, req.Command, req.Env)
	if err != nil {
		cancel()
		return SessionMetadata{}, mapLaunchError(err)
	}
	state := &sessionState{meta: meta, proc: proc, cancel: cancel, writeWindowStart: now, subscribers: map[int]chan []byte{}}

	s.mu.Lock()
	s.sessions[sessionID] = state
	s.tokens[tokenHash] = sessionID
	s.mu.Unlock()

	persisted := meta
	persisted.ResumeToken = ""
	if err := s.store.Upsert(persisted); err != nil {
		_ = s.Close(sessionID, token)
		return SessionMetadata{}, &FriendlyError{Code: "PERSISTENCE_FAILED", Message: "session metadata could not be persisted", Cause: err}
	}
	go s.watch(sessionID, proc)
	go s.captureOutput(sessionID, proc)
	return meta, nil
}

func (s *Service) SubscribeOutput(sessionID string, token string) (<-chan []byte, func(), error) {
	if _, err := s.authorize(sessionID, token); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return nil, nil, ErrSessionNotFound
	}
	id := st.nextSubscriberID
	st.nextSubscriberID++
	ch := make(chan []byte, 128)
	st.subscribers[id] = ch
	s.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.mu.Lock()
			if current, ok := s.sessions[sessionID]; ok {
				if sub, exists := current.subscribers[id]; exists {
					delete(current.subscribers, id)
					close(sub)
				}
			}
			s.mu.Unlock()
		})
	}

	s.touchSession(sessionID)
	return ch, unsubscribe, nil
}

func (s *Service) ResumeSession(token string) (SessionMetadata, error) {
	tokenHash := s.tokenHash(token)
	s.mu.Lock()
	sid, ok := s.tokens[tokenHash]
	if !ok {
		s.mu.Unlock()
		meta, err := s.store.ByTokenHash(tokenHash)
		if err != nil {
			return SessionMetadata{}, ErrSessionNotFound
		}
		if s.isExpired(meta) {
			return SessionMetadata{}, ErrSessionExpired
		}
		meta.LastSeenAt = s.now().UTC()
		meta.ResumeToken = token
		persisted := meta
		persisted.ResumeToken = ""
		if err := s.store.Upsert(persisted); err != nil {
			log.Printf("level=warn event=gateway_store_upsert_failed session=%s error=%v", meta.SessionID, err)
		}
		return meta, nil
	}
	st, ok := s.sessions[sid]
	if !ok {
		s.mu.Unlock()
		return SessionMetadata{}, ErrSessionNotFound
	}
	if s.isExpired(st.meta) {
		s.mu.Unlock()
		_ = s.Close(sid, token)
		return SessionMetadata{}, ErrSessionExpired
	}
	st.meta.LastSeenAt = s.now().UTC()
	meta := st.meta
	meta.ResumeToken = token
	s.mu.Unlock()
	persisted := meta
	persisted.ResumeToken = ""
	if err := s.store.Upsert(persisted); err != nil {
		log.Printf("level=warn event=gateway_store_upsert_failed session=%s error=%v", meta.SessionID, err)
	}
	return meta, nil
}

func (s *Service) WriteStdin(sessionID string, token string, payload []byte) error {
	st, err := s.authorize(sessionID, token)
	if err != nil {
		return err
	}
	if err := s.checkWriteBudget(sessionID, len(payload)); err != nil {
		return err
	}
	if _, err := st.proc.Write(payload); err != nil {
		return mapLaunchError(err)
	}
	s.touchSession(sessionID)
	return nil
}

func (s *Service) Resize(sessionID string, token string, cols, rows uint16) error {
	st, err := s.authorize(sessionID, token)
	if err != nil {
		return err
	}
	if err := st.proc.Resize(cols, rows); err != nil {
		return mapLaunchError(err)
	}
	s.touchSession(sessionID)
	return nil
}

func (s *Service) Close(sessionID string, token string) error {
	if _, err := s.authorize(sessionID, token); err != nil {
		return err
	}
	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if ok {
		delete(s.sessions, sessionID)
		delete(s.tokens, st.meta.ResumeTokenHash)
	}
	s.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
	}
	st.cancel()
	if err := st.proc.Close(); err != nil {
		log.Printf("level=warn event=gateway_process_close_failed session=%s error=%v", sessionID, err)
	}
	st.meta.Connected = false
	st.meta.LastSeenAt = s.now().UTC()
	st.meta.ResumeToken = ""
	if err := s.store.Upsert(st.meta); err != nil {
		log.Printf("level=warn event=gateway_store_upsert_failed session=%s error=%v", st.meta.SessionID, err)
	}
	return nil
}

func (s *Service) watch(sessionID string, proc Process) {
	<-proc.Done()
	s.closeSubscribers(sessionID)
	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return
	}
	st.meta.Connected = false
	st.meta.LastSeenAt = s.now().UTC()
	meta := st.meta
	delete(s.sessions, sessionID)
	delete(s.tokens, st.meta.ResumeTokenHash)
	s.mu.Unlock()
	meta.ResumeToken = ""
	if err := s.store.Upsert(meta); err != nil {
		log.Printf("level=warn event=gateway_store_upsert_failed session=%s error=%v", meta.SessionID, err)
	}
}

func (s *Service) captureOutput(sessionID string, proc Process) {
	buf := make([]byte, 4096)
	for {
		n, err := proc.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			s.publishOutput(sessionID, chunk)
		}
		if err != nil {
			s.closeSubscribers(sessionID)
			return
		}
	}
}

func (s *Service) publishOutput(sessionID string, payload []byte) {
	s.mu.RLock()
	st, ok := s.sessions[sessionID]
	if !ok {
		s.mu.RUnlock()
		return
	}
	subs := make([]chan []byte, 0, len(st.subscribers))
	for _, ch := range st.subscribers {
		subs = append(subs, ch)
	}
	s.mu.RUnlock()
	for _, ch := range subs {
		ch <- append([]byte(nil), payload...)
	}
}

func (s *Service) closeSubscribers(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	for id, ch := range st.subscribers {
		close(ch)
		delete(st.subscribers, id)
	}
}

func (s *Service) authorize(sessionID string, token string) (*sessionState, error) {
	tokenHash := s.tokenHash(strings.TrimSpace(token))
	tokenHashPrefix := tokenHash
	if len(tokenHashPrefix) > 12 {
		tokenHashPrefix = tokenHashPrefix[:12]
	}

	s.mu.RLock()
	sid, ok := s.tokens[tokenHash]
	if !ok {
		_, sessionActive := s.sessions[sessionID]
		s.mu.RUnlock()
		if !sessionActive {
			log.Printf("level=warn event=gateway_authorize_failed reason=session_not_active session=%s token_hash_prefix=%s", sessionID, tokenHashPrefix)
			return nil, ErrSessionClosed
		}
		log.Printf("level=warn event=gateway_authorize_failed reason=token_not_found session=%s token_hash_prefix=%s", sessionID, tokenHashPrefix)
		return nil, ErrUnauthorized
	}
	if sid != sessionID {
		s.mu.RUnlock()
		log.Printf("level=warn event=gateway_authorize_failed reason=session_mismatch session=%s mapped_session=%s token_hash_prefix=%s", sessionID, sid, tokenHashPrefix)
		return nil, ErrUnauthorized
	}
	st, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		log.Printf("level=warn event=gateway_authorize_failed reason=session_not_active session=%s token_hash_prefix=%s", sessionID, tokenHashPrefix)
		return nil, ErrSessionNotFound
	}
	if s.isExpired(st.meta) {
		log.Printf("level=warn event=gateway_authorize_failed reason=session_expired session=%s token_hash_prefix=%s", sessionID, tokenHashPrefix)
		return nil, ErrSessionExpired
	}
	return st, nil
}

func (s *Service) checkWriteBudget(sessionID string, bytes int) error {
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}
	if st.writeWindowStart.IsZero() || now.Sub(st.writeWindowStart) >= time.Second {
		st.writeWindowStart = now
		st.bytesInWindow = 0
	}
	if st.bytesInWindow+bytes > maxStdinBytesPerSecond {
		return &FriendlyError{Code: "STDIN_RATE_LIMITED", Message: "stdin throughput limit exceeded", Cause: nil}
	}
	st.bytesInWindow += bytes
	return nil
}

func (s *Service) touchSession(sessionID string) {
	lastSeen := s.now().UTC()
	s.mu.Lock()
	if current, ok := s.sessions[sessionID]; ok {
		current.meta.LastSeenAt = lastSeen
	}
	s.mu.Unlock()
}

func (s *Service) isExpired(meta SessionMetadata) bool {
	now := s.now().UTC()
	if !meta.ExpiresAt.IsZero() && now.After(meta.ExpiresAt) {
		return true
	}
	if !meta.LastSeenAt.IsZero() && now.After(meta.LastSeenAt.Add(sessionIdleLimit)) {
		return true
	}
	return false
}

func (s *Service) tokenHash(token string) string {
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Service) hostAllowed(host string) bool {
	if len(s.hostAllowlist) == 0 {
		return true
	}
	for _, allowed := range s.hostAllowlist {
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

func parseHostAllowlist(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		v := strings.ToLower(strings.TrimSpace(part))
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func validateInputEnv(input map[string]string) error {
	const maxEnvEntries = 8
	if len(input) > maxEnvEntries {
		return ErrInvalidRequest
	}
	allowed := map[string]struct{}{"LANG": {}, "LC_ALL": {}, "TERM": {}}
	for k, v := range input {
		if _, ok := allowed[k]; !ok {
			return ErrInvalidRequest
		}
		if len(strings.TrimSpace(v)) == 0 || len(v) > 128 {
			return ErrInvalidRequest
		}
	}
	return nil
}

func randomID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
