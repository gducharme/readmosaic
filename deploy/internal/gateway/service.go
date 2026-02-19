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
)

var (
	validUserPattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
	validHostPattern = regexp.MustCompile(`^[a-zA-Z0-9.-]{1,255}$`)
)

const (
	sessionTokenTTL  = 12 * time.Hour
	sessionIdleLimit = 30 * time.Minute
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
	Write([]byte) (int, error)
	Resize(cols, rows uint16) error
	Close() error
	Done() <-chan error
}

type Launcher interface {
	Launch(context.Context, SessionMetadata, []string, map[string]string) (Process, error)
}

type Service struct {
	launcher Launcher
	store    MetadataStore
	now      func() time.Time
	secret   []byte

	mu       sync.RWMutex
	sessions map[string]*sessionState
	tokens   map[string]string
}

type sessionState struct {
	meta   SessionMetadata
	proc   Process
	cancel context.CancelFunc
}

func NewService(launcher Launcher, store MetadataStore) *Service {
	return &Service{
		launcher: launcher,
		store:    store,
		now:      time.Now,
		secret:   []byte("gateway-local-dev-secret"),
		sessions: map[string]*sessionState{},
		tokens:   map[string]string{},
	}
}

func (s *Service) OpenSession(ctx context.Context, req OpenSessionRequest) (SessionMetadata, error) {
	if req.User == "" || req.Host == "" || !validUserPattern.MatchString(req.User) || !validHostPattern.MatchString(req.Host) {
		return SessionMetadata{}, ErrInvalidRequest
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
	meta := SessionMetadata{
		SessionID:       sessionID,
		ResumeToken:     token,
		ResumeTokenHash: tokenHash,
		User:            req.User,
		Host:            req.Host,
		Port:            req.Port,
		StartedAt:       now,
		LastSeenAt:      now,
		ExpiresAt:       now.Add(sessionTokenTTL),
		Connected:       true,
		Limits:          req.Limits,
	}

	procCtx, cancel := context.WithCancel(ctx)
	proc, err := s.launcher.Launch(procCtx, meta, req.Command, req.Env)
	if err != nil {
		cancel()
		return SessionMetadata{}, mapLaunchError(err)
	}
	state := &sessionState{meta: meta, proc: proc, cancel: cancel}

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
	return meta, nil
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
		if err := s.store.Upsert(meta); err != nil {
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

func (s *Service) authorize(sessionID string, token string) (*sessionState, error) {
	tokenHash := s.tokenHash(strings.TrimSpace(token))
	s.mu.RLock()
	sid, ok := s.tokens[tokenHash]
	if !ok || sid != sessionID {
		s.mu.RUnlock()
		return nil, ErrUnauthorized
	}
	st, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotFound
	}
	if s.isExpired(st.meta) {
		return nil, ErrSessionExpired
	}
	return st, nil
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
