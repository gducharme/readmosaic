package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrInvalidRequest  = errors.New("invalid request")
)

type SessionLimits struct {
	CPUSeconds  int           `json:"cpu_seconds"`
	MemoryBytes uint64        `json:"memory_bytes"`
	MaxDuration time.Duration `json:"max_duration"`
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
	SessionID   string        `json:"session_id"`
	ResumeToken string        `json:"resume_token"`
	User        string        `json:"user"`
	Host        string        `json:"host"`
	Port        int           `json:"port"`
	StartedAt   time.Time     `json:"started_at"`
	LastSeenAt  time.Time     `json:"last_seen_at"`
	Connected   bool          `json:"connected"`
	Limits      SessionLimits `json:"limits"`
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
	return &Service{launcher: launcher, store: store, now: time.Now, sessions: map[string]*sessionState{}, tokens: map[string]string{}}
}

func (s *Service) OpenSession(ctx context.Context, req OpenSessionRequest) (SessionMetadata, error) {
	if req.User == "" || req.Host == "" {
		return SessionMetadata{}, ErrInvalidRequest
	}
	if req.Port == 0 {
		req.Port = 22
	}
	started := s.now().UTC()
	sessionID, err := randomID()
	if err != nil {
		return SessionMetadata{}, fmt.Errorf("session id: %w", err)
	}
	token, err := randomID()
	if err != nil {
		return SessionMetadata{}, fmt.Errorf("resume token: %w", err)
	}
	meta := SessionMetadata{SessionID: sessionID, ResumeToken: token, User: req.User, Host: req.Host, Port: req.Port, StartedAt: started, LastSeenAt: started, Connected: true, Limits: req.Limits}
	procCtx, cancel := context.WithCancel(ctx)
	proc, err := s.launcher.Launch(procCtx, meta, req.Command, req.Env)
	if err != nil {
		cancel()
		return SessionMetadata{}, mapLaunchError(err)
	}

	state := &sessionState{meta: meta, proc: proc, cancel: cancel}
	s.mu.Lock()
	s.sessions[sessionID] = state
	s.tokens[token] = sessionID
	s.mu.Unlock()
	_ = s.store.Upsert(meta)
	go s.watch(sessionID, proc)
	return meta, nil
}

func (s *Service) ResumeSession(token string) (SessionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid, ok := s.tokens[token]
	if !ok {
		meta, err := s.store.ByToken(token)
		if err != nil {
			return SessionMetadata{}, ErrSessionNotFound
		}
		meta.LastSeenAt = s.now().UTC()
		_ = s.store.Upsert(meta)
		return meta, nil
	}
	st, ok := s.sessions[sid]
	if !ok {
		return SessionMetadata{}, ErrSessionNotFound
	}
	st.meta.LastSeenAt = s.now().UTC()
	_ = s.store.Upsert(st.meta)
	return st.meta, nil
}

func (s *Service) WriteStdin(sessionID string, payload []byte) error {
	s.mu.RLock()
	st, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return ErrSessionNotFound
	}
	if _, err := st.proc.Write(payload); err != nil {
		return mapLaunchError(err)
	}
	return nil
}

func (s *Service) Resize(sessionID string, cols, rows uint16) error {
	s.mu.RLock()
	st, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return ErrSessionNotFound
	}
	if err := st.proc.Resize(cols, rows); err != nil {
		return mapLaunchError(err)
	}
	return nil
}

func (s *Service) Close(sessionID string) error {
	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if ok {
		delete(s.sessions, sessionID)
		delete(s.tokens, st.meta.ResumeToken)
	}
	s.mu.Unlock()
	if !ok {
		return ErrSessionNotFound
	}
	st.cancel()
	_ = st.proc.Close()
	st.meta.Connected = false
	st.meta.LastSeenAt = s.now().UTC()
	_ = s.store.Upsert(st.meta)
	return nil
}

func (s *Service) watch(sessionID string, proc Process) {
	<-proc.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	st.meta.Connected = false
	st.meta.LastSeenAt = s.now().UTC()
	delete(s.sessions, sessionID)
	delete(s.tokens, st.meta.ResumeToken)
	_ = s.store.Upsert(st.meta)
}

func randomID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
