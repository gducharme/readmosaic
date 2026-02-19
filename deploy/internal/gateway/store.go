package gateway

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type MetadataStore interface {
	Upsert(SessionMetadata) error
	ByToken(token string) (SessionMetadata, error)
}

type FileMetadataStore struct {
	path string
	mu   sync.Mutex
}

func NewFileMetadataStore(path string) *FileMetadataStore {
	if path == "" {
		path = filepath.Join(os.TempDir(), "mosaic-terminal-sessions.json")
	}
	return &FileMetadataStore{path: path}
}

func (s *FileMetadataStore) Upsert(meta SessionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.readLocked()
	if err != nil {
		return err
	}
	rows[meta.SessionID] = meta
	return s.writeLocked(rows)
}

func (s *FileMetadataStore) ByToken(token string) (SessionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.readLocked()
	if err != nil {
		return SessionMetadata{}, err
	}
	for _, meta := range rows {
		if meta.ResumeToken == token {
			return meta, nil
		}
	}
	return SessionMetadata{}, errors.New("token not found")
}

func (s *FileMetadataStore) readLocked() (map[string]SessionMetadata, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]SessionMetadata{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return map[string]SessionMetadata{}, nil
	}
	rows := map[string]SessionMetadata{}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *FileMetadataStore) writeLocked(rows map[string]SessionMetadata) error {
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}
