package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeStore struct{ rows map[string]SessionMetadata }

func (f *fakeStore) Upsert(m SessionMetadata) error {
	if f.rows == nil {
		f.rows = map[string]SessionMetadata{}
	}
	f.rows[m.SessionID] = m
	return nil
}
func (f *fakeStore) ByToken(token string) (SessionMetadata, error) {
	for _, m := range f.rows {
		if m.ResumeToken == token {
			return m, nil
		}
	}
	return SessionMetadata{}, ErrSessionNotFound
}

type fakeProc struct {
	done       chan error
	writes     [][]byte
	cols, rows uint16
	closed     bool
}

func (f *fakeProc) Write(p []byte) (int, error) {
	f.writes = append(f.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (f *fakeProc) Resize(c, r uint16) error { f.cols = c; f.rows = r; return nil }
func (f *fakeProc) Close() error {
	f.closed = true
	select {
	case f.done <- nil:
	default:
	}
	return nil
}
func (f *fakeProc) Done() <-chan error { return f.done }

type fakeLauncher struct {
	lastMeta SessionMetadata
	proc     *fakeProc
}

func (f *fakeLauncher) Launch(_ context.Context, meta SessionMetadata, _ []string, _ map[string]string) (Process, error) {
	f.lastMeta = meta
	if f.proc == nil {
		f.proc = &fakeProc{done: make(chan error, 1)}
	}
	return f.proc, nil
}

func TestGatewaySessionLifecycle(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{}
	svc := NewService(launcher, store)
	h := NewHandler(svc).Routes()

	openBody := []byte(`{"user":"alice","host":"example.com","cpu_seconds":1,"memory_bytes":1024,"max_duration_seconds":2}`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewReader(openBody)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("open status=%d body=%s", rec.Code, rec.Body.String())
	}
	var meta SessionMetadata
	if err := json.Unmarshal(rec.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.ResumeToken == "" || meta.SessionID == "" {
		t.Fatalf("missing metadata ids")
	}
	if launcher.lastMeta.Limits.CPUSeconds != 1 || launcher.lastMeta.Limits.MemoryBytes != 1024 || launcher.lastMeta.Limits.MaxDuration != 2*time.Second {
		t.Fatalf("limits not propagated")
	}

	stdinPayload := base64.StdEncoding.EncodeToString([]byte("pwd\n"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", bytes.NewBufferString(`{"data":"`+stdinPayload+`"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("stdin status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/resize", bytes.NewBufferString(`{"cols":120,"rows":40}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("resize status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/gateway/sessions/"+meta.SessionID, nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("close status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGatewayResume(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(&fakeLauncher{}, store)
	h := NewHandler(svc).Routes()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewBufferString(`{"user":"alice","host":"example.com"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("open status=%d", rec.Code)
	}
	var meta SessionMetadata
	_ = json.Unmarshal(rec.Body.Bytes(), &meta)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions/resume", bytes.NewBufferString(`{"resume_token":"`+meta.ResumeToken+`"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", rec.Code, rec.Body.String())
	}
}
