package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
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
func (f *fakeStore) ByTokenHash(tokenHash string) (SessionMetadata, error) {
	for _, m := range f.rows {
		if m.ResumeTokenHash == tokenHash {
			return m, nil
		}
	}
	return SessionMetadata{}, ErrSessionNotFound
}

type fakeProc struct {
	done       chan error
	reads      chan []byte
	writes     [][]byte
	cols, rows uint16
	closed     bool
}

func (f *fakeProc) Read(p []byte) (int, error) {
	chunk, ok := <-f.reads
	if !ok {
		return 0, io.EOF
	}
	n := copy(p, chunk)
	return n, nil
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
		f.proc = &fakeProc{done: make(chan error, 1), reads: make(chan []byte, 8)}
	}
	return f.proc, nil
}

func mustNewService(t *testing.T, launcher Launcher, store MetadataStore) *Service {
	t.Helper()
	svc, err := NewServiceWithSecret(launcher, store, []byte("0123456789abcdef0123456789abcdef"), nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc
}

func openSession(t *testing.T, h http.Handler) SessionMetadata {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewBufferString(`{"user":"alice","host":"example.com","cpu_seconds":1,"memory_bytes":1024,"max_duration_seconds":2}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("open status=%d body=%s", rec.Code, rec.Body.String())
	}
	var meta SessionMetadata
	if err := json.Unmarshal(rec.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.ResumeToken == "" {
		t.Fatalf("resume token must be returned on open")
	}
	return meta
}

func authedRequest(method, path, token, body string) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func TestGatewaySessionLifecycle(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{}
	svc := mustNewService(t, launcher, store)
	h := NewHandler(svc).Routes()

	meta := openSession(t, h)
	if launcher.lastMeta.Limits.CPUSeconds != 1 || launcher.lastMeta.Limits.MemoryBytes != 1024 || launcher.lastMeta.Limits.MaxDurationSeconds != 2 {
		t.Fatalf("limits not propagated")
	}

	stdinPayload := base64.StdEncoding.EncodeToString([]byte("pwd\n"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", meta.ResumeToken, `{"data":"`+stdinPayload+`"}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("stdin status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/resize", meta.ResumeToken, `{"cols":120,"rows":40}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("resize status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodDelete, "/gateway/sessions/"+meta.SessionID, meta.ResumeToken, ""))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("close status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGatewayRequestLogging(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{}
	h := NewHandler(mustNewService(t, launcher, store)).Routes()

	var logBuf bytes.Buffer
	originalWriter := log.Writer()
	log.SetOutput(&logBuf)
	t.Cleanup(func() { log.SetOutput(originalWriter) })

	req := httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewBufferString(`{"user":"alice","host":"example.com"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("open status=%d body=%s", rec.Code, rec.Body.String())
	}

	line := logBuf.String()
	for _, expected := range []string{"event=gateway_http_request", "method=POST", "path=\"/gateway/sessions\"", "status=201"} {
		if !strings.Contains(line, expected) {
			t.Fatalf("missing %q in log line: %s", expected, line)
		}
	}
}

func TestGatewayResume(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/resume", meta.ResumeToken, `{}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAuthRequired(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", bytes.NewBufferString(`{"data":""}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewBufferString(`{"user":"alice","host":"example.com","oops":1}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOversizedStdinRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	large := strings.Repeat("a", maxStdinBytes+1)
	payload := base64.StdEncoding.EncodeToString([]byte(large))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", meta.ResumeToken, `{"data":"`+payload+`"}`))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestResizeBoundsRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	for _, body := range []string{`{"cols":0,"rows":10}`, `{"cols":10,"rows":5000}`} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/resize", meta.ResumeToken, body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d resp=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestUnknownSessionAndTokenReturn404(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodDelete, "/gateway/sessions/0123456789abcdef0123456789abcdef", meta.ResumeToken, ""))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("close unknown status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/resume", "missing", `{}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("resume unknown status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSessionActionExtraSegmentsRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin/extra", meta.ResumeToken, `{"data":""}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInvalidPortRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	for _, body := range []string{`{"user":"alice","host":"example.com","port":-1}`, `{"user":"alice","host":"example.com","port":70000}`} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/gateway/sessions", bytes.NewBufferString(body)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d resp=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestWrongTokenForbidden(t *testing.T) {
	svc := mustNewService(t, &fakeLauncher{}, &fakeStore{})
	h := NewHandler(svc).Routes()
	meta := openSession(t, h)
	payload := base64.StdEncoding.EncodeToString([]byte("pwd\n"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", "wrong-token", `{"data":"`+payload+`"}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestTokenFromAnotherSessionForbidden(t *testing.T) {
	svc := mustNewService(t, &fakeLauncher{}, &fakeStore{})
	h := NewHandler(svc).Routes()
	first := openSession(t, h)
	second := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodDelete, "/gateway/sessions/"+first.SessionID, second.ResumeToken, ""))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestExpiredTokenUnauthorized(t *testing.T) {
	svc := mustNewService(t, &fakeLauncher{}, &fakeStore{})
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return fixed }
	h := NewHandler(svc).Routes()
	meta := openSession(t, h)
	svc.now = func() time.Time { return fixed.Add(sessionTokenTTL + time.Minute) }
	payload := base64.StdEncoding.EncodeToString([]byte("pwd\n"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", meta.ResumeToken, `{"data":"`+payload+`"}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDeleteRequiresAuthorization(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/gateway/sessions/"+meta.SessionID, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInvalidSessionIDRejected(t *testing.T) {
	h := NewHandler(mustNewService(t, &fakeLauncher{}, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/not-hex/stdin", meta.ResumeToken, `{"data":""}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestStdinRateLimited(t *testing.T) {
	svc := mustNewService(t, &fakeLauncher{}, &fakeStore{})
	fixed := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return fixed }
	h := NewHandler(svc).Routes()
	meta := openSession(t, h)
	chunk := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 64*1024)))
	for i := 0; i < 4; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", meta.ResumeToken, `{"data":"`+chunk+`"}`))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("i=%d status=%d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodPost, "/gateway/sessions/"+meta.SessionID+"/stdin", meta.ResumeToken, `{"data":"`+chunk+`"}`))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOutputStreamAuthorized(t *testing.T) {
	launcher := &fakeLauncher{}
	handler := NewHandler(mustNewService(t, launcher, &fakeStore{})).Routes()
	meta := openSession(t, handler)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/gateway/sessions/"+meta.SessionID+"/output", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+meta.ResumeToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	launcher.proc.reads <- []byte("\x1b[31mred\x1b[0m")
	close(launcher.proc.reads)

	buf := make([]byte, 256)
	n, err := resp.Body.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	body := string(buf[:n])
	if !strings.Contains(body, "event: output") {
		t.Fatalf("missing output event: %s", body)
	}
	if !strings.Contains(body, base64.StdEncoding.EncodeToString([]byte("\x1b[31mred\x1b[0m"))) {
		t.Fatalf("missing base64 payload: %s", body)
	}
}

func TestOutputStreamUnauthorized(t *testing.T) {
	launcher := &fakeLauncher{}
	h := NewHandler(mustNewService(t, launcher, &fakeStore{})).Routes()
	meta := openSession(t, h)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedRequest(http.MethodGet, "/gateway/sessions/"+meta.SessionID+"/output", "wrong-token", ""))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
