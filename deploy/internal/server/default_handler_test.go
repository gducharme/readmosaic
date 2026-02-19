package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/ssh"

	"mosaic-terminal/internal/router"
)

type fakeDefaultHandlerContext struct {
	context.Context
	mu     sync.Mutex
	values map[any]any
	remote net.Addr
	local  net.Addr
}

func newFakeDefaultHandlerContext(ctx context.Context, remote net.Addr) *fakeDefaultHandlerContext {
	return &fakeDefaultHandlerContext{
		Context: ctx,
		values:  map[any]any{},
		remote:  remote,
		local:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222},
	}
}

func (f *fakeDefaultHandlerContext) Lock()                         { f.mu.Lock() }
func (f *fakeDefaultHandlerContext) Unlock()                       { f.mu.Unlock() }
func (f *fakeDefaultHandlerContext) User() string                  { return "guest" }
func (f *fakeDefaultHandlerContext) SessionID() string             { return "session-default-handler" }
func (f *fakeDefaultHandlerContext) ClientVersion() string         { return "ssh-test-client" }
func (f *fakeDefaultHandlerContext) ServerVersion() string         { return "ssh-test-server" }
func (f *fakeDefaultHandlerContext) RemoteAddr() net.Addr          { return f.remote }
func (f *fakeDefaultHandlerContext) LocalAddr() net.Addr           { return f.local }
func (f *fakeDefaultHandlerContext) Permissions() *ssh.Permissions { return &ssh.Permissions{} }
func (f *fakeDefaultHandlerContext) SetValue(key, value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = value
}
func (f *fakeDefaultHandlerContext) Value(key interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

type fakeDefaultHandlerSession struct {
	ctx      *fakeDefaultHandlerContext
	remote   net.Addr
	user     string
	reader   io.Reader
	closer   io.Closer
	pty      ssh.Pty
	hasPTY   bool
	windows  chan ssh.Window
	mu       sync.Mutex
	writes   bytes.Buffer
	stderr   bytes.Buffer
	exitCode *int
}

func newFakeDefaultHandlerSession(ctx context.Context, user string, hasPTY bool, input string) *fakeDefaultHandlerSession {
	remote := &net.TCPAddr{IP: net.ParseIP("203.0.113.60"), Port: 2022}
	reader := io.Reader(bytes.NewBufferString(input))
	var closer io.Closer
	if input == "" {
		r, w := io.Pipe()
		reader = r
		closer = w
	}
	return &fakeDefaultHandlerSession{
		ctx:     newFakeDefaultHandlerContext(ctx, remote),
		remote:  remote,
		user:    user,
		reader:  reader,
		closer:  closer,
		hasPTY:  hasPTY,
		pty:     ssh.Pty{Window: ssh.Window{Width: 80, Height: 24}},
		windows: make(chan ssh.Window, 1),
	}
}

func (f *fakeDefaultHandlerSession) Read(p []byte) (int, error) { return f.reader.Read(p) }
func (f *fakeDefaultHandlerSession) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes.Write(p)
}
func (f *fakeDefaultHandlerSession) Close() error {
	f.closeInput()
	return nil
}
func (f *fakeDefaultHandlerSession) CloseWrite() error {
	f.closeInput()
	return nil
}
func (f *fakeDefaultHandlerSession) SendRequest(string, bool, []byte) (bool, error) {
	return false, nil
}
func (f *fakeDefaultHandlerSession) Stderr() io.ReadWriter { return &f.stderr }
func (f *fakeDefaultHandlerSession) User() string          { return f.user }
func (f *fakeDefaultHandlerSession) RemoteAddr() net.Addr  { return f.remote }
func (f *fakeDefaultHandlerSession) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222}
}
func (f *fakeDefaultHandlerSession) Environ() []string { return nil }
func (f *fakeDefaultHandlerSession) Exit(code int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exitCode = &code
	return nil
}
func (f *fakeDefaultHandlerSession) Command() []string            { return nil }
func (f *fakeDefaultHandlerSession) RawCommand() string           { return "" }
func (f *fakeDefaultHandlerSession) Subsystem() string            { return "" }
func (f *fakeDefaultHandlerSession) PublicKey() ssh.PublicKey     { return nil }
func (f *fakeDefaultHandlerSession) Context() ssh.Context         { return f.ctx }
func (f *fakeDefaultHandlerSession) Permissions() ssh.Permissions { return ssh.Permissions{} }
func (f *fakeDefaultHandlerSession) EmulatedPty() bool            { return false }
func (f *fakeDefaultHandlerSession) Pty() (ssh.Pty, <-chan ssh.Window, bool) {
	return f.pty, f.windows, f.hasPTY
}
func (f *fakeDefaultHandlerSession) Signals(chan<- ssh.Signal) {}
func (f *fakeDefaultHandlerSession) Break(chan<- bool)         {}

func (f *fakeDefaultHandlerSession) output() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes.String()
}

func (f *fakeDefaultHandlerSession) closeInput() {
	f.mu.Lock()
	closer := f.closer
	f.closer = nil
	f.mu.Unlock()

	if closer != nil {
		_ = closer.Close()
	}
}

func (f *fakeDefaultHandlerSession) recordedExitCode() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exitCode == nil {
		return 0, false
	}
	return *f.exitCode, true
}

func routedHandler() ssh.Handler {
	chain := router.MiddlewareFromDescriptors(router.DefaultChain())
	h := ssh.Handler(defaultHandler)
	for i := len(chain) - 1; i >= 0; i-- {
		h = chain[i](h)
	}
	return h
}

func TestDefaultHandlerDoesNotAutoExitOnSuccessfulRuntimeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess := newFakeDefaultHandlerSession(ctx, "west", true, "")
	h := routedHandler()

	done := make(chan struct{})
	go func() {
		h(sess)
		close(done)
	}()

	waitForOutputContains(t, sess, "MOSAIC PROTOCOL", time.Second)
	if code, ok := sess.recordedExitCode(); ok {
		t.Fatalf("expected no immediate exit code, got %d", code)
	}

	select {
	case <-done:
		t.Fatal("session handler exited immediately; expected runtime loop to remain active")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler did not exit after context cancellation")
	}

	if code, ok := sess.recordedExitCode(); ok {
		t.Fatalf("expected no explicit exit code on context cancellation, got %d", code)
	}
}

func TestDefaultHandlerExitsGracefullyOnEOF(t *testing.T) {
	sess := newFakeDefaultHandlerSession(context.Background(), "west", true, "")
	h := routedHandler()

	done := make(chan struct{})
	go func() {
		h(sess)
		close(done)
	}()

	waitForOutputContains(t, sess, "MOSAIC PROTOCOL", time.Second)
	sess.closeInput()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler did not exit after EOF")
	}

	code, ok := sess.recordedExitCode()
	if !ok || code != 0 {
		t.Fatalf("expected graceful EOF exit code 0, got (%d, %v)", code, ok)
	}
}

func TestDefaultHandlerRejectsMissingPTYWithExitCode(t *testing.T) {
	sess := newFakeDefaultHandlerSession(context.Background(), "west", false, "")
	routedHandler()(sess)

	out := sess.output()
	if out != "interactive terminal requires an attached PTY\n" {
		t.Fatalf("unexpected message: %q", out)
	}

	code, ok := sess.recordedExitCode()
	if !ok || code != 1 {
		t.Fatalf("expected exit code 1, got (%d, %v)", code, ok)
	}
}

func TestDefaultHandlerThemeResolveFailureDoesNotRejectSession(t *testing.T) {
	t.Setenv("THEME_VARIANT", "mystery")

	sess := newFakeDefaultHandlerSession(context.Background(), "west", true, "")
	routedHandler()(sess)

	out := sess.output()
	if !strings.Contains(out, "MOSAIC PROTOCOL") {
		t.Fatalf("expected session output despite theme resolve failure")
	}
	if strings.Contains(out, "unknown theme variant") {
		t.Fatalf("raw theme resolution error should not be echoed to session")
	}
	code, ok := sess.recordedExitCode()
	if !ok || code != 0 {
		t.Fatalf("expected normal exit code 0, got (%d, %v)", code, ok)
	}
}

func TestDefaultHandlerRouteSelectionByUsernamePolicy(t *testing.T) {
	tests := []struct {
		name       string
		user       string
		wantMarker string
	}{
		{name: "vector-user", user: "west", wantMarker: "VECTOR FLOW ACTIVE [west]"},
		{name: "triage-user-read", user: "read", wantMarker: "TRIAGE FLOW ACTIVE [read]"},
		{name: "triage-user-archive", user: "archive", wantMarker: "TRIAGE FLOW ACTIVE [archive]"},
	}

	h := routedHandler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sess := newFakeDefaultHandlerSession(context.Background(), tc.user, true, "\x04")
			h(sess)

			out := sess.output()
			if !bytes.Contains([]byte(out), []byte(tc.wantMarker)) {
				t.Fatalf("output missing route marker %q in %q", tc.wantMarker, out)
			}
			code, ok := sess.recordedExitCode()
			if !ok || code != 0 {
				t.Fatalf("expected graceful exit code 0, got (%d, %v)", code, ok)
			}
		})
	}
}

func waitForOutputContains(t *testing.T, sess *fakeDefaultHandlerSession, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if bytes.Contains([]byte(sess.output()), []byte(want)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for output containing %q; got %q", want, sess.output())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestResolveFlowRejectsUnsupportedIdentity(t *testing.T) {
	_, err := resolveFlow(router.Identity{Username: "unknown"})
	if err == nil {
		t.Fatal("expected unsupported identity to return an error")
	}
}

func TestStreamKeysDecodesUTF8AndControls(t *testing.T) {
	input := strings.NewReader("é\n\x7f\x04")
	keys := make(chan string, 8)
	eof := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go streamKeys(ctx, input, keys, eof)

	got := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		select {
		case key := <-keys:
			got = append(got, key)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for key %d", i)
		}
	}

	want := []string{"é", "enter", "backspace", "ctrl+d"}
	if len(got) != len(want) {
		t.Fatalf("unexpected key count: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func TestStreamKeysSwallowsArrowEscapeSequences(t *testing.T) {
	input := strings.NewReader("[Ax")
	keys := make(chan string, 8)
	eof := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go streamKeys(ctx, input, keys, eof)

	select {
	case key := <-keys:
		if key != "x" {
			t.Fatalf("expected only trailing printable key, got %q", key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for key")
	}

	select {
	case extra := <-keys:
		t.Fatalf("unexpected extra key from escape sequence: %q", extra)
	default:
	}
}

func TestStreamKeysSwallowsArrowEscapeSequencesWhenChunked(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	keys := make(chan string, 8)
	eof := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go streamKeys(ctx, r, keys, eof)
	go func() {
		_, _ = w.Write([]byte{0x1b})
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("["))
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("A"))
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte("x"))
		_ = w.Close()
	}()

	select {
	case key := <-keys:
		if key != "x" {
			t.Fatalf("expected only trailing printable key, got %q", key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for key")
	}

	select {
	case extra := <-keys:
		t.Fatalf("unexpected extra key from chunked escape sequence: %q", extra)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestStreamKeysEmitsStandaloneEscOnEOF(t *testing.T) {
	input := strings.NewReader("")
	keys := make(chan string, 8)
	eof := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go streamKeys(ctx, input, keys, eof)

	select {
	case key := <-keys:
		if key != "esc" {
			t.Fatalf("expected esc key, got %q", key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for esc key")
	}
}
