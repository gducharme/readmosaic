package server

import (
	"bytes"
	"context"
	"io"
	"net"
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
func (f *fakeDefaultHandlerSession) Close() error      { return nil }
func (f *fakeDefaultHandlerSession) CloseWrite() error { return nil }
func (f *fakeDefaultHandlerSession) SendRequest(string, bool, []byte) (bool, error) {
	return false, nil
}
func (f *fakeDefaultHandlerSession) Stderr() io.ReadWriter { return &bytes.Buffer{} }
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
	if f.closer != nil {
		_ = f.closer.Close()
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

	time.Sleep(80 * time.Millisecond)
	if code, ok := sess.recordedExitCode(); ok {
		t.Fatalf("expected no immediate exit code, got %d", code)
	}

	select {
	case <-done:
		t.Fatal("session handler exited immediately; expected runtime loop to remain active")
	default:
	}

	cancel()
	sess.closeInput()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler did not exit after context cancellation")
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

func TestDefaultHandlerRouteSelectionByUsernamePolicy(t *testing.T) {
	tests := []struct {
		name       string
		user       string
		wantMarker string
	}{
		{name: "vector-user", user: "west", wantMarker: "VECTOR FLOW ACTIVE [west]"},
		{name: "triage-user", user: "read", wantMarker: "TRIAGE FLOW ACTIVE [read]"},
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
