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
)

type fakeMaxSessionsContext struct {
	context.Context
	mu     sync.Mutex
	values map[any]any
	remote net.Addr
	local  net.Addr
}

func newFakeMaxSessionsContext(ctx context.Context, remote net.Addr) *fakeMaxSessionsContext {
	return &fakeMaxSessionsContext{
		Context: ctx,
		values:  map[any]any{},
		remote:  remote,
		local:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222},
	}
}

func (f *fakeMaxSessionsContext) Lock()                         { f.mu.Lock() }
func (f *fakeMaxSessionsContext) Unlock()                       { f.mu.Unlock() }
func (f *fakeMaxSessionsContext) User() string                  { return "guest" }
func (f *fakeMaxSessionsContext) SessionID() string             { return "test-session" }
func (f *fakeMaxSessionsContext) ClientVersion() string         { return "ssh-test-client" }
func (f *fakeMaxSessionsContext) ServerVersion() string         { return "ssh-test-server" }
func (f *fakeMaxSessionsContext) RemoteAddr() net.Addr          { return f.remote }
func (f *fakeMaxSessionsContext) LocalAddr() net.Addr           { return f.local }
func (f *fakeMaxSessionsContext) Permissions() *ssh.Permissions { return &ssh.Permissions{} }
func (f *fakeMaxSessionsContext) SetValue(key, value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[key] = value
}
func (f *fakeMaxSessionsContext) Value(key interface{}) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

type fakeMaxSessionsSession struct {
	ctx    *fakeMaxSessionsContext
	remote net.Addr
	writes []string
}

func newFakeMaxSessionsSession(ctx context.Context, remote net.Addr) *fakeMaxSessionsSession {
	return &fakeMaxSessionsSession{ctx: newFakeMaxSessionsContext(ctx, remote), remote: remote}
}

func (f *fakeMaxSessionsSession) Read(_ []byte) (int, error) { return 0, io.EOF }
func (f *fakeMaxSessionsSession) Write(p []byte) (int, error) {
	f.writes = append(f.writes, string(p))
	return len(p), nil
}
func (f *fakeMaxSessionsSession) Close() error                                   { return nil }
func (f *fakeMaxSessionsSession) CloseWrite() error                              { return nil }
func (f *fakeMaxSessionsSession) SendRequest(string, bool, []byte) (bool, error) { return false, nil }
func (f *fakeMaxSessionsSession) Stderr() io.ReadWriter                          { return &bytes.Buffer{} }
func (f *fakeMaxSessionsSession) User() string                                   { return "guest" }
func (f *fakeMaxSessionsSession) RemoteAddr() net.Addr                           { return f.remote }
func (f *fakeMaxSessionsSession) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222}
}
func (f *fakeMaxSessionsSession) Environ() []string            { return nil }
func (f *fakeMaxSessionsSession) Exit(int) error               { return nil }
func (f *fakeMaxSessionsSession) Command() []string            { return nil }
func (f *fakeMaxSessionsSession) RawCommand() string           { return "" }
func (f *fakeMaxSessionsSession) Subsystem() string            { return "" }
func (f *fakeMaxSessionsSession) PublicKey() ssh.PublicKey     { return nil }
func (f *fakeMaxSessionsSession) Context() ssh.Context         { return f.ctx }
func (f *fakeMaxSessionsSession) Permissions() ssh.Permissions { return ssh.Permissions{} }
func (f *fakeMaxSessionsSession) EmulatedPty() bool            { return false }
func (f *fakeMaxSessionsSession) Pty() (ssh.Pty, <-chan ssh.Window, bool) {
	return ssh.Pty{}, nil, false
}
func (f *fakeMaxSessionsSession) Signals(chan<- ssh.Signal) {}
func (f *fakeMaxSessionsSession) Break(chan<- bool)         {}

func TestMaxSessionsMiddlewareReleasesSlotOnContextDone(t *testing.T) {
	mw := MaxSessionsMiddleware(1)

	blockCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := newFakeMaxSessionsSession(blockCtx, &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 22})
	second := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.11"), Port: 22})

	releaseHandler := make(chan struct{})
	handler := mw(func(ssh.Session) {
		<-releaseHandler
	})

	done := make(chan struct{})
	go func() {
		handler(first)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	handler(second)
	if len(second.writes) != 1 || second.writes[0] != "max sessions exceeded\n" {
		t.Fatalf("unexpected overflow writes: %#v", second.writes)
	}

	cancel()
	time.Sleep(20 * time.Millisecond)
	close(releaseHandler)
	<-done

	third := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.12"), Port: 22})
	called := false
	allow := mw(func(ssh.Session) { called = true })
	allow(third)
	if !called {
		t.Fatal("expected slot to be available after context cancellation")
	}
}

func TestMaxSessionsMiddlewareRecoversFromPanicAndReleasesSlot(t *testing.T) {
	mw := MaxSessionsMiddleware(1)
	panicSession := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.20"), Port: 22})

	mw(func(ssh.Session) { panic("boom") })(panicSession)
	time.Sleep(20 * time.Millisecond)

	followUp := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.21"), Port: 22})
	called := false
	mw(func(ssh.Session) { called = true })(followUp)
	if !called {
		t.Fatal("expected slot to be released after panic")
	}
}

func TestMaxSessionsMiddlewareContextDoneAndHandlerReturnDoNotDoubleRelease(t *testing.T) {
	mw := MaxSessionsMiddleware(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := newFakeMaxSessionsSession(ctx, &net.TCPAddr{IP: net.ParseIP("203.0.113.30"), Port: 22})
	releaseFirst := make(chan struct{})
	h := mw(func(ssh.Session) { <-releaseFirst })
	doneFirst := make(chan struct{})
	go func() {
		h(first)
		close(doneFirst)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	close(releaseFirst)
	<-doneFirst

	second := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.31"), Port: 22})
	third := newFakeMaxSessionsSession(context.Background(), &net.TCPAddr{IP: net.ParseIP("203.0.113.32"), Port: 22})
	releaseSecond := make(chan struct{})
	gate := mw(func(ssh.Session) { <-releaseSecond })
	doneSecond := make(chan struct{})
	go func() {
		gate(second)
		close(doneSecond)
	}()

	time.Sleep(20 * time.Millisecond)
	gate(third)
	if len(third.writes) != 1 || third.writes[0] != "max sessions exceeded\n" {
		t.Fatalf("unexpected overflow writes: %#v", third.writes)
	}

	close(releaseSecond)
	<-doneSecond
}
