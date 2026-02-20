package gateway

import (
	"context"
	"testing"
	"time"
)

type ctxBoundProcess struct {
	done chan error
}

func (p *ctxBoundProcess) Read(_ []byte) (int, error)     { <-p.done; return 0, context.Canceled }
func (p *ctxBoundProcess) Write(data []byte) (int, error) { return len(data), nil }
func (p *ctxBoundProcess) Resize(_, _ uint16) error       { return nil }
func (p *ctxBoundProcess) Close() error {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
	return nil
}
func (p *ctxBoundProcess) Done() <-chan error { return p.done }

type ctxBoundLauncher struct{}

func (l *ctxBoundLauncher) Launch(ctx context.Context, _ SessionMetadata, _ []string, _ map[string]string) (Process, error) {
	proc := &ctxBoundProcess{done: make(chan error)}
	go func() {
		<-ctx.Done()
		select {
		case <-proc.done:
		default:
			close(proc.done)
		}
	}()
	return proc, nil
}

type noopStore struct{}

func (n *noopStore) Upsert(SessionMetadata) error { return nil }
func (n *noopStore) ByTokenHash(string) (SessionMetadata, error) {
	return SessionMetadata{}, ErrSessionNotFound
}

func TestOpenSessionNotBoundToCallerContext(t *testing.T) {
	svc, err := NewServiceWithSecret(&ctxBoundLauncher{}, &noopStore{}, []byte("0123456789abcdef0123456789abcdef"), nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	meta, err := svc.OpenSession(ctx, OpenSessionRequest{User: "alice", Host: "example.com", Port: 22})
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	cancel()
	time.Sleep(20 * time.Millisecond)

	if err := svc.WriteStdin(meta.SessionID, meta.ResumeToken, []byte("pwd\n")); err != nil {
		t.Fatalf("session should remain active after caller context cancel, got error: %v", err)
	}
}
