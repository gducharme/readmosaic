package ssh

// Package ssh provides the minimal Session/Handler contracts used by the local Wish shim.
// It is intentionally incomplete and should not be treated as a drop-in replacement for upstream.
import "context"

type Handler func(Session)

type Session interface {
	User() string
	Context() context.Context
	SetValue(key any, value any)
	Write(p []byte) (n int, err error)
}
