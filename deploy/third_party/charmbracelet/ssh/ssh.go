package ssh

import "context"

type Handler func(Session)

type Session interface {
	User() string
	Context() context.Context
	SetValue(key string, value any)
}
