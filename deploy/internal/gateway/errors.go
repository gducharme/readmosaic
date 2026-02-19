package gateway

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

type FriendlyError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Cause   error  `json:"-"`
}

func (e *FriendlyError) Error() string {
	return e.Message
}

func (e *FriendlyError) Unwrap() error { return e.Cause }

func mapLaunchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrSessionNotFound) {
		return err
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		stderr := strings.TrimSpace(string(exitErr.Stderr))
		if strings.Contains(stderr, "Could not resolve hostname") {
			return &FriendlyError{Code: "SSH_HOST_UNREACHABLE", Message: "Unable to reach SSH host. Check hostname and network access.", Cause: err}
		}
		if strings.Contains(stderr, "Permission denied") {
			return &FriendlyError{Code: "SSH_AUTH_FAILED", Message: "SSH authentication failed. Verify keys or credentials.", Cause: err}
		}
		return &FriendlyError{Code: "SSH_EXIT", Message: "SSH process terminated unexpectedly.", Cause: err}
	}
	if strings.Contains(err.Error(), "executable file not found") {
		return &FriendlyError{Code: "SPAWN_BINARY_NOT_FOUND", Message: "Terminal worker binary is missing on server.", Cause: err}
	}
	return &FriendlyError{Code: "SESSION_IO_FAILURE", Message: fmt.Sprintf("Terminal session failed: %v", err), Cause: err}
}
