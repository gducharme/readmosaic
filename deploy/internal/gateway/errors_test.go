package gateway

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestMapLaunchErrorMapsMissingBinaryFromExecErrNotFound(t *testing.T) {
	err := mapLaunchError(exec.ErrNotFound)
	friendly, ok := err.(*FriendlyError)
	if !ok {
		t.Fatalf("expected FriendlyError, got %T", err)
	}
	if friendly.Code != "SPAWN_BINARY_NOT_FOUND" {
		t.Fatalf("code=%s", friendly.Code)
	}
}

func TestMapLaunchErrorMapsMissingBinaryFromPathError(t *testing.T) {
	pathErr := &os.PathError{Op: "fork/exec", Path: "/usr/bin/ssh", Err: os.ErrNotExist}
	err := mapLaunchError(pathErr)
	friendly, ok := err.(*FriendlyError)
	if !ok {
		t.Fatalf("expected FriendlyError, got %T", err)
	}
	if friendly.Code != "SPAWN_BINARY_NOT_FOUND" {
		t.Fatalf("code=%s", friendly.Code)
	}
}

func TestMapLaunchErrorDoesNotMisclassifyUnrelatedNotExist(t *testing.T) {
	err := mapLaunchError(errors.New("config file: no such file or directory"))
	friendly, ok := err.(*FriendlyError)
	if !ok {
		t.Fatalf("expected FriendlyError, got %T", err)
	}
	if friendly.Code != "SESSION_IO_FAILURE" {
		t.Fatalf("code=%s", friendly.Code)
	}
}
