package gateway

import (
	"os"
	"strings"
	"testing"
)

func TestNormalizeStrictHostKeyChecking(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default empty", input: "", want: "accept-new"},
		{name: "valid yes", input: "yes", want: "yes"},
		{name: "valid no", input: "no", want: "no"},
		{name: "valid ask", input: "ask", want: "ask"},
		{name: "valid mixed case", input: " AcCePt-NeW ", want: "accept-new"},
		{name: "invalid", input: "strict", want: "accept-new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeStrictHostKeyChecking(tt.input); got != tt.want {
				t.Fatalf("normalizeStrictHostKeyChecking(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewSSHLauncherDefaults(t *testing.T) {
	t.Setenv("GATEWAY_SSH_KNOWN_HOSTS", "")
	t.Setenv("GATEWAY_SSH_STRICT_HOST_KEY_CHECKING", "")

	launcher := NewSSHLauncher()
	if launcher.KnownHostsPath != "/tmp/gateway_known_hosts" {
		t.Fatalf("KnownHostsPath = %q, want %q", launcher.KnownHostsPath, "/tmp/gateway_known_hosts")
	}
	if launcher.StrictHostKey != "accept-new" {
		t.Fatalf("StrictHostKey = %q, want %q", launcher.StrictHostKey, "accept-new")
	}
}

func TestNewSSHLauncherFromEnv(t *testing.T) {
	customKnownHosts := "/run/keys/known_hosts"
	customStrictHost := "yes"
	if err := os.Setenv("GATEWAY_SSH_KNOWN_HOSTS", customKnownHosts); err != nil {
		t.Fatalf("set known hosts env: %v", err)
	}
	defer os.Unsetenv("GATEWAY_SSH_KNOWN_HOSTS")
	if err := os.Setenv("GATEWAY_SSH_STRICT_HOST_KEY_CHECKING", customStrictHost); err != nil {
		t.Fatalf("set strict host env: %v", err)
	}
	defer os.Unsetenv("GATEWAY_SSH_STRICT_HOST_KEY_CHECKING")

	launcher := NewSSHLauncher()
	if launcher.KnownHostsPath != customKnownHosts {
		t.Fatalf("KnownHostsPath = %q, want %q", launcher.KnownHostsPath, customKnownHosts)
	}
	if launcher.StrictHostKey != customStrictHost {
		t.Fatalf("StrictHostKey = %q, want %q", launcher.StrictHostKey, customStrictHost)
	}
}

func TestCommandSpecIncludesForcedTTY(t *testing.T) {
	launcher := &SSHLauncher{SSHPath: "/custom/ssh", KnownHostsPath: "/custom/known_hosts", StrictHostKey: "accept-new"}
	meta := SessionMetadata{User: "reader", Host: "example.internal", Port: 2222}

	cmdPath, cmdArgs := launcher.commandSpec(meta)
	if cmdPath != "/custom/ssh" {
		t.Fatalf("cmdPath = %q, want %q", cmdPath, "/custom/ssh")
	}
	if len(cmdArgs) == 0 || cmdArgs[0] != "-tt" {
		t.Fatalf("expected forced tty flag as first arg, got %v", cmdArgs)
	}
}

func TestCommandSpecWithLimitsUsesPrlimit(t *testing.T) {
	launcher := &SSHLauncher{SSHPath: "/custom/ssh", PrlimitPath: "/custom/prlimit", KnownHostsPath: "/custom/known_hosts", StrictHostKey: "accept-new"}
	meta := SessionMetadata{User: "reader", Host: "example.internal", Port: 2222, Limits: SessionLimits{CPUSeconds: 5, MemoryBytes: 1024}}

	cmdPath, cmdArgs := launcher.commandSpec(meta)
	if cmdPath != "/custom/prlimit" {
		t.Fatalf("cmdPath = %q, want %q", cmdPath, "/custom/prlimit")
	}
	joined := strings.Join(cmdArgs, " ")
	if !strings.Contains(joined, "-- /custom/ssh -tt") {
		t.Fatalf("expected prlimit invocation to wrap ssh with forced tty, args=%v", cmdArgs)
	}
}
