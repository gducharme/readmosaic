package gateway

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

type SSHLauncher struct {
	SSHPath        string
	PrlimitPath    string
	KnownHostsPath string
	StrictHostKey  string
}

func NewSSHLauncher() *SSHLauncher {
	knownHosts := os.Getenv("GATEWAY_SSH_KNOWN_HOSTS")
	if knownHosts == "" {
		knownHosts = "/tmp/gateway_known_hosts"
	}
	strictHostKey := strings.TrimSpace(os.Getenv("GATEWAY_SSH_STRICT_HOST_KEY_CHECKING"))
	if strictHostKey == "" {
		strictHostKey = "accept-new"
	}
	return &SSHLauncher{SSHPath: "/usr/bin/ssh", PrlimitPath: "/usr/bin/prlimit", KnownHostsPath: knownHosts, StrictHostKey: strictHostKey}
}

func (l *SSHLauncher) Launch(ctx context.Context, meta SessionMetadata, command []string, env map[string]string) (Process, error) {
	cmdPath, cmdArgs := l.commandSpec(meta)
	log.Printf("level=info event=gateway_process_launch_attempt session=%s user=%s host=%s port=%d command_path=%q args_count=%d", meta.SessionID, meta.User, meta.Host, meta.Port, cmdPath, len(cmdArgs))
	cmd := exec.CommandContext(ctx, cmdPath, cmdArgs...)
	cmd.Env = sanitizedEnv(env)
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("level=warn event=gateway_process_launch_failed session=%s user=%s host=%s port=%d error=%v", meta.SessionID, meta.User, meta.Host, meta.Port, err)
		return nil, err
	}
	log.Printf("level=info event=gateway_process_launch_started session=%s pid=%d", meta.SessionID, cmd.Process.Pid)
	proc := &sshProcess{cmd: cmd, pty: ptmx, done: make(chan error, 1)}
	if meta.Limits.MaxDurationSeconds > 0 {
		proc.timer = time.AfterFunc(time.Duration(meta.Limits.MaxDurationSeconds)*time.Second, func() {
			log.Printf("level=warn event=gateway_process_timeout session=%s max_duration_seconds=%d", meta.SessionID, meta.Limits.MaxDurationSeconds)
			_ = proc.Close()
		})
	}
	go func() {
		waitErr := cmd.Wait()
		if waitErr != nil {
			log.Printf("level=warn event=gateway_process_wait_done session=%s pid=%d error=%v", meta.SessionID, cmd.Process.Pid, waitErr)
		} else {
			log.Printf("level=info event=gateway_process_wait_done session=%s pid=%d", meta.SessionID, cmd.Process.Pid)
		}
		proc.done <- waitErr
		close(proc.done)
		_ = proc.Close()
	}()
	_ = command
	return proc, nil
}

func (l *SSHLauncher) commandSpec(meta SessionMetadata) (string, []string) {
	sshPath := l.SSHPath
	if sshPath == "" {
		sshPath = "/usr/bin/ssh"
	}
	knownHosts := l.KnownHostsPath
	if knownHosts == "" {
		knownHosts = "/etc/ssh/ssh_known_hosts"
	}
	strictHostKey := normalizeStrictHostKeyChecking(l.StrictHostKey)
	baseArgs := []string{"-tt", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=" + strictHostKey, "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no", "-o", "UserKnownHostsFile=" + knownHosts, "-p", strconv.Itoa(meta.Port), fmt.Sprintf("%s@%s", meta.User, meta.Host), "--", "bash", "--noprofile", "--norc", "-i"}

	cmdPath := sshPath
	cmdArgs := baseArgs
	if meta.Limits.CPUSeconds > 0 || meta.Limits.MemoryBytes > 0 {
		prlimitPath := l.PrlimitPath
		if prlimitPath == "" {
			prlimitPath = "/usr/bin/prlimit"
		}
		args := make([]string, 0, 16)
		if meta.Limits.CPUSeconds > 0 {
			args = append(args, "--cpu="+strconv.Itoa(meta.Limits.CPUSeconds))
		}
		if meta.Limits.MemoryBytes > 0 {
			args = append(args, "--as="+strconv.FormatUint(meta.Limits.MemoryBytes, 10))
		}
		args = append(args, "--", sshPath)
		args = append(args, baseArgs...)
		cmdPath = prlimitPath
		cmdArgs = args
	}
	return cmdPath, cmdArgs
}

func sanitizedEnv(extra map[string]string) []string {
	env := []string{"LANG=C.UTF-8", "LC_ALL=C.UTF-8", "TERM=xterm-256color", "PATH=/usr/bin:/bin", "HOME=/tmp"}
	allow := map[string]struct{}{"LANG": {}, "LC_ALL": {}, "TERM": {}}
	for k, v := range extra {
		if _, ok := allow[k]; ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

type sshProcess struct {
	cmd   *exec.Cmd
	pty   *os.File
	done  chan error
	timer *time.Timer
	once  sync.Once
}

func (p *sshProcess) Read(data []byte) (int, error)  { return p.pty.Read(data) }
func (p *sshProcess) Write(data []byte) (int, error) { return p.pty.Write(data) }
func (p *sshProcess) Resize(cols, rows uint16) error {
	return pty.Setsize(p.pty, &pty.Winsize{Cols: cols, Rows: rows})
}

func (p *sshProcess) Close() error {
	var closeErr error
	p.once.Do(func() {
		if p.timer != nil {
			p.timer.Stop()
		}
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			go func(proc *os.Process) {
				select {
				case <-p.done:
					return
				case <-time.After(2 * time.Second):
					_ = proc.Signal(syscall.SIGKILL)
				}
			}(p.cmd.Process)
		}
		closeErr = p.pty.Close()
	})
	return closeErr
}

func (p *sshProcess) Done() <-chan error { return p.done }

func normalizeStrictHostKeyChecking(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "yes", "no", "accept-new", "ask":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "accept-new"
	}
}
