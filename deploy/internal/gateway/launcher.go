package gateway

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

type SSHLauncher struct {
	SSHPath     string
	PrlimitPath string
}

func NewSSHLauncher() *SSHLauncher {
	return &SSHLauncher{SSHPath: "/usr/bin/ssh", PrlimitPath: "/usr/bin/prlimit"}
}

func (l *SSHLauncher) Launch(ctx context.Context, meta SessionMetadata, command []string, env map[string]string) (Process, error) {
	sshPath := l.SSHPath
	if sshPath == "" {
		sshPath = "/usr/bin/ssh"
	}
	baseArgs := []string{"-o", "BatchMode=yes", "-p", strconv.Itoa(meta.Port), fmt.Sprintf("%s@%s", meta.User, meta.Host), "--", "bash", "-l"}

	cmdPath := sshPath
	cmdArgs := baseArgs
	if meta.Limits.CPUSeconds > 0 || meta.Limits.MemoryBytes > 0 {
		prlimitPath := l.PrlimitPath
		if prlimitPath == "" {
			prlimitPath = "/usr/bin/prlimit"
		}
		args := make([]string, 0, 12)
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
	cmd := exec.CommandContext(ctx, cmdPath, cmdArgs...)
	cmd.Env = sanitizedEnv(env)
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return nil, err
	}
	proc := &sshProcess{cmd: cmd, pty: ptmx, done: make(chan error, 1)}
	if meta.Limits.MaxDurationSeconds > 0 {
		proc.timer = time.AfterFunc(time.Duration(meta.Limits.MaxDurationSeconds)*time.Second, func() {
			_ = proc.Close()
		})
	}
	go func() {
		proc.done <- cmd.Wait()
		close(proc.done)
		_ = proc.Close()
	}()
	_ = command // intentionally ignored; command is server-fixed for safety.
	return proc, nil
}

func sanitizedEnv(extra map[string]string) []string {
	env := []string{
		"LANG=C.UTF-8",
		"LC_ALL=C.UTF-8",
		"TERM=xterm-256color",
		"PATH=/usr/bin:/bin",
		"HOME=/tmp",
	}
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

func (p *sshProcess) Write(data []byte) (int, error) {
	return p.pty.Write(data)
}

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

func (p *sshProcess) Done() <-chan error {
	return p.done
}
