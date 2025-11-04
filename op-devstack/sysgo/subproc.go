package sysgo

import (
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/ethereum-optimism/optimism/op-devstack/devtest"
	"github.com/ethereum-optimism/optimism/op-service/logpipe"
)

// SubProcess is a process that can be started and stopped.
type SubProcess struct {
	p   devtest.Scope
	cmd *exec.Cmd

	stdOutLogs logpipe.LogProcessor
	stdErrLogs logpipe.LogProcessor

	mu sync.Mutex
}

// lineBuffer is an io.Writer that buffers data across writes and emits complete lines
// to the provided LogProcessor, preserving log entries that may span multiple writes.
type lineBuffer struct {
	mu   sync.Mutex
	buf  []byte
	proc logpipe.LogProcessor
}

func (lb *lineBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Append new data
	lb.buf = append(lb.buf, p...)

	start := 0
	for i := 0; i < len(lb.buf); i++ {
		if lb.buf[i] == '\n' {
			line := lb.buf[start:i]
			if len(line) > 0 {
				lb.proc(line)
			}
			start = i + 1
		}
	}
	// Keep any partial trailing line
	if start > 0 {
		lb.buf = append([]byte(nil), lb.buf[start:]...)
	}
	return len(p), nil
}

func NewSubProcess(p devtest.Scope, stdOutLogs, stdErrLogs logpipe.LogProcessor) *SubProcess {
	return &SubProcess{
		p:          p,
		stdOutLogs: stdOutLogs,
		stdErrLogs: stdErrLogs,
	}
}

func (sp *SubProcess) Start(cmdPath string, args []string, env []string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.cmd != nil {
		return fmt.Errorf("process is still running (PID: %d)", sp.cmd.Process.Pid)
	}
	sp.p.Logger().Info("Starting subprocess", "cmd", cmdPath, "args", args)

	cmd := exec.Command(cmdPath, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = &lineBuffer{proc: sp.stdOutLogs}
	cmd.Stderr = &lineBuffer{proc: sp.stdErrLogs}
	if err := cmd.Start(); err != nil {
		return err
	}
	sp.cmd = cmd
	sp.p.Cleanup(func() {
		err := sp.Stop(true)
		if err != nil {
			sp.p.Logger().Error("Shutdown error", "err", err)
		}
	})
	return nil
}

// Stop waits for the process to stop, interrupting the process if it has not completed and
// interrupt is true.
func (sp *SubProcess) Stop(interrupt bool) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.cmd == nil {
		return nil // already stopped gracefully
	}

	// If not already done, then try an interrupt first as requested.
	if sp.cmd.ProcessState == nil && interrupt {
		sp.p.Logger().Info("Sending interrupt")
		if err := sp.cmd.Process.Signal(os.Interrupt); err != nil {
			return err
		}
	}

	if _, err := sp.cmd.Process.Wait(); err != nil {
		sp.p.Logger().Warn("Sub-process exited with error", "err", err)
	} else {
		sp.p.Logger().Info("Sub-process gracefully exited")
	}

	sp.cmd = nil
	return nil
}
