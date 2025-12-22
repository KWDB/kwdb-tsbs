package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

type Executor struct {
	binPath string
}

type ExecutionResult struct {
	Output string
	Error  error
}

func NewExecutor(binPath string) *Executor {
	return &Executor{binPath: binPath}
}

func (e *Executor) ExecuteWithOutput(ctx context.Context, cmd *exec.Cmd) (*ExecutionResult, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return &ExecutionResult{
			Output: stderr.String(),
			Error:  err,
		}, fmt.Errorf("command failed: %w", err)
	}

	return &ExecutionResult{
		Output: stdout.String(),
		Error:  nil,
	}, nil
}

func (e *Executor) GetBinaryPath(name string) string {
	return filepath.Join(e.binPath, name)
}
