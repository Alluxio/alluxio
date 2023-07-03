/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/palantir/stacktrace"
)

const bash = "bash"

// BashBuilder is a builder pattern struct for constructing and executing bash commands
type BashBuilder struct {
	// required input
	cmd string // command to run with bash -c

	// optional inputs
	context context.Context // run command with given context if provided
	dir     string          // directory to execute bash command from
	env     []string        // environment variables to append to current env from os.Environ()

	// output handling
	ignoreExitError bool      // does not return an error if the returned error is of type *exec.ExitError
	stderr          io.Writer // streams stderr into given Writer. note this affects the evaluation of the returned error
	stdout          io.Writer // streams stdout into given Writer

	// outputs
	ExitError *exec.ExitError // set if the returned error from command execution is of type *exec.ExitError
}

func New(cmd string) *BashBuilder {
	return &BashBuilder{
		cmd: cmd,
	}
}

func NewF(format string, args ...interface{}) *BashBuilder {
	return New(fmt.Sprintf(format, args...))
}

// CombinedOutput builds the given command, starts and waits for the command to successfully complete
// It returns the combined contents of stderr and stdout as a []byte
// The command returns an error if there is a non-zero exit code, unless ignoreExitError is enabled
func CombinedOutput(cmd string) ([]byte, error) {
	b := New(cmd)
	out, err := b.Cmd().CombinedOutput()
	if err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return nil, stacktrace.Propagate(err, "error running command: %v\nerror: %v", b.String(), string(out))
		}
	}
	return out, nil
}

// CombinedOutputF executes the formatted string as CombinedOutput()
func CombinedOutputF(format string, args ...interface{}) ([]byte, error) {
	return CombinedOutput(fmt.Sprintf(format, args...))
}

// Output builds the given command, starts and waits for the command to successfully complete
// It returns the contents of stdout as a []byte
// The command returns an error if there is any stderr or non-zero exit code, unless ignoreExitError is enabled
func Output(cmd string) ([]byte, error) {
	b := New(cmd)
	out, err := b.Cmd().Output()
	if err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return nil, stacktrace.Propagate(err, "error running command")
		}
	}
	return out, nil
}

// OutputF executes the formatted string as Output()
func OutputF(format string, args ...interface{}) ([]byte, error) {
	return Output(fmt.Sprintf(format, args...))
}

// Run builds the given command, starts, and waits for the command to successfully complete
// The command returns an error if there is any stderr or non-zero exit code, unless ignoreExitError is enabled
func Run(cmd string) error {
	b := New(cmd)
	if out, err := b.Cmd().CombinedOutput(); err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return stacktrace.Propagate(err, "error running command: %v\nerror: %v", b.String(), string(out))
		}
	}
	return nil
}

// RunF executes the formatted string as Run()
func RunF(format string, args ...interface{}) error {
	return Run(fmt.Sprintf(format, args...))
}

func (b *BashBuilder) Command() string {
	return b.cmd
}

func (b *BashBuilder) WithContext(ctx context.Context) *BashBuilder {
	b.context = ctx
	return b
}

func (b *BashBuilder) Env(key string, value interface{}) *BashBuilder {
	b.env = append(b.env, fmt.Sprintf("%s=%v", key, value))
	return b
}

func (b *BashBuilder) IgnoreExitError() *BashBuilder {
	b.ignoreExitError = true
	return b
}

func (b *BashBuilder) SetStderr(stderr io.Writer) *BashBuilder {
	b.stderr = stderr
	return b
}

func (b *BashBuilder) SetStdout(stdout io.Writer) *BashBuilder {
	b.stdout = stdout
	return b
}

func (b *BashBuilder) WithDir(dir string) *BashBuilder {
	b.dir = dir
	return b
}

func (b *BashBuilder) String() string {
	return b.cmd
}

func (b *BashBuilder) Cmd() *exec.Cmd {
	cmdStr := b.cmd

	var ret *exec.Cmd
	if b.context != nil {
		ret = exec.CommandContext(b.context, bash, "-c", cmdStr)
	} else {
		ret = exec.Command(bash, "-c", cmdStr)
	}
	if b.dir != "" {
		ret.Dir = b.dir
	}
	ret.Env = os.Environ()
	for _, envPv := range b.env {
		ret.Env = append(ret.Env, envPv)
	}
	if b.stderr != nil {
		ret.Stderr = b.stderr
	}
	if b.stdout != nil {
		ret.Stdout = b.stdout
	}
	return ret
}

// CombinedOutput builds the given command, starts and waits for the command to successfully complete
// It returns the combined contents of stderr and stdout as a []byte
// The command returns an error if there is a non-zero exit code, unless ignoreExitError is enabled
func (b *BashBuilder) CombinedOutput() ([]byte, error) {
	if b.stdout != nil || b.stderr != nil {
		return nil, stacktrace.NewError("cannot ask for combined output when stdout or stderr has been set")
	}
	combinedOut, err := b.Cmd().CombinedOutput()
	if err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return nil, stacktrace.Propagate(err, "error running command: %v\n error: %v", b.String(), string(combinedOut))
		}
	}
	return combinedOut, nil
}

// Output builds the given command, starts and waits for the command to successfully complete
// It returns the contents of stdout as a []byte
// The command returns an error if there is any stderr or non-zero exit code, unless ignoreExitError is enabled
func (b *BashBuilder) Output() ([]byte, error) {
	if b.stdout != nil {
		return nil, stacktrace.NewError("cannot ask for output when stdout has been set")
	}
	out, err := b.Cmd().Output()
	if err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return nil, stacktrace.Propagate(err, "error running command")
		}
	}
	return out, nil
}

// Run builds the given command, starts, and waits for the command to successfully complete
// The command returns an error if there is any stderr or non-zero exit code, unless ignoreExitError is enabled
func (b *BashBuilder) Run() error {
	if err := b.Cmd().Run(); err != nil {
		if handledErr := b.handleExitError(err); handledErr != nil {
			return stacktrace.Propagate(err, "error running command")
		}
	}
	return nil
}

func (b *BashBuilder) handleExitError(err error) error {
	if ee, ok := err.(*exec.ExitError); ok {
		b.ExitError = ee
		if b.ignoreExitError {
			return nil
		}
	}
	return err
}
