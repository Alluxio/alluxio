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

package job

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Service = &env.Service{
	Name:        "job",
	Description: "Command line tool for interacting with the job service.",
	Commands: []env.Command{
		Load,
	},
}

const (
	progress = "progress"
	stop     = "stop"
	submit   = "submit"
)

var operations = []string{
	progress,
	stop,
	submit,
}

type BaseJobCommand struct {
	*env.BaseJavaCommand

	isProgress bool
	isStop     bool
	isSubmit   bool

	progressFormat  string
	progressVerbose bool
}

func (c *BaseJobCommand) AttachOperationFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&c.isProgress, progress, false, "View progress of submitted job")
	cmd.Flags().BoolVar(&c.isStop, stop, false, "Stop running job")
	cmd.Flags().BoolVar(&c.isSubmit, submit, false, "Submit job")
	cmd.MarkFlagsMutuallyExclusive(operations...)

	cmd.Flags().StringVar(&c.progressFormat, "format", "TEXT", "[progress] Format of output, either TEXT or JSON")
	cmd.Flags().BoolVar(&c.progressVerbose, "verbose", false, "[progress] Verbose output")
}

func (c *BaseJobCommand) OperationWithArgs() ([]string, error) {
	// rely on MarkFlagsMutuallyExclusive to ensure there is at most one boolean switched to true
	if c.isProgress {
		ret := []string{"--" + progress, "--format", c.progressFormat}
		if c.progressVerbose {
			ret = append(ret, "--verbose")
		}
		return ret, nil
	} else if c.isStop {
		return []string{"--" + stop}, nil
	} else if c.isSubmit {
		return []string{"--" + submit}, nil
	}
	return nil, stacktrace.NewError("Did not specify an operation flag: %v", operations)
}
