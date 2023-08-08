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
	"strconv"

	"alluxio.org/cli/env"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
)

var Cancel = &CancelCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "cancel",
		JavaClassName: "alluxio.cli.job.JobShell",
	},
}

type CancelCommand struct {
	*env.BaseJavaCommand
	jobId int
}

func (c *CancelCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CancelCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Cancel.CommandName,
		Short: "Cancels a job asynchronously.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().IntVar(&c.jobId, "id", 0, "Determine a job ID to cancel.")
	cmd.MarkFlagRequired("id")
	return cmd
}

func (c *CancelCommand) Run(args []string) error {
	var javaArgs []string
	if c.jobId <= 0 {
		stacktrace.Propagate(nil, "Flag --id should be a positive integer")
	}
	javaArgs = append(javaArgs, "cancel")
	javaArgs = append(javaArgs, strconv.Itoa(c.jobId))
	return c.Base().Run(args)
}
