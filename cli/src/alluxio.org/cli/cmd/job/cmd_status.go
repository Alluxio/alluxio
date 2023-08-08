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

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var CStatus = &CStatusCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "cmdStatus",
		JavaClassName: "alluxio.cli.job.JobShell",
	},
}

type CStatusCommand struct {
	*env.BaseJavaCommand
	jobControlId int
}

func (c *CStatusCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CStatusCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Cancel.CommandName,
		Short: "Get the status information for a distributed command.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().IntVar(&c.jobControlId, "jobControlId", 0,
		"Determine the jobControl ID to get the status information.")
	cmd.MarkFlagRequired("jobControlId")
	return cmd
}

func (c *CStatusCommand) Run(args []string) error {
	var javaArgs []string
	if c.jobControlId <= 0 {
		stacktrace.Propagate(nil, "Flag --jobControlId should be a positive integer")
	}
	javaArgs = append(javaArgs, "getCmdStatus")
	javaArgs = append(javaArgs, strconv.Itoa(c.jobControlId))
	return c.Base().Run(args)
}
