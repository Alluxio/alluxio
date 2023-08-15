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

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var CmdStatus = &CmdStatusCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "cmdStatus",
		JavaClassName: names.JobShellJavaClass,
	},
}

type CmdStatusCommand struct {
	*env.BaseJavaCommand
	jobControlId int
}

func (c *CmdStatusCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CmdStatusCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   CmdStatus.CommandName,
		Short: "Get the status information for a distributed command.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const id = "id"
	cmd.Flags().IntVar(&c.jobControlId, id, 0, "Determine the job control ID to get the status information")
	cmd.MarkFlagRequired(id)
	return cmd
}

func (c *CmdStatusCommand) Run(args []string) error {
	if c.jobControlId <= 0 {
		return stacktrace.NewError("Flag --id should be a positive integer")
	}
	javaArgs := []string{"getCmdStatus", strconv.Itoa(c.jobControlId)}
	return c.Base().Run(javaArgs)
}
