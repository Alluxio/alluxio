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

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/env"
)

var JStatus = &JStatusCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "jobStatus",
		JavaClassName: cmd.JobShellJavaClass,
	},
}

type JStatusCommand struct {
	*env.BaseJavaCommand
	jobId     int
	everyTask bool
}

func (c *JStatusCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *JStatusCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   JStatus.CommandName,
		Short: "Displays the status info for the specific job.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().IntVar(&c.jobId, "id", 0,
		"Determine the job ID to get status info.")
	cmd.Flags().BoolVarP(&c.everyTask, "every-task", "v", false,
		"Determine display the status of every task.")
	cmd.MarkFlagRequired("id")
	return cmd
}

func (c *JStatusCommand) Run(args []string) error {
	if c.jobId <= 0 {
		return stacktrace.NewError("Flag --id should be a positive integer")
	}
	javaArgs := []string{"stat"}
	if c.everyTask {
		javaArgs = append(javaArgs, "-v")
	}
	javaArgs = append(javaArgs, strconv.Itoa(c.jobId))
	return c.Base().Run(javaArgs)
}
