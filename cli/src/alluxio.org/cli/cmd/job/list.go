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
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var List = &ListCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "list",
		JavaClassName: names.JobShellJavaClass,
	},
}

type ListCommand struct {
	*env.BaseJavaCommand
}

func (c *ListCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ListCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use: List.CommandName,
		Short: "Prints the IDs of the most recent jobs, running and finished, " +
			"in the history up to the capacity set in alluxio.job.master.job.capacity",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *ListCommand) Run(args []string) error {
	javaArgs := []string{"ls"}
	return c.Base().Run(javaArgs)
}
