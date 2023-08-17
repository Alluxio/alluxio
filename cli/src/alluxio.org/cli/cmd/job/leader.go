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

var Leader = &LeaderCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "leader",
		JavaClassName: names.JobShellJavaClass,
	},
}

type LeaderCommand struct {
	*env.BaseJavaCommand
}

func (c *LeaderCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *LeaderCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Leader.CommandName,
		Short: "Prints the hostname of the job master service leader.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *LeaderCommand) Run(args []string) error {
	javaArgs := []string{"leader"}
	return c.Base().Run(javaArgs)
}
