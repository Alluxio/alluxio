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

package info

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Master = &MasterCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "master",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
		Parameters:    []string{"masterInfo"},
	},
}

type MasterCommand struct {
	*env.BaseJavaCommand
}

func (c *MasterCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *MasterCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Master.CommandName,
		Short: "Prints information regarding master fault tolerance such as leader address and list of master addresses",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	return cmd
}

func (c *MasterCommand) Run(_ []string) error {
	// TODO: output in a serializable format
	return c.Base().Run(nil)
}
