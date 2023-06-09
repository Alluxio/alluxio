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

package fs

import (
	"fmt"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Mv = &MoveCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "mv",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
		Parameters:    []string{"mv"},
	},
}

type MoveCommand struct {
	*env.BaseJavaCommand
}

func (c *MoveCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *MoveCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [srcPath] [dstPath]", Mv.CommandName),
		Short: "Rename a file or directory",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *MoveCommand) Run(args []string) error {
	return c.Base().Run(args)
}
