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
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func Mv(className string) env.Command {
	return &MoveCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "mv",
			JavaClassName: className,
			Parameters:    []string{"mv"},
		},
	}
}

type MoveCommand struct {
	*env.BaseJavaCommand
}

func (c *MoveCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *MoveCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "mv [srcPath] [dstPath]",
		Short: "Rename a file or directory",
		Long: `The mv command moves a file or directory to another path in Alluxio.
The destination path must not exist or be a directory.
If it is a directory, the file or directory will be placed as a child of the directory.
The command is purely a metadata operation and does not affect the data blocks of the file.`,
		Example: `# Moving a file
$ ./bin/alluxio fs mv /data/2014 /data/archives/2014`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *MoveCommand) Run(args []string) error {
	return c.Base().Run(args)
}
