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

func Mkdir(className string) env.Command {
	return &MkdirCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "mkdir",
			JavaClassName: className,
			Parameters:    []string{"mkdir"},
		},
	}
}

type MkdirCommand struct {
	*env.BaseJavaCommand
}

func (c *MkdirCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *MkdirCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "mkdir [path1 path2 ...]",
		Short: "Create directories at the specified paths, creating the parent directory if not exists",
		Long: `The mkdir command creates a new directory in the Alluxio filesystem.
It is recursive and will create any parent directories that do not exist.
Note that the created directory will not be created in the under storage system until a file in the directory is persisted to the underlying storage.
Using mkdir on an invalid or existing path will fail.`,
		Example: `# Creating a folder structure
$ ./bin/alluxio fs mkdir /users
$ ./bin/alluxio fs mkdir /users/Alice
$ ./bin/alluxio fs mkdir /users/Bob`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *MkdirCommand) Run(args []string) error {
	return c.Base().Run(args)
}
