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

var Cp = &CopyCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "cp",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
		Parameters:    []string{"cp"},
	},
}

type CopyCommand struct {
	*env.BaseJavaCommand

	isRecursive bool
}

func (c *CopyCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CopyCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [srcPath] [dstPath]", Cp.CommandName),
		Short: "Copy a file or directory",
		Long: `Copies a file or directory in the Alluxio filesystem or between local and Alluxio filesystems
Use the file:// schema to indicate a local filesystem path (ex. file:///absolute/path/to/file) and
use the recursive flag to copy directories`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVarP(&c.isRecursive, "recursive", "R", false, "True to copy the directory subtree to the destination directory")
	// TODO: the java class also exposes flags for preserve, and threads
	//  buffersize is also declared and mentioned in the usage text, but it's not added as an option in java code so it was never working
	return cmd
}

func (c *CopyCommand) Run(args []string) error {
	var javaArgs []string
	if c.isRecursive {
		javaArgs = append(javaArgs, "-r")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
