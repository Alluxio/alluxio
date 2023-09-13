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

func Chgrp(className string) env.Command {
	return &ChgrpCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "chgrp",
			JavaClassName: className,
		},
	}
}

type ChgrpCommand struct {
	*env.BaseJavaCommand
	recursive bool
}

func (c *ChgrpCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ChgrpCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%s [group] [path]", c.CommandName),
		Short: "Changes the group of a file or directory",
		Long: `The chgrp command changes the group of the file or directory in Alluxio.
Alluxio supports file authorization with POSIX file permissions.
The file owner or superuser can execute this command.`,
		Example: `# Change the group of a file
$ ./bin/alluxio fs chgrp alluxio-group-new /input/file1`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVarP(&c.recursive, "recursive", "R", false,
		"change the group recursively for all files and directories under the given path")
	return cmd
}

func (c *ChgrpCommand) Run(args []string) error {
	var javaArgs []string
	if c.recursive {
		javaArgs = append(javaArgs, "-R")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(args)
}
