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

var Count = &CountCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "count",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
		Parameters:    []string{"count"},
	},
}

type CountCommand struct {
	*env.BaseJavaCommand

	isHumanReadable bool
}

func (c *CountCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CountCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [path]", Count.CommandName),
		Short: "Displays the number of files and directories matching the specified path",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	// special case to overwrite the -h shorthand for --help flag for --human-readable
	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")

	cmd.Flags().BoolVarP(&c.isHumanReadable, "human-readable", "h", false, "Print sizes in human readable format")
	return cmd
}

func (c *CountCommand) Run(args []string) error {
	var javaArgs []string
	if c.isHumanReadable {
		javaArgs = append(javaArgs, "-h")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
