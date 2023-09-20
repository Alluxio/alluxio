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

func Head(className string) env.Command {
	return &HeadCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "head",
			JavaClassName: className,
			Parameters:    []string{"head"},
		},
	}
}

type HeadCommand struct {
	*env.BaseJavaCommand

	bytes string
}

func (c *HeadCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *HeadCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "head [path]",
		Short: "Print the leading bytes from the specified file",
		Long: `The head command prints the first 1KB of data of a file to the shell.
Specifying the -c flag sets the number of bytes to print.`,
		Example: `# Print first 2048 bytes of a file
$ ./bin/alluxio fs head -c 2048 /output/part-00000`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVarP(&c.bytes, "bytes", "c", "", "Byte size to print")
	return cmd
}

func (c *HeadCommand) Run(args []string) error {
	var javaArgs []string
	if c.bytes != "" {
		javaArgs = append(javaArgs, "-c", c.bytes)
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
