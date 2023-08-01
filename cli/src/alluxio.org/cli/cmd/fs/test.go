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

func Test(className string) env.Command {
	return &TestCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "test",
			JavaClassName: className,
			Parameters:    []string{"test"},
		},
	}
}

type TestCommand struct {
	*env.BaseJavaCommand

	isDirectory  bool
	isExists     bool
	isFile       bool
	isNotEmpty   bool
	isZeroLength bool
}

func (c *TestCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "test [path]",
		Short: "Test a property of a path, returning 0 if the property is true, or 1 otherwise",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVarP(&c.isDirectory, "dir", "d", false, "Test if path is a directory")
	cmd.Flags().BoolVarP(&c.isExists, "exists", "e", false, "Test if path exists")
	cmd.Flags().BoolVarP(&c.isFile, "file", "f", false, "Test if path is a file")
	cmd.Flags().BoolVarP(&c.isFile, "not-empty", "s", false, "Test if path is not empty")
	cmd.Flags().BoolVarP(&c.isFile, "zero", "z", false, "Test if path is zero length")
	return cmd
}

func (c *TestCommand) Run(args []string) error {
	var javaArgs []string
	if c.isDirectory {
		javaArgs = append(javaArgs, "-d")
	}
	if c.isExists {
		javaArgs = append(javaArgs, "-e")
	}
	if c.isFile {
		javaArgs = append(javaArgs, "-f")
	}
	if c.isNotEmpty {
		javaArgs = append(javaArgs, "-s")
	}
	if c.isZeroLength {
		javaArgs = append(javaArgs, "-z")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
