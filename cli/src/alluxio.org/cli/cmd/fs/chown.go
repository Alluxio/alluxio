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

func Chown(className string) env.Command {
	return &ChownCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "chown",
			JavaClassName: className,
		},
	}
}

type ChownCommand struct {
	*env.BaseJavaCommand
	recursive bool
}

func (c *ChownCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ChownCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%s <owner>[:<group>] <path>", c.CommandName),
		Short: "Changes the owner of a file or directory specified by args",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVarP(&c.recursive, "recursive", "R", false,
		"change the owner recursively")
	return cmd
}

func (c *ChownCommand) Run(args []string) error {
	var javaArgs []string
	if c.recursive {
		javaArgs = append(javaArgs, "-R")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(args)
}
