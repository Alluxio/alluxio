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

func Cat(className string) env.Command {
	return &CatCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "cat",
			JavaClassName: className,
			Parameters:    []string{"cat"},
		},
	}
}

type CatCommand struct {
	*env.BaseJavaCommand
}

func (c *CatCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CatCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "cat [path]",
		Short: "Print specified file's content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *CatCommand) Run(args []string) error {
	return c.Base().Run(args)
}
