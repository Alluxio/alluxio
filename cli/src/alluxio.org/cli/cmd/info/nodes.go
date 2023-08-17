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

package info

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Nodes = &NodesCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "nodes",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"nodes", "status"},
	},
}

type NodesCommand struct {
	*env.BaseJavaCommand
}

func (c *NodesCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *NodesCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   c.CommandName,
		Short: "Show all registered workers' status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *NodesCommand) Run(args []string) error {
	return c.Base().Run(args)
}
