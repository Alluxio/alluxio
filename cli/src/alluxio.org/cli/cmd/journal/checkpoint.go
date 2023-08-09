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

package journal

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Checkpoint = &CheckpointCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "checkpoint",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"journal", "checkpoint"},
	},
}

type CheckpointCommand struct {
	*env.BaseJavaCommand
}

func (c *CheckpointCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CheckpointCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Checkpoint.CommandName,
		Short: "Create a checkpoint in the primary master journal system",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	return cmd
}
