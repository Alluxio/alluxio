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

var Checksum = &ChecksumCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "checksum",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
		Parameters:    []string{"checksum"},
	},
}

type ChecksumCommand struct {
	*env.BaseJavaCommand
}

func (c *ChecksumCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ChecksumCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [path]", Checksum.CommandName),
		Short: "Calculates the md5 checksum of a specified file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *ChecksumCommand) Run(args []string) error {
	return c.Base().Run(args)
}
